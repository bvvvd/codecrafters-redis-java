package redis;

import redis.cache.Cache;
import redis.cache.CachedValue;
import redis.cache.RedisSortedSet;
import redis.cache.StreamCache;
import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.replication.EventReplicationService;
import redis.resp.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static redis.util.Logger.debug;
import static redis.util.Logger.error;

public class MainEventLoop implements AutoCloseable {
    private static final byte[] PONG = new RespSimpleString("PONG").serialize();
    private static final RespArray ACK_COMMAND = new RespArray(List.of(
            new RespBulkString("REPLCONF"),
            new RespBulkString("GETACK"),
            new RespBulkString("*")
    ));
    private static final byte[] QUEUED = new RespSimpleString("QUEUED").serialize();
    private static final byte[] OK = new RespSimpleString("OK").serialize();
    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final Set<SocketChannel> servingClients;
    private final ByteBuffer buffer;
    private final Parser parser;
    private final RedisConfig config;
    private final Cache cache;
    private final StreamCache streams;
    private final EventReplicationService replicationService;
    private PendingWait pendingWait;
    private String lastCommand;
    private final Map<RespValue, Queue<PendingWait>> blPopWaiters;
    private final Map<List<RespValue>, PendingWait> xReadWaiters;
    private final Map<ClientState, Queue<RespArray>> transactions;
    private final Map<ClientState, Set<RespValue>> pubSub;
    private final Map<RespValue, RedisSortedSet> sortedSets;

    public MainEventLoop(RedisConfig redisConfig, Cache cache, StreamCache streams) throws IOException {
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.bind(new InetSocketAddress("127.0.0.1", redisConfig.getPort()));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        servingClients = new HashSet<>();
        buffer = ByteBuffer.allocate(1024);
        parser = new Parser();
        config = redisConfig;
        this.cache = cache;
        this.streams = streams;
        replicationService = new EventReplicationService(redisConfig, parser, 0L);
        blPopWaiters = new HashMap<>();
        xReadWaiters = new HashMap<>();
        transactions = new HashMap<>();
        pubSub = new HashMap<>();
        sortedSets = new HashMap<>();
    }

    public void serve() throws IOException {
        replication();
        runLoop();
    }

    private void runLoop() throws IOException {
        while (!Thread.currentThread().isInterrupted()) {
            selector.select(10);
            checkWaitClients();
            checkBlPopWaiters();
            checkXReadWaiters();
            Set<SelectionKey> keys = selector.selectedKeys();
            handleKeys(keys);
            keys.clear();
        }
    }

    private void handleKeys(Set<SelectionKey> keys) {
        for (SelectionKey key : keys) {
            try {
                if (key.isAcceptable()) {
                    handleAccept(key);
                } else if (key.isReadable()) {
                    handleRead(key);
                } else if (key.isWritable()) {
                    handleWrite(key);
                }
            } catch (Exception e) {
                error("Error handling key %s: %s", key, e);
                key.cancel();
                try {
                    if (key.channel() instanceof SocketChannel client) {
                        key.channel().close();
                        servingClients.remove(client);
                    }
                } catch (IOException closeException) {
                    error("Error closing channel %s: %s", key.channel(), closeException);
                }
            }
        }
    }

    private void replication() throws IOException {
        if (config.getRole().equalsIgnoreCase("slave")) {
            SocketChannel replicationConnection = replicationService.establishReplication();
            replicationConnection.configureBlocking(false);
            SelectionKey registeredKey = replicationConnection.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            registeredKey.attach(new ClientState(registeredKey));
            servingClients.add(replicationConnection);
            debug("Accepted connection from %s", replicationConnection.getRemoteAddress());
            servingClients.add(replicationConnection);
        }
    }

    private void checkWaitClients() {
        if (pendingWait != null
            && (pendingWait.receivedAcks >= pendingWait.requiredAcks || System.currentTimeMillis() >= pendingWait.expiration)) {
            sendResponse(pendingWait.state, new RespInteger(pendingWait.receivedAcks).serialize());
            pendingWait = null;
        }
    }

    private void checkBlPopWaiters() {
        Set<RespValue> keys = new HashSet<>(blPopWaiters.keySet());
        keys.forEach(this::checkBlPopWaiters);
    }

    private boolean checkBlPopWaiters(RespValue key) {
        Queue<PendingWait> waiters = blPopWaiters.get(key);
        if (waiters != null) {
            PendingWait firstWaiter = waiters.peek();
            long currentTime = System.currentTimeMillis();
            if (firstWaiter.expiration != -1 && currentTime >= firstWaiter.expiration) {
                sendResponse(firstWaiter.state, new RespBulkString(null).serialize());
                if (waiters.size() == 1) {
                    blPopWaiters.remove(key);
                } else {
                    waiters.poll();
                }
                return true;
            } else {
                CachedValue<RespValue> cachedValue = cache.get(key);
                if (cachedValue.value() instanceof RespArray array) {
                    firstWaiter = waiters.poll();
                    if (array.values().size() == 1) {
                        cache.remove(key);
                        sendResponse(firstWaiter.state, new RespArray(List.of(key, array.values().getFirst())).serialize());
                    } else {
                        sendResponse(firstWaiter.state, new RespArray(List.of(key, array.values().removeFirst())).serialize());
                    }
                    while (!waiters.isEmpty()) {
                        PendingWait waiter = waiters.poll();
                        waiter.state.pendingForAcks = false;
                    }
                    cache.remove(key);
                    blPopWaiters.remove(key);
                    return true;
                }
            }
        }

        return false;
    }

    private void checkXReadWaiters() {
        var iterator = xReadWaiters.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();

            if (entry.getValue().expiration != -1 && System.currentTimeMillis() > entry.getValue().expiration) {
                sendResponse(entry.getValue().state, new RespBulkString(null).serialize());
                iterator.remove();
            } else {
                RespArray read = streams.xReadBlocking(entry.getKey());
                if (!read.values().isEmpty()) {
                    sendResponse(entry.getValue().state, read.serialize());
                    iterator.remove();
                }
            }
        }
    }

    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept();
        client.configureBlocking(false);
        SelectionKey registeredKey = client.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        registeredKey.attach(new ClientState(registeredKey));
        servingClients.add(client);
        debug("Accepted connection from %s", client.getRemoteAddress());
    }

    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        ClientState state = (ClientState) key.attachment();
        buffer.clear();
        int bytesRead = client.read(buffer);
        if (bytesRead == -1 && !state.pendingForAcks) {
            debug("Client %s disconnected", client.getRemoteAddress());
            client.close();
            servingClients.remove(client);
            return;
        }
        if (bytesRead == -1) {
            return;
        }
        buffer.flip();
        List<RespValue> respValues = parser.parse(buffer);

        for (RespValue respValue : respValues) {
            if (respValue instanceof RespArray array) {
                List<RespValue> values = array.values();
                String command = ((RespBulkString) values.getFirst()).value();
                if (pubSub.containsKey(state) &&
                    !"SUBSCRIBE".equalsIgnoreCase(command)
                    && !"UNSUBSCRIBE".equalsIgnoreCase(command)
                    && !"PSUBSCRIBE".equalsIgnoreCase(command)
                    && !"PUNSUBSCRIBE".equalsIgnoreCase(command)
                    && !"PING".equalsIgnoreCase(command)
                    && !"QUIT".equalsIgnoreCase(command)) {
                    sendResponse(state, new RespError("ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context".formatted(command)).serialize());
                } else if (!("EXEC".equalsIgnoreCase(command) || "DISCARD".equalsIgnoreCase(command)) && transactions.containsKey(state)) {
                    transactions.get(state).add(array);
                    sendResponse(state, QUEUED);
                } else {
                    byte[] response = handleCommand(array, state);
                    if (response != null) {
                        sendResponse(state, response);
                    }
                }
                lastCommand = command;
            }
        }

        if (!state.pendingForAcks) {
            key.interestOps(SelectionKey.OP_WRITE);
        }
    }

    private byte[] handleCommand(RespArray array, ClientState state) throws IOException {
        List<RespValue> values = array.values();
        String command = ((RespBulkString) values.getFirst()).value();
        return switch (command) {
            case "PING" -> ping(state);
            case "ECHO" -> echo(values);
            case "SET" -> set(values, array);
            case "GET" -> get(values);
            case "CONFIG" -> configGet(values);
            case "KEYS" -> keys();
            case "INFO" -> info();
            case "REPLCONF" -> replConf(values, array);
            case "PSYNC" -> pSync(state);
            case "WAIT" -> wait(values, state);
            case "RPUSH" -> rPush(values, array);
            case "LRANGE" -> lRange(values);
            case "LPUSH" -> lPush(values, array);
            case "LLEN" -> lLen(values);
            case "LPOP" -> lPop(values, array);
            case "BLPOP" -> blPop(values, state, array);
            case "TYPE" -> type(values);
            case "XADD" -> xAdd(values);
            case "XRANGE" -> xRange(values);
            case "XREAD" -> xRead(values, state);
            case "INCR" -> incr(values);
            case "MULTI" -> multi(state);
            case "EXEC" -> exec(state);
            case "DISCARD" -> discard(state);
            case "SUBSCRIBE" -> subscribe(values, state);
            case "PUBLISH" -> publish(values);
            case "UNSUBSCRIBE" -> unsubscribe(values, state);
            case "ZADD" -> zAdd(values);
            default -> new RespSimpleString("ERR unknown command").serialize();
        };
    }

    private byte[] zAdd(List<RespValue> values) {
        RespValue key = values.get(1);
        double score = Double.parseDouble(((RespBulkString) values.get(2)).value());
        RespValue value = values.get(3);
        boolean added = sortedSets.computeIfAbsent(key, k -> new RedisSortedSet()).add(value, score);
        return new RespInteger(added ? 1 : 0).serialize();
    }

    private byte[] unsubscribe(List<RespValue> values, ClientState state) {
        RespValue channel = values.get(1);
        Set<RespValue> subscriptions = pubSub.get(state);
        if (subscriptions == null) {
            return new RespArray(List.of(new RespBulkString("unsubscribe"), channel, new RespInteger(0))).serialize();
        }

        subscriptions.remove(channel);
        if (subscriptions.isEmpty()) {
            pubSub.remove(state);
        }
        return new RespArray(List.of(new RespBulkString("unsubscribe"), channel, new RespInteger(subscriptions.size()))).serialize();
    }

    private byte[] publish(List<RespValue> values) {
        RespValue channel = values.get(1);
        RespValue content = values.get(2);
        int subscriptions = 0;
        for (var entry : pubSub.entrySet()) {
            if (entry.getValue().contains(channel)) {
                subscriptions++;
                sendResponse(entry.getKey(), new RespArray(List.of(new RespBulkString("message"), channel, content)).serialize());
            }
        }
        return new RespInteger(subscriptions).serialize();
    }

    private byte[] subscribe(List<RespValue> values, ClientState state) {
        RespValue channel = values.get(1);
        pubSub.computeIfAbsent(state, k -> new HashSet<>()).add(channel);
        return new RespArray(List.of(new RespBulkString("subscribe"), channel, new RespInteger(pubSub.get(state).size()))).serialize();
    }

    private byte[] discard(ClientState state) {
        if (transactions.containsKey(state)) {
            transactions.remove(state);
            return OK;
        } else {
            return new RespError("ERR DISCARD without MULTI").serialize();
        }
    }

    private byte[] exec(ClientState state) throws IOException {
        if (!transactions.containsKey(state)) {
            return new RespError("ERR EXEC without MULTI").serialize();
        } else {
            byte[] response = exec(transactions.get(state), state);
            transactions.remove(state);
            return response;
        }
    }

    private byte[] exec(Queue<RespArray> queue, ClientState state) throws IOException {
        List<byte[]> responses = new ArrayList<>(queue.size());
        while (!queue.isEmpty()) {
            responses.add(handleCommand(queue.poll(), state));
        }
        int totalSize = 0;
        for (byte[] response : responses) {
            totalSize += response.length;
        }
        byte[] transactionResponse = new byte[totalSize + 3 + Integer.toString(responses.size()).length()];
        transactionResponse[0] = '*';
        int i = 1;
        for (byte b : Integer.toString(responses.size()).getBytes()) {
            transactionResponse[i] = b;
            i++;
        }
        transactionResponse[i] = '\r';
        i++;
        transactionResponse[i] = '\n';
        i++;
        for (byte[] response : responses) {
            System.arraycopy(response, 0, transactionResponse, i, response.length);
            i += response.length;
        }
        return transactionResponse;
    }

    private byte[] multi(ClientState state) {
        transactions.put(state, new LinkedList<>());
        return new RespBulkString("OK").serialize();
    }

    private byte[] incr(List<RespValue> values) {
        RespValue key = values.get(1);
        CachedValue<RespValue> cachedValue = cache.get(key);
        RespBulkString respBulkString = (RespBulkString) cachedValue.value();
        try {
            if (respBulkString.value() == null) {
                respBulkString.setValue("1");
            } else {
                respBulkString.setValue(Long.toString(Long.parseLong(respBulkString.value()) + 1));
            }
            return new RespInteger(Long.parseLong(respBulkString.value())).serialize();
        } catch (NumberFormatException _) {
            return new RespError("ERR value is not an integer or out of range").serialize();
        }
    }

    private byte[] xRead(List<RespValue> values, ClientState state) {
        if (values.get(1) instanceof RespBulkString type
            && type.value().equalsIgnoreCase("block")) {
            int timeout = Integer.parseInt(((RespBulkString) values.get(2)).value());
            List<RespValue> keys = values.subList(4, values.size());

            RespArray data = streams.xReadBlocking(keys);
            if (!data.values().isEmpty()) {
                return data.serialize();
            } else {
                state.pendingForAcks = true;
                PendingWait xReadWait = new PendingWait(state, -1, timeout == 0 ? -1 : System.currentTimeMillis() + timeout);
                xReadWaiters.put(keys, xReadWait);
            }
        } else {
            List<RespValue> keys = values.subList(2, values.size());

            return streams.xRead(keys).serialize();
        }
        return null;
    }

    private byte[] xRange(List<RespValue> values) {
        RespValue key = values.get(1);
        String start = null;
        String end = null;
        if (values.size() > 2) {
            start = ((RespBulkString) values.get(2)).value();
            if (values.size() > 3) {
                end = ((RespBulkString) values.get(3)).value();
            }
        }

        return streams.range(key, start, end).serialize();
    }

    private byte[] xAdd(List<RespValue> values) {
        RespValue key = values.get(1);
        RespBulkString entryId = ((RespBulkString) values.get(2));
        List<RespValue> streamValues = values.subList(3, values.size());
        return streams.add(key, entryId, streamValues).serialize();
    }

    private byte[] type(List<RespValue> values) {
        RespBulkString key = (RespBulkString) values.get(1);
        CachedValue<RespValue> cachedValue = cache.get(key);
        if (cachedValue.value() instanceof RespArray) {
            return new RespSimpleString("list").serialize();
        } else if (cachedValue.value() instanceof RespBulkString bulkString) {
            if (bulkString.value() == null) {
                if (streams.containsKey(key)) {
                    return new RespSimpleString("stream").serialize();
                } else {
                    return new RespSimpleString("none").serialize();
                }
            } else {
                return new RespSimpleString("string").serialize();
            }
        } else if (cachedValue.value() instanceof RespSet) {
            return new RespSimpleString("set").serialize();
        }
        return null;
    }

    private byte[] blPop(List<RespValue> values, ClientState state, RespArray array) throws ClosedChannelException {
        RespValue key = values.get(1);
        CachedValue<RespValue> cachedValue = cache.get(key);
        if (!(cachedValue.value() instanceof RespArray cachedArray)) {
            long expiration = -1;
            if (values.size() > 2) {
                String value = ((RespBulkString) values.get(2)).value();
                if (!"0".equalsIgnoreCase(value)) {
                    expiration = System.currentTimeMillis() + (long) (Double.parseDouble(value) * 1000);
                }
            }
            PendingWait blPopWaiter = new PendingWait(state, 0, expiration);
            state.pendingForAcks = true;
            blPopWaiters.computeIfAbsent(key, k -> new LinkedList<>()).offer(blPopWaiter);
        } else {
            boolean unblocked = checkBlPopWaiters(key);
            if (!unblocked) {
                List<RespValue> list = cachedArray.values();
                if (config.getRole().equalsIgnoreCase("master")) {
                    replicationService.propagate(array, selector);
                } else {
                    replicationService.moveOffset(array.getSize());
                }
                if (list.size() == 1) {
                    cache.remove(key);
                    return new RespArray(List.of(key, list.getFirst())).serialize();
                } else {
                    return new RespArray(List.of(key, list.removeFirst())).serialize();
                }
            }
        }
        return null;
    }

    private byte[] lPop(List<RespValue> values, RespArray array) throws ClosedChannelException {
        RespValue key = values.get(1);
        int range = values.size() < 3 || (!(values.get(2) instanceof RespBulkString respBulkString))
                ? 1
                : Integer.parseInt(respBulkString.value());
        CachedValue<RespValue> cachedValue = cache.get(key);
        if (config.getRole().equalsIgnoreCase("master")) {
            replicationService.propagate(array, selector);
        } else {
            replicationService.moveOffset(array.getSize());
        }
        if (!(cachedValue.getValue() instanceof RespArray cachedArray) || cachedArray.values().isEmpty()) {
            return new RespBulkString(null).serialize();
        } else if (cachedArray.values().size() <= range) {
            cache.remove(key);
            return cachedArray.serialize();
        } else if (range == 1) {
            return cachedArray.values().removeFirst().serialize();
        } else {
            List<RespValue> output = cachedArray.values().subList(0, Math.min(range, cachedArray.getSize()));
            List<RespValue> newArray = cachedArray.values().subList(Math.min(range, cachedArray.values().size()), cachedArray.values().size());
            cache.put(key, new RespArray(newArray), -1);
            return new RespArray(output).serialize();
        }
    }

    private byte[] lLen(List<RespValue> values) {
        CachedValue<RespValue> cachedValue = cache.get(values.get(1));
        if (cachedValue.value() instanceof RespArray array) {
            return new RespInteger(array.values().size()).serialize();
        } else {
            return new RespInteger(0).serialize();
        }
    }

    private byte[] lPush(List<RespValue> values, RespArray array) throws ClosedChannelException {
        RespValue key = values.get(1);
        List<RespValue> listValue = new ArrayList<>();
        for (int i = 2; i < values.size(); i++) {
            listValue.add(values.get(i));
        }
        CachedValue<RespValue> cachedValue = cache.get(key);
        List<RespValue> newArray = new CopyOnWriteArrayList<>(listValue.reversed());
        if (cachedValue != null && cachedValue.getValue() instanceof RespArray cachedArray) {
            newArray.addAll(cachedArray.values());
        }
        cache.put(key, new RespArray(newArray), -1);
        if (config.getRole().equalsIgnoreCase("master")) {
            replicationService.propagate(array, selector);
        } else {
            replicationService.moveOffset(array.getSize());
        }
        return new RespInteger(newArray.size()).serialize();
    }

    private byte[] lRange(List<RespValue> values) {
        RespValue key = values.get(1);
        int start = Integer.parseInt(((RespBulkString) values.get(2)).value());
        int end = Integer.parseInt(((RespBulkString) values.get(3)).value());
        CachedValue<RespValue> cachedValue = cache.get(key);
        if (cachedValue == null || !(cachedValue.getValue() instanceof RespArray array)) {
            return new RespArray(List.of()).serialize();
        } else {
            int from = normalize(start, array.values());
            int to = normalize(end, array.values());
            List<RespValue> subList = array.values().subList(from, Math.min(array.values().size(), to + 1));
            return new RespArray(subList).serialize();
        }
    }

    private int normalize(int index, List<RespValue> values) {
        if (index < 0) {
            if (-index > values.size()) {
                return 0;
            }
            return (index % values.size()) + values.size();
        }

        return index;
    }

    private byte[] rPush(List<RespValue> values, RespArray array) throws ClosedChannelException {
        RespValue key = values.get(1);
        List<RespValue> listValues = new ArrayList<>();
        for (int i = 2; i < values.size(); i++) {
            listValues.add(values.get(i));
        }
        CachedValue<RespValue> cachedValue = cache.get(key);
        if (config.getRole().equalsIgnoreCase("master")) {
            replicationService.propagate(array, selector);
        } else {
            replicationService.moveOffset(array.getSize());
        }
        if (cachedValue == null || !(cachedValue.getValue() instanceof RespArray cachedArray)) {
            cache.put(key, new RespArray(new CopyOnWriteArrayList<>(listValues)), -1);
            return new RespInteger(listValues.size()).serialize();
        } else {
            cachedArray.values().addAll(listValues);
            return new RespInteger(cachedArray.values().size()).serialize();
        }
    }

    private byte[] wait(List<RespValue> values, ClientState state) throws IOException {
        int numberOfReplicas = Integer.parseInt(((RespBulkString) values.get(1)).value());
        int delta = Integer.parseInt(((RespBulkString) values.get(2)).value());
        long timeout = System.currentTimeMillis() + delta;
        debug("Received WAIT command with numslaves: %d and timeout: %d", numberOfReplicas, timeout);
        if (!Objects.equals(lastCommand, "SET")) {
            return new RespInteger(replicationService.getReplicaNumber()).serialize();
        } else {
            var numReplicas = Math.max(numberOfReplicas, replicationService.getReplicaNumber());
            PendingWait pendingWaitSync = new PendingWait(state, numReplicas, timeout);
            replicationService.propagate(ACK_COMMAND, selector);
            pendingWait = pendingWaitSync;
            state.pendingForAcks = true;
        }
        return null;
    }

    private byte[] ping(ClientState state) {
        if (config.getRole().equalsIgnoreCase("master")) {
            if (pubSub.containsKey(state)) {
                return new RespArray(List.of(new RespBulkString("pong"), new RespBulkString(""))).serialize();
            }
            return PONG;
        }
        replicationService.moveOffset(PONG.length * 2);
        return null;
    }

    private byte[] echo(List<RespValue> values) {
        return values.getLast().serialize();
    }

    private byte[] pSync(ClientState state) {
        debug("Received PSYNC command");
        byte[] fullResyncResponse = new RespSimpleString("FULLRESYNC %s 0".formatted(config.getReplicationId())).serialize();

        byte[] rdbContent = {'$', '8', '8', '\r', '\n', 0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, (byte) 0xfa, 0x09, 0x72, 0x65,
                0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, (byte) 0xfa, 0x0a, 0x72,
                0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, (byte) 0xc0, 0x40, (byte) 0xfa, 0x05, 0x63, 0x74,
                0x69, 0x6d, 0x65, (byte) 0xc2, 0x6d, 0x08, (byte) 0xbc, 0x65, (byte) 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64,
                0x2d, 0x6d, 0x65, 0x6d, (byte) 0xc2, (byte) 0xb0, (byte) 0xc4, 0x10, 0x00, (byte) 0xfa, 0x08, 0x61,
                0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, (byte) 0xc0, 0x00, (byte) 0xff, (byte) 0xf0, 0x6e, 0x3b,
                (byte) 0xfe, (byte) 0xc0, (byte) 0xff, 0x5a, (byte) 0xa2};
        byte[] fullResponse = new byte[fullResyncResponse.length + rdbContent.length];
        System.arraycopy(fullResyncResponse, 0, fullResponse, 0, fullResyncResponse.length);
        System.arraycopy(rdbContent, 0, fullResponse, fullResyncResponse.length, rdbContent.length);
        debug("Sending response: %s", new String(rdbContent));
        replicationService.addReplica(((SocketChannel) state.key.channel()));
        return fullResponse;
    }

    private byte[] replConf(List<RespValue> values, RespArray array) {
        debug("Received REPLCONF command: %s", values);
        String mode = ((RespBulkString) values.get(1)).value().toUpperCase();
        RespValue response = switch (mode) {
            case "ACK" -> {
                var expectedOffset = replicationService.getOffset() - ACK_COMMAND.serialize().length;
                var actualOffset = Integer.parseInt(((RespBulkString) values.get(2)).value());
                debug("expected offset: %d, actual offset: %d", expectedOffset, actualOffset);
                if (pendingWait != null && actualOffset == expectedOffset) {
                    pendingWait.receivedAcks++;
                }
                checkWaitClients();
                yield null;
            }
            case "CAPA", "LISTENING-PORT" -> new RespSimpleString("OK");
            case "GETACK" ->
                    new RespArray(List.of(new RespBulkString("REPLCONF"), new RespBulkString("ACK"), new RespBulkString(Long.toString(replicationService.getOffset()))));
            default -> throw new RedisException("REPLCONF command requires a valid mode argument: " + mode);
        };
        if (config.getRole().equalsIgnoreCase("slave")) {
            long previous = replicationService.getOffset();
            replicationService.moveOffset(array.getSize());
            debug("manipulating offset by as replica: %d-%d-%d", previous, array.getSize(), replicationService.getOffset());
        }
        return response == null ? null : response.serialize();
    }

    private byte[] info() {
        debug("Received INFO command");
        return (config.getRole().equalsIgnoreCase("master")
                ? new RespBulkString("role:%s\r\nmaster_repl_offset:0\r\nmaster_replid:%s".formatted(config.getRole(), config.getReplicationId()))
                : new RespBulkString("role:%s".formatted(config.getRole()))).serialize();
    }

    private byte[] keys() {
        debug("Received KEYS command");
        return new RespArray(cache.getPersistedKeys()).serialize();
    }

    private byte[] configGet(List<RespValue> values) {
        String pattern = ((RespBulkString) values.get(2)).value();
        return new RespArray(List.of(
                new RespBulkString(pattern),
                new RespBulkString(pattern.equalsIgnoreCase("dir")
                        ? config.getDir()
                        : config.getDbFileName()))).serialize();
    }

    private byte[] get(List<RespValue> values) {
        RespBulkString getKey = ((RespBulkString) values.get(1));
        CachedValue<RespValue> cachedValue = cache.get(getKey);
        return cachedValue.getValue().serialize();
    }

    private byte[] set(List<RespValue> values, RespArray array) throws IOException {
        if (values.size() >= 3) {
            boolean isMaster = config.getRole().equalsIgnoreCase("master");
            if (isMaster) {
                debug("SET command received, caching %s", values);
            } else {
                debug("Replica SET received, caching: %s", values);
            }
            RespValue setKey = values.get(1);
            RespValue value = values.get(2);
            if (values.size() > 3) {
                cache.put(setKey, value, System.currentTimeMillis() + Long.parseLong(((RespBulkString) values.get(4)).value()));
            } else {
                cache.put(setKey, value);
            }
            if (isMaster) {
                replicationService.propagate(array, selector);
                return OK;
            } else {
                long previous = replicationService.getOffset();
                replicationService.moveOffset(array.getSize());
                debug("manipulating offset by as replica: %d-%d-%d", previous, array.getSize(), replicationService.getOffset());
            }
        }
        return null;
    }

    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        ClientState state = (ClientState) key.attachment();

        handleWrite(key, state, client);
    }

    private void handleWrite(SelectionKey key, ClientState state, SocketChannel client) throws IOException {
        boolean written = false;
        while (!state.pendingWrites.isEmpty()) {
            ByteBuffer stateBuffer = state.pendingWrites.poll();
            if (stateBuffer == null || !stateBuffer.hasRemaining()) {
                continue;
            }
            debug("sending #%s# to the client: %s", new String(stateBuffer.array()), ((InetSocketAddress) client.getRemoteAddress()).getPort());
            client.write(stateBuffer);
            written = true;
            if (stateBuffer.hasRemaining()) {
                return;
            }
        }

        if (written || !((ClientState) key.attachment()).pendingForAcks) {
            key.interestOps(SelectionKey.OP_READ);
            if (pendingWait != null && pendingWait.state == key.attachment()) {
                ((ClientState) key.attachment()).pendingForAcks = false;
                pendingWait = null;
            }
            ((ClientState) key.attachment()).pendingForAcks = false;
        }
    }

    private void sendResponse(ClientState client, byte[] data) {
        client.pendingWrites.add(ByteBuffer.wrap(data));
        client.key.interestOps(SelectionKey.OP_WRITE);
    }

    @Override
    public void close() throws Exception {
        serverChannel.close();
        selector.close();
        for (SocketChannel client : servingClients) {
            client.close();
        }
    }

    public static class ClientState {
        final Queue<ByteBuffer> pendingWrites = new ArrayDeque<>();
        final SelectionKey key;
        boolean pendingForAcks = false;

        public ClientState(SelectionKey key) {
            this.key = key;
        }

        public void append(ByteBuffer chunk) {
            pendingWrites.add(chunk);
        }

        @Override
        public String toString() {
            try {
                return String.valueOf(((InetSocketAddress) ((SocketChannel) key.channel()).getRemoteAddress()).getPort());
            } catch (IOException e) {
                throw new RedisException(e);
            }
        }
    }

    public static class PendingWait {
        final ClientState state;
        int receivedAcks;
        final int requiredAcks;
        final long expiration;

        public PendingWait(ClientState state, int requiredAcks, long expiration) {
            this.state = state;
            this.requiredAcks = requiredAcks;
            this.receivedAcks = 0;
            this.expiration = expiration;
        }

        @Override
        public String toString() {
            return "PendingWait{" +
                   "state=" + state +
                   ", receivedAcks=" + receivedAcks +
                   ", requiredAcks=" + requiredAcks +
                   ", expiration=" + expiration +
                   '}';
        }
    }
}
