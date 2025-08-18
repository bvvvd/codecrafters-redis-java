package redis;

import redis.cache.Cache;
import redis.cache.CachedValue;
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
    private static final RespArray ACK_COMMAND = new RespArray(List.of(
            new RespBulkString("REPLCONF"),
            new RespBulkString("GETACK"),
            new RespBulkString("*")
    ));
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
                switch (command) {
                    case "PING" -> ping(state);
                    case "ECHO" -> echo(state, values);
                    case "SET" -> set(values, state, array);
                    case "GET" -> get(values, state);
                    case "CONFIG" -> configGet(values, state);
                    case "KEYS" -> keys(state);
                    case "INFO" -> info(state);
                    case "REPLCONF" -> replConf(values, state, array);
                    case "PSYNC" -> pSync(state);
                    case "WAIT" -> wait(values, state);
                    case "RPUSH" -> rPush(values, state, array);
                    case "LRANGE" -> lRange(values, state);
                    case "LPUSH" -> lPush(values, state, array);
                    case "LLEN" -> lLen(values, state);
                    case "LPOP" -> lPop(values, state, array);
                    case "BLPOP" -> blPop(values, state, array);
                    case "TYPE" -> type(values, state);
                    case "XADD" -> xAdd(values, state);
                    case "XRANGE" -> xRange(values, state);
                    case "XREAD" -> xRead(values, state);
                    case "INCR" -> incr(values, state);
                    case "MULTI" -> multi(values, state);
                    case "EXEC" -> exec(values, state);
                    default -> sendResponse(state, "-ERR unknown command\r\n".getBytes());
                }
                lastCommand = command;
            }
        }

        if (!state.pendingForAcks) {
            key.interestOps(SelectionKey.OP_WRITE);
        }
    }

    private void exec(List<RespValue> values, ClientState state) {
        sendResponse(state, new RespError("ERR EXEC without MULTI").serialize());
    }

    private void multi(List<RespValue> values, ClientState state) {
        sendResponse(state, new RespBulkString("OK").serialize());
    }

    private void incr(List<RespValue> values, ClientState state) {
        RespValue key = values.get(1);
        CachedValue<RespValue> cachedValue = cache.get(key);
        RespBulkString respBulkString = (RespBulkString) cachedValue.value();
        try {
            if (respBulkString.value() == null) {
                respBulkString.setValue("1");
            } else {
                respBulkString.setValue(Long.toString(Long.parseLong(respBulkString.value()) + 1));
            }
            sendResponse(state, new RespInteger(Long.parseLong(respBulkString.value())).serialize());
        } catch (NumberFormatException _) {
            sendResponse(state, new RespError("ERR value is not an integer or out of range").serialize());
        }
    }

    private void xRead(List<RespValue> values, ClientState state) {
        if (values.get(1) instanceof RespBulkString type
            && type.value().equalsIgnoreCase("block")) {
            int timeout = Integer.parseInt(((RespBulkString) values.get(2)).value());
            List<RespValue> keys = values.subList(4, values.size());

            RespArray data = streams.xReadBlocking(keys);
            if (!data.values().isEmpty()) {
                sendResponse(state, data.serialize());
            } else {
                state.pendingForAcks = true;
                PendingWait xReadWait = new PendingWait(state, -1, timeout == 0 ? -1 : System.currentTimeMillis() + timeout);
                xReadWaiters.put(keys, xReadWait);
            }
        } else {
            List<RespValue> keys = values.subList(2, values.size());

            sendResponse(state, streams.xRead(keys).serialize());
        }
    }

    private void xRange(List<RespValue> values, ClientState state) {
        RespValue key = values.get(1);
        String start = null;
        String end = null;
        if (values.size() > 2) {
            start = ((RespBulkString) values.get(2)).value();
            if (values.size() > 3) {
                end = ((RespBulkString) values.get(3)).value();
            }
        }

        sendResponse(state, streams.range(key, start, end).serialize());
    }

    private void xAdd(List<RespValue> values, ClientState state) {
        RespValue key = values.get(1);
        RespBulkString entryId = ((RespBulkString) values.get(2));
        List<RespValue> streamValues = values.subList(3, values.size());
        sendResponse(state, streams.add(key, entryId, streamValues).serialize());
    }

    private void type(List<RespValue> values, ClientState state) {
        RespBulkString key = (RespBulkString) values.get(1);
        CachedValue<RespValue> cachedValue = cache.get(key);
        if (cachedValue.value() instanceof RespArray) {
            sendResponse(state, new RespSimpleString("list").serialize());
        } else if (cachedValue.value() instanceof RespBulkString bulkString) {
            if (bulkString.value() == null) {
                if (streams.containsKey(key)) {
                    sendResponse(state, new RespSimpleString("stream").serialize());
                } else {
                    sendResponse(state, new RespSimpleString("none").serialize());
                }
            } else {
                sendResponse(state, new RespSimpleString("string").serialize());
            }
        } else if (cachedValue.value() instanceof RespSet) {
            sendResponse(state, new RespSimpleString("set").serialize());
        }
    }

    private void blPop(List<RespValue> values, ClientState state, RespArray array) throws ClosedChannelException {
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
                if (list.size() == 1) {
                    cache.remove(key);
                    sendResponse(state, new RespArray(List.of(key, list.getFirst())).serialize());
                } else {
                    sendResponse(state, new RespArray(List.of(key, list.removeFirst())).serialize());
                }
                if (config.getRole().equalsIgnoreCase("master")) {
                    replicationService.propagate(array, selector);
                } else {
                    replicationService.moveOffset(array.getSize());
                }
            }
        }
    }

    private void lPop(List<RespValue> values, ClientState state, RespArray array) throws ClosedChannelException {
        RespValue key = values.get(1);
        int range = values.size() < 3 || (!(values.get(2) instanceof RespBulkString respBulkString))
                ? 1
                : Integer.parseInt(respBulkString.value());
        CachedValue<RespValue> cachedValue = cache.get(key);
        if (!(cachedValue.getValue() instanceof RespArray cachedArray) || cachedArray.values().isEmpty()) {
            sendResponse(state, new RespBulkString(null).serialize());
        } else if (cachedArray.values().size() <= range) {
            cache.remove(key);
            sendResponse(state, cachedArray.serialize());
        } else if (range == 1) {
            sendResponse(state, cachedArray.values().removeFirst().serialize());
        } else {
            List<RespValue> output = cachedArray.values().subList(0, Math.min(range, cachedArray.getSize()));
            List<RespValue> newArray = cachedArray.values().subList(Math.min(range, cachedArray.values().size()), cachedArray.values().size());
            cache.put(key, new RespArray(newArray), -1);
            sendResponse(state, new RespArray(output).serialize());
        }
        if (config.getRole().equalsIgnoreCase("master")) {
            replicationService.propagate(array, selector);
        } else {
            replicationService.moveOffset(array.getSize());
        }
    }

    private void lLen(List<RespValue> values, ClientState state) {
        CachedValue<RespValue> cachedValue = cache.get(values.get(1));
        if (cachedValue.value() instanceof RespArray array) {
            sendResponse(state, new RespInteger(array.values().size()).serialize());
        } else {
            sendResponse(state, new RespInteger(0).serialize());
        }
    }

    private void lPush(List<RespValue> values, ClientState state, RespArray array) throws ClosedChannelException {
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
        sendResponse(state, new RespInteger(newArray.size()).serialize());
        if (config.getRole().equalsIgnoreCase("master")) {
            replicationService.propagate(array, selector);
        } else {
            replicationService.moveOffset(array.getSize());
        }
    }

    private void lRange(List<RespValue> values, ClientState state) {
        RespValue key = values.get(1);
        int start = Integer.parseInt(((RespBulkString) values.get(2)).value());
        int end = Integer.parseInt(((RespBulkString) values.get(3)).value());
        CachedValue<RespValue> cachedValue = cache.get(key);
        if (cachedValue == null || !(cachedValue.getValue() instanceof RespArray array)) {
            sendResponse(state, new RespArray(List.of()).serialize());
        } else {
            int from = normalize(start, array.values());
            int to = normalize(end, array.values());
            List<RespValue> subList = array.values().subList(from, Math.min(array.values().size(), to + 1));
            sendResponse(state, new RespArray(subList).serialize());
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

    private void rPush(List<RespValue> values, ClientState state, RespArray array) throws ClosedChannelException {
        RespValue key = values.get(1);
        List<RespValue> listValues = new ArrayList<>();
        for (int i = 2; i < values.size(); i++) {
            listValues.add(values.get(i));
        }
        CachedValue<RespValue> cachedValue = cache.get(key);
        if (cachedValue == null || !(cachedValue.getValue() instanceof RespArray cachedArray)) {
            cache.put(key, new RespArray(new CopyOnWriteArrayList<>(listValues)), -1);
            sendResponse(state, new RespInteger(listValues.size()).serialize());
        } else {
            cachedArray.values().addAll(listValues);
            sendResponse(state, new RespInteger(cachedArray.values().size()).serialize());
        }
        if (config.getRole().equalsIgnoreCase("master")) {
            replicationService.propagate(array, selector);
        } else {
            replicationService.moveOffset(array.getSize());
        }
    }

    private void wait(List<RespValue> values, ClientState state) throws IOException {
        int numberOfReplicas = Integer.parseInt(((RespBulkString) values.get(1)).value());
        int delta = Integer.parseInt(((RespBulkString) values.get(2)).value());
        long timeout = System.currentTimeMillis() + delta;
        debug("Received WAIT command with numslaves: %d and timeout: %d", numberOfReplicas, timeout);
        if (!Objects.equals(lastCommand, "SET")) {
            sendResponse(state, new RespInteger(replicationService.getReplicaNumber()).serialize());
        } else {
            var numReplicas = Math.max(numberOfReplicas, replicationService.getReplicaNumber());
            PendingWait pendingWaitSync = new PendingWait(state, numReplicas, timeout);
            replicationService.propagate(ACK_COMMAND, selector);
            pendingWait = pendingWaitSync;
            state.pendingForAcks = true;
        }
    }

    private void ping(ClientState state) {
        if (config.getRole().equalsIgnoreCase("master")) {
            sendResponse(state, "+PONG\r\n".getBytes());
        } else {
            replicationService.moveOffset(14);
        }
    }

    private void echo(ClientState state, List<RespValue> values) {
        sendResponse(state, values.getLast().serialize());
    }

    private void pSync(ClientState state) {
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
        sendResponse(state, fullResponse);
    }

    private void replConf(List<RespValue> values, ClientState state, RespArray array) throws IOException {
        debug("Received REPLCONF command: %s", values);
        String mode = ((RespBulkString) values.get(1)).value().toUpperCase();
        RespValue response = switch (mode) {
            case "ACK" -> {
                var expectedOffset = replicationService.getOffset() - ACK_COMMAND.serialize().length;
                var actualOffset = Integer.parseInt(((RespBulkString) values.get(2)).value());
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
            replicationService.moveOffset(array.serialize().length);
        }
        if (response != null) {
            sendResponse(state, response.serialize());
        }
    }

    private void info(ClientState state) {
        debug("Received INFO command");
        RespBulkString response = config.getRole().equalsIgnoreCase("master")
                ? new RespBulkString("role:%s\r\nmaster_repl_offset:0\r\nmaster_replid:%s".formatted(config.getRole(), config.getReplicationId()))
                : new RespBulkString("role:%s".formatted(config.getRole()));
        sendResponse(state, response.serialize());
    }

    private void keys(ClientState state) {
        debug("Received KEYS command");
        sendResponse(state, new RespArray(cache.getPersistedKeys()).serialize());
    }

    private void configGet(List<RespValue> values, ClientState state) {
        String pattern = ((RespBulkString) values.get(2)).value();
        RespArray response = new RespArray(List.of(
                new RespBulkString(pattern),
                new RespBulkString(pattern.equalsIgnoreCase("dir")
                        ? config.getDir()
                        : config.getDbFileName())));
        debug("CONFIG GET command received, returning configuration.");
        sendResponse(state, response.serialize());
    }

    private void get(List<RespValue> values, ClientState state) {
        RespBulkString getKey = ((RespBulkString) values.get(1));
        CachedValue<RespValue> cachedValue = cache.get(getKey);
        sendResponse(state, cachedValue.getValue().serialize());
    }

    private void set(List<RespValue> values, ClientState state, RespArray array) throws IOException {
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
                sendResponse(state, new RespSimpleString("OK").serialize());
            } else {
                replicationService.moveOffset(array.getSize());
            }
        }
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
