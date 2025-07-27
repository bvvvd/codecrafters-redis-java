package redis.command;

import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.replication.ReplicationService;
import redis.resp.RespArray;
import redis.resp.RespBulkString;
import redis.resp.RespValue;

import java.net.Socket;
import java.util.List;
import java.util.Objects;

import static redis.util.Logger.debug;

public final class ReplConf extends AbstractRedisCommand {
    private final Mode mode;
    private final String value;
    private final ReplicationService replicationService;

    public ReplConf(List<RespValue> tokens, RedisConfig config, ReplicationService replicationService) {
        super(config);

        if (tokens.size() < 2 || !(tokens.get(1) instanceof RespBulkString replConfMode)) {
            throw new RedisException("REPLCONF command requires a valid mode argument: " + tokens);
        }
        this.mode = getMode(tokens, replConfMode);
        this.value = tokens.size() > 2 ? ((RespBulkString) tokens.get(2)).value() : null;
        this.replicationService = replicationService;
    }

    private Mode getMode(List<RespValue> tokens, RespBulkString mode) {
        Mode replConfMode;
        if (mode.value().equalsIgnoreCase(Mode.GET_ACK.getMode())) {
            replConfMode = Mode.GET_ACK;
        } else if (mode.value().equalsIgnoreCase(Mode.CAPA.getMode())) {
            replConfMode = Mode.CAPA;
        } else if (mode.value().equalsIgnoreCase(Mode.LISTENING_PORT.getMode())) {
            replConfMode = Mode.LISTENING_PORT;
        } else if (mode.value().equalsIgnoreCase(Mode.ACK.getMode())) {
            replConfMode = Mode.ACK;
        } else {
            throw new RedisException("REPLCONF command requires a valid mode argument: " + tokens);
        }
        return replConfMode;
    }

    @Override
    public void handle(Socket client) {
        if (config.getRole().equalsIgnoreCase("master")) {
            debug("Received REPLCONF command as master: %s", this);
        } else {
            debug("Received REPLCONF command as replica: %s, offset: %d", this, replicationService.getOffset());
        }
        RespValue response = switch (mode) {
            case ACK -> {
                // If I receive a response here, I have sent a REPLCONF GETACK and
                // the response returned to me will not include the bytes counted
                // for that command I sent, so compare the received length with
                // my current replicationOffset - the REPLCONF GETACK request sent
                RespArray respArray = new RespArray(List.of(new RespBulkString("REPLCONF"),
                        new RespBulkString("GETACK"),
                        new RespBulkString("*")));
                var expectedOffset = replicationService.getOffset() -
                                     respArray
                                             .serialize().length;
                debug("REPLCONF ACK received: %s, current replication offset: %d", this, replicationService.getOffset());
                debug("current replication offset: %d, delta: %d", replicationService.getOffset(), respArray.serialize().length);
                var actualOffset = Integer.parseInt(value);
                debug("expected offset: %d, actual offset: %d", expectedOffset, actualOffset);
//                if (waitLatch != null && expectedOffset == actualOffset) {
//                    waitLatch.countDown();
//                }
                yield null;
            }
            case CAPA, LISTENING_PORT -> new RespBulkString("OK");
            case GET_ACK ->
                    new RespArray(List.of(new RespBulkString("REPLCONF"), new RespBulkString("ACK"), new RespBulkString(Long.toString(replicationService.getOffset()))));
        };
        if (response != null) {
            sendResponse(client, response);
        }
    }

    public Mode mode() {
        return mode;
    }

    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ReplConf) obj;
        return Objects.equals(this.mode, that.mode) &&
               Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode, value);
    }

    @Override
    public String toString() {
        return "ReplConf[" +
               "mode=" + mode + ", " +
               "value=" + value + ']';
    }
}
