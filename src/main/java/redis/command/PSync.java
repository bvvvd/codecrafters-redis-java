package redis.command;

import redis.RedisSocket;
import redis.config.RedisConfig;
import redis.replication.ReplicationService;
import redis.resp.RespSimpleString;

import static redis.util.Logger.debug;

public final class PSync extends AbstractRedisCommand {
    public static final String CODE = "PSYNC";

    public PSync(RedisConfig config, ReplicationService replicationService) {
        super(config, replicationService);
    }

    @Override
    public void handleCommand(RedisSocket client) {
        debug("Received PSYNC command");
        RespSimpleString response = new RespSimpleString("FULLRESYNC %s 0".formatted(config.getReplicationId()));
        sendResponse(client, response);

        byte[] rdbContent = {'$', '8', '8', '\r', '\n', 0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, (byte) 0xfa, 0x09, 0x72, 0x65,
                0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, (byte) 0xfa, 0x0a, 0x72,
                0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, (byte) 0xc0, 0x40, (byte) 0xfa, 0x05, 0x63, 0x74,
                0x69, 0x6d, 0x65, (byte) 0xc2, 0x6d, 0x08, (byte) 0xbc, 0x65, (byte) 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64,
                0x2d, 0x6d, 0x65, 0x6d, (byte) 0xc2, (byte) 0xb0, (byte) 0xc4, 0x10, 0x00, (byte) 0xfa, 0x08, 0x61,
                0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, (byte) 0xc0, 0x00, (byte) 0xff, (byte) 0xf0, 0x6e, 0x3b,
                (byte) 0xfe, (byte) 0xc0, (byte) 0xff, 0x5a, (byte) 0xa2};
        debug("Sending response: %s", new String(rdbContent));
        client.write(rdbContent);

        replicationService.addReplica(client);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj != null && obj.getClass() == this.getClass();
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public String toString() {
        return "PSync[]";
    }
}
