package redis.config;

public class Constants {

    private Constants() {

    }

    public static final String CRLF = "\r\n";
    public static final String PING = "PING";
    public static final String PONG = "PONG";
    public static final String ECHO_COMMAND_CONTENT = "ECHO";
    public static final String SET_COMMAND_CONTENT = "SET";
    public static final String GET_COMMAND_CONTENT = "GET";
    public static final String EXPIRATION_TOKEN_CONTENT = "PX";
    public static final byte[] OK_RESPONSE_CONTENT = ("+OK" + CRLF).getBytes();
    public static final byte[] NIL_RESPONSE_CONTENT = ("$-1" + CRLF).getBytes();
    public static final String CONFIG_COMMAND_PREFIX_CONTENT = "CONFIG";
    public static final String KEYS_COMMAND_CONTENT = "KEYS";
    public static final String INFO_COMMAND_PREFIX_CONTENT = "INFO";
    public static final String DEFAULT_ROLE = "master";
    public static final String DEFAULT_DIR = "/tmp/redis-data";
    public static final String DEFAULT_RDB_FILENAME = "dump.rdb";
    public static final String REPLCONF_COMMAND_PREFIX_CONTENT = "REPLCONF";
    public static final String PSYNC_COMMAND_PREFIX_CONTENT = "PSYNC";
    public static final String WAIT_COMMAND_PREFIX_CONTENT = "WAIT";
    public static final int DEFAULT_PORT = 6379;
}
