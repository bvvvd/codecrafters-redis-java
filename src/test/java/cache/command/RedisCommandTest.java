//package cache.command;
//
//import cache.resp.*;
//import exception.RedisException;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.params.ParameterizedTest;
//import org.junit.jupiter.params.provider.MethodSource;
//
//import java.util.List;
//import java.util.stream.Stream;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
//
//class RedisCommandTest {
//
//    @Test
//    void throwRedisExceptionOnNullInput() {
//        assertThatThrownBy(() -> RedisCommand.from(null))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("input array is null or empty");
//    }
//
//    @Test
//    void throwRedisExceptionOnNullValues() {
//        assertThatThrownBy(() -> RedisCommand.from(new RespArray(null)))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("input array is null or empty");
//    }
//
//    @Test
//    void throwRedisExceptionOnEmptyValues() {
//        assertThatThrownBy(() -> RedisCommand.from(new RespArray(List.of())))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("input array is null or empty");
//    }
//
//    @ParameterizedTest
//    @MethodSource("invalidTypedValues")
//    void throwRedisExceptionOnInvalidType(RespValue invalidTypedValue) {
//        assertThatThrownBy(() -> RedisCommand.from(new RespArray(List.of(invalidTypedValue))))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("invalid command type: " + invalidTypedValue.getClass().getSimpleName());
//    }
//
//    static Stream<RespValue> invalidTypedValues() {
//        return Stream.of(
//                new RespArray(null),
//                new RespAttribute(null, null),
//                new RespBigNumber(null),
//                new RespBoolean(false),
//                new RespBulkError(null),
//                new RespDouble(0),
//                new RespError(null),
//                new RespInteger(0),
//                new RespMap(null),
//                new RespNull(),
//                new RespSet(null),
//                new RespSimpleString(null),
//                new RespVerbatimString(null, null)
//        );
//    }
//
//    @Test
//    void testCreatesPingCommand() {
//        RespArray array = new RespArray(List.of(new RespBulkString("PING")));
//        RedisCommand command = RedisCommand.from(array);
//        assertThat(command).isInstanceOf(Ping.class);
//    }
//
//    @Test
//    void testCreatesPingCommandWithArguments() {
//        RespArray array = new RespArray(List.of(new RespBulkString("PING"),
//                new RespBulkString("Hello")));
//        RedisCommand command = RedisCommand.from(array);
//        assertThat(command).isEqualTo(new Ping());
//    }
//
//    @Test
//    void testThrowsExceptionOnEchoWithNoArguments() {
//        RespArray array = new RespArray(List.of(new RespBulkString("ECHO")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("ECHO command requires a valid message argument");
//    }
//
//    @ParameterizedTest
//    @MethodSource("invalidTypedValues")
//    void testThrowsExceptionOnEchoWithInvalidArgument(RespValue invalidArgument) {
//        RespArray array = new RespArray(List.of(new RespBulkString("ECHO"), invalidArgument));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("ECHO command requires a valid message argument");
//
//    }
//
//    @Test
//    void testCreatesEchoCommand() {
//        RespArray array = new RespArray(List.of(new RespBulkString("ECHO"),
//                new RespBulkString("Hello World!")));
//        RedisCommand command = RedisCommand.from(array);
//        assertThat(command).isEqualTo(new Echo(new RespBulkString("Hello World!")));
//    }
//
//    @Test
//    void testThrowsExceptionOnSetWithNoArguments() {
//        RespArray array = new RespArray(List.of(new RespBulkString("SET")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("SET command requires valid key and value arguments");
//    }
//
//    @Test
//    void testThrowsExceptionOnSetWithInvalidKey() {
//        RespArray array = new RespArray(List.of(new RespBulkString("SET"),
//                new RespBulkError("key")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("SET command requires valid key and value arguments");
//    }
//
//    @Test
//    void testThrowsExceptionOnSetWithInvalidValue() {
//        RespArray array = new RespArray(List.of(new RespBulkString("SET"),
//                new RespBulkString("key"),
//                new RespBulkError("value")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("SET command requires valid key and value arguments");
//    }
//
//    @Test
//    void testCreatesSetCommandWithoutExpiration() {
//        RespArray array = new RespArray(List.of(new RespBulkString("SET"),
//                new RespBulkString("key"),
//                new RespBulkString("value")));
//        RedisCommand command = RedisCommand.from(array);
//        assertThat(command).isEqualTo(new Set(new RespBulkString("key"), new RespBulkString("value"), -1, array.serialize()));
//    }
//
//    @Test
//    void testThrowsExceptionWhenInvalidPx() {
//        RespArray array = new RespArray(List.of(new RespBulkString("SET"),
//                new RespBulkString("key"),
//                new RespBulkString("value"),
//                new RespBulkString("qew")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("unrecognized set option: qew");
//    }
//
//    @Test
//    void testThrowsExceptionWhenPresentPxAndNoExpirationTime() {
//        RespArray array = new RespArray(List.of(new RespBulkString("SET"),
//                new RespBulkString("key"),
//                new RespBulkString("value"),
//                new RespBulkString("PX")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("SET command requires a valid expiration if px is specified");
//    }
//
//    @Test
//    void testThrowsExceptionWhenInvalidTypedPx() {
//        RespArray array = new RespArray(List.of(new RespBulkString("SET"),
//                new RespBulkString("key"),
//                new RespBulkString("value"),
//                new RespBulkError("qwe"),
//                new RespSimpleString("1000")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("SET command requires a valid expiration if px is specified");
//    }
//
//    @Test
//    void testThrowsExceptionWhenInvalidTypedExpirationTime() {
//        RespArray array = new RespArray(List.of(new RespBulkString("SET"),
//                new RespBulkString("key"),
//                new RespBulkString("value"),
//                new RespBulkString("PX"),
//                new RespBulkError("qwe")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("SET command requires a valid expiration if px is specified");
//    }
//
//    @Test
//    void testThrowsExceptionWhenNegativeExpirationTime() {
//        RespArray array = new RespArray(List.of(new RespBulkString("SET"),
//                new RespBulkString("key"),
//                new RespBulkString("value"),
//                new RespBulkString("PX"),
//                new RespBulkString("-1000")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("SET command requires a positive expiration time");
//    }
//
//    @Test
//    void testCreatesSetCommandWithExpiration() {
//        RespArray array = new RespArray(List.of(new RespBulkString("SET"),
//                new RespBulkString("key"),
//                new RespBulkString("value"),
//                new RespBulkString("PX"),
//                new RespBulkString("1000")));
//        long start = System.currentTimeMillis();
//        RedisCommand command = RedisCommand.from(array);
//        Set expected = new Set(new RespBulkString("key"), new RespBulkString("value"), 1000 + System.currentTimeMillis(), array.serialize());
//        assertThat(command)
//                .hasFieldOrPropertyWithValue("key", expected.key())
//                .hasFieldOrPropertyWithValue("value", expected.value());
//        long actualExpirationTime = ((Set) command).expirationTime();
//        assertThat(actualExpirationTime).isBetween(start, start + 10000);
//    }
//
//    @Test
//    void testThrowsExceptionOnGetWithNoArguments() {
//        RespArray array = new RespArray(List.of(new RespBulkString("GET")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("GET command requires a valid key argument");
//    }
//
//    @Test
//    void testThrowsExceptionOnGetWithInvalidKey() {
//        RespArray array = new RespArray(List.of(new RespBulkString("GET"),
//                new RespBulkError("key")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("GET command requires a valid key argument");
//    }
//
//    @Test
//    void testCreatesGetCommand() {
//        RespArray array = new RespArray(List.of(new RespBulkString("GET"),
//                new RespBulkString("key")));
//        RedisCommand command = RedisCommand.from(array);
//        assertThat(command).isEqualTo(new Get(new RespBulkString("key")));
//    }
//
//    @Test
//    void testThrowsExceptionOnUnknownCommand() {
//        RespArray array = new RespArray(List.of(new RespBulkString("UNKNOWN_COMMAND")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("unsupported command: UNKNOWN_COMMAND");
//    }
//
//    @Test
//    void testThrowsExceptionOnUnfinishedConfigCommand() {
//        RespArray array = new RespArray(List.of(new RespBulkString("CONFIG")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("CONFIG command requires a valid subcommand");
//    }
//
//    @Test
//    void testThrowsExceptionOnConfigInvalidTypedGet() {
//        RespArray array = new RespArray(List.of(new RespBulkString("CONFIG"),
//                new RespBulkError("GET")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("CONFIG command requires a valid subcommand");
//    }
//
//    @Test
//    void testThrowsExceptionOnInvalidSubcommand() {
//        RespArray array = new RespArray(List.of(new RespBulkString("CONFIG"),
//                new RespBulkString("invalid")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("CONFIG command requires a valid subcommand");
//    }
//
//    @Test
//    void testThrowsExceptionOnConfigGetWithNoArguments() {
//        RespArray array = new RespArray(List.of(new RespBulkString("CONFIG"),
//                new RespBulkString("GET")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("CONFIG GET command requires a valid pattern argument");
//    }
//
//    @Test
//    void testThrowsExceptionOnConfigGetWithInvalidPatternType() {
//        RespArray array = new RespArray(List.of(new RespBulkString("CONFIG"),
//                new RespBulkString("GET"),
//                new RespBulkError("pattern")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("CONFIG GET command requires a valid pattern argument");
//    }
//
//    @Test
//    void testThrowsExceptionOnConfigGetWithEmptyPattern() {
//        RespArray array = new RespArray(List.of(new RespBulkString("CONFIG"),
//                new RespBulkString("GET"),
//                new RespBulkString("")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("CONFIG GET command requires a valid pattern argument");
//    }
//
//    @Test
//    void testThrowsExceptionOnConfigGetWithInvalidPattern() {
//        RespArray array = new RespArray(List.of(new RespBulkString("CONFIG"),
//                new RespBulkString("GET"),
//                new RespBulkString("invalid")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("CONFIG GET command requires a valid pattern argument");
//    }
//
//    @Test
//    void testCreatesConfigGetCommand() {
//        RespArray array = new RespArray(List.of(new RespBulkString("CONFIG"),
//                new RespBulkString("GET"),
//                new RespBulkString("dir")));
//        RedisCommand command = RedisCommand.from(array);
//        assertThat(command).isEqualTo(new ConfigGet("dir"));
//    }
//
//    @Test
//    void testThrowsExceptionOnKeysWithNoArguments() {
//        RespArray array = new RespArray(List.of(new RespBulkString("KEYS")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("KEYS command requires a valid pattern argument");
//    }
//
//    @Test
//    void testThrowsExceptionOnKeysWithInvalidPatternType() {
//        RespArray array = new RespArray(List.of(new RespBulkString("KEYS"),
//                new RespBulkError("pattern")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("KEYS command requires a valid pattern argument");
//    }
//
//    @Test
//    void testThrowsExceptionOnKeysWithInvalidPattern() {
//        RespArray array = new RespArray(List.of(new RespBulkString("KEYS"),
//                new RespBulkString("")));
//        assertThatThrownBy(() -> RedisCommand.from(array))
//                .isInstanceOf(RedisException.class)
//                .hasMessage("KEYS command requires a valid pattern argument");
//    }
//
//    @Test
//    void testCreatesKeysCommand() {
//        RespArray array = new RespArray(List.of(new RespBulkString("KEYS"),
//                new RespBulkString("*")));
//        RedisCommand command = RedisCommand.from(array);
//        assertThat(command).isEqualTo(new Keys());
//    }
//
//    @Test
//    void testCreatesInfoCommand() {
//        RespArray array = new RespArray(List.of(new RespBulkString("INFO")));
//        RedisCommand command = RedisCommand.from(array);
//        assertThat(command).isInstanceOf(Info.class);
//    }
//
//    @Test
//    void testCreatesReplConfCommand() {
//        RespArray array = new RespArray(List.of(new RespBulkString("REPLCONF"),
//                new RespBulkString("listening-port"),
//                new RespBulkString("6380")));
//        RedisCommand command = RedisCommand.from(array);
//        assertThat(command).isInstanceOf(ReplConf.class)
//                .hasFieldOrPropertyWithValue("mode", Mode.LISTENING_PORT)
//                .hasFieldOrPropertyWithValue("value", "6380");
//    }
//
//    @Test
//    void testCreatesReplConfGetAckCommand() {
//        RespArray array = new RespArray(
//                List.of(new RespBulkString("REPLCONF"),
//                        new RespBulkString("GETACK"),
//                        new RespBulkString("*")));
//        RedisCommand command = RedisCommand.from(array);
//        assertThat(command).isInstanceOf(ReplConf.class)
//                .hasFieldOrProperty("mode")
//                .hasFieldOrProperty("value");
//    }
//
//    @Test
//    void testCreatesPSyncCommand() {
//        RespArray array = new RespArray(List.of(new RespBulkString("PSYNC")));
//        RedisCommand command = RedisCommand.from(array);
//        assertThat(command).isInstanceOf(PSync.class);
//    }
//
//    @Test
//    void createWaitCommand() {
//        RespArray array = new RespArray(List.of(new RespBulkString("WAIT"),
//                new RespBulkString("1"),
//                new RespBulkString("1000")));
//        RedisCommand command = RedisCommand.from(array);
//        assertThat(command).isEqualTo(new Wait(1, 1000));
//    }
//}