package cache.resp;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import redis.resp.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class ParserTest {

    private static final Parser parser = new Parser();

    @Nested
    class DeserializationTests {

        @Test
        void testParseNullInput() {
            assertThatThrownBy(() -> parser.parse((byte[]) null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Input cannot be null or empty");
        }

        @Test
        void testParseEmptyInput() {
            assertThatThrownBy(() -> parser.parse(new byte[0]))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Input cannot be null or empty");
        }

        @Test
        void testParseInvalidInput() {
            byte[] input = "invalid input".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid RESP format");
        }

        @Test
        void testParseNullValue() {
            byte[] input = "_\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespNull());
        }

        @Test
        void testParseSimpleString() {
            byte[] input = "+hello world\r\n".getBytes();
            assertThat(parser.parse(input))
                    .containsExactlyInAnyOrder(new RespSimpleString("hello world"));
        }

        @Test
        void testParseError() {
            byte[] input = "-error message\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespError("error message"));
        }

        @Test
        void testParseInteger() {
            byte[] input = ":42\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespInteger(42));
        }

        @Test
        void testParseLargeInteger() {
            byte[] input = ":9223372036854775807\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespInteger(Long.MAX_VALUE));
        }

        @Test
        void testParseCorruptedInteger() {
            byte[] input = ":not a number\r\n".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid number format");
        }

        @Test
        void testParseBulkString() {
            byte[] input = "$25\r\nhellohellohellohellohello\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespBulkString("hellohellohellohellohello"));
        }

        @Test
        void testParseBulkStringWithZeroLength() {
            byte[] input = "$0\r\n\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespBulkString(""));
        }

        @Test
        void testParseBulkStringWithCorruptedLength() {
            byte[] input = "$not a number\r\nhello".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid bulk string length");
        }

        @Test
        void testParseBulkStringWithUnexpectedEnd() {
            byte[] input = "$5\r\nhello".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Bulk string must end with CRLF");
        }

        @Test
        void testBulkStringNull() {
            byte[] input = "$-1\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespBulkString(null));
        }

        @Test
        void testParseEmptyArray() {
            byte[] input = "*0\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespArray(List.of()));
        }

        @Test
        void testParseArray() {
            byte[] input = "*3\r\n+item1\r\n+item2\r\n+item3\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespArray(List.of(
                    new RespSimpleString("item1"),
                    new RespSimpleString("item2"),
                    new RespSimpleString("item3")
            )));
        }

        @Test
        void testParseArrayWithNestedValues() {
            byte[] input = "*2\r\n+item1\r\n*2\r\n+nested1\r\n:666\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespArray(List.of(
                    new RespSimpleString("item1"),
                    new RespArray(List.of(
                            new RespSimpleString("nested1"),
                            new RespInteger(666)
                    ))
            )));
        }

        @Test
        void testParseBooleanTrue() {
            byte[] input = "#t\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespBoolean(true));
        }

        @Test
        void testParseBooleanFalse() {
            byte[] input = "#f\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespBoolean(false));
        }

        @Test
        void testParseInvalidBoolean() {
            byte[] input = "#invalid\r\n".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid boolean format");
        }

        @Test
        void testParseDouble() {
            byte[] input = ",3.1415\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespDouble(3.1415));
        }

        @Test
        void testParsePositiveInfinityDouble() {
            byte[] input = ",inf\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespDouble(Double.POSITIVE_INFINITY));
        }

        @Test
        void testParseNegativeInfinityDouble() {
            byte[] input = ",-inf\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespDouble(Double.NEGATIVE_INFINITY));
        }

        @Test
        void testParseNaNDouble() {
            byte[] input = ",nan\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespDouble(Double.NaN));
        }

        @Test
        void testParseBigNumber() {
            byte[] input = "(3492890328409238509324850943850943825024385\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(
                    new RespBigNumber(new BigDecimal("3492890328409238509324850943850943825024385")));
        }

        @Test
        void testParseInvalidBigNumber() {
            byte[] input = "(invalid number\r\n".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid big number format");
        }

        @Test
        void testParseBulkError() {
            byte[] input = "!25\r\nerrorerrorerrorerrorerror\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(
                    new RespBulkError("errorerrorerrorerrorerror"));
        }

        @Test
        void testParseBulkErrorWithZeroLength() {
            byte[] input = "!0\r\n\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespBulkError(""));
        }

        @Test
        void testParseBulkErrorWithCorruptedLength() {
            byte[] input = "!not a number\r\nerror".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid bulk error string length");
        }

        @Test
        void testParseBulkErrorWithUnexpectedEnd() {
            byte[] input = "!5\r\nerror".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Bulk string must end with CRLF");
        }

        @Test
        void testParseVerbatimString() {
            byte[] input = "=15\r\ntxt:Some string\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(
                    new RespVerbatimString("txt", "Some string"));
        }

        @Test
        void testParseVerbatimStringWithWrongSeparator() {
            byte[] input = "=15\r\ntxt;Some string\r\n".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid verbatim string format");

        }

        @Test
        void testParseVerbatimStringWithEmptyValue() {
            byte[] input = "=4\r\ntxt:\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(
                    new RespVerbatimString("txt", ""));
        }

        @Test
        void testParseMap() {
            byte[] input = "%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(
                    new RespMap(Map.of(
                            new RespSimpleString("first"), new RespInteger(1),
                            new RespSimpleString("second"), new RespInteger(2)
                    )));
        }

        @Test
        void testParseMapWithNestedValues() {
            byte[] input = "%2\r\n+mapKey1\r\n*2\r\n+val1\r\n+val2\r\n+mapKey2\r\n*2\r\n:1\r\n:2\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(
                    new RespMap(Map.of(
                            new RespSimpleString("mapKey1"), new RespArray(List.of(
                                    new RespSimpleString("val1"),
                                    new RespSimpleString("val2")
                            )),
                            new RespSimpleString("mapKey2"), new RespArray(List.of(
                                    new RespInteger(1),
                                    new RespInteger(2)
                            ))
                    )));
        }

        @Test
        void testParseInvalidMap() {
            byte[] input = "%2\r\n+key1\r\n:1\r\n+key2\r\nnot a number\r\n".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid RESP format");
        }

        @Test
        void testParseInvalidMapWithUnexpectedEnd() {
            byte[] input = "%2\r\n+key1\r\n:1\r\n+key2\r\n".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Offset is out of bounds");
        }

        @Test
        void testParseInvalidMapWithCorruptedLength() {
            byte[] input = "%not a number\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid map length");
        }

        @Test
        void testParseInvalidVerbatimString() {
            byte[] input = "=15\r\ninvalid:format\r\n".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unexpected end of input");
        }

        @Test
        void testParseAttributeWithEmptyInput() {
            byte[] input = "|0\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(
                    new RespAttribute(new RespMap(Map.of()), new RespArray(List.of())));
        }

        @Test
        void testParseAttributeWithInvalidInput() {
            byte[] input = "|2\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n$1\r\nb\r\n,0.0012\r\n*2\r\n:2039123\r\n:9543892\r\n".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Offset is out of bounds");
        }

        @Test
        void testParseAttributeWithInvalidArray() {
            byte[] input = "|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n$1\r\nb\r\n,0.0012\r\n*3\r\n:2039123\r\n:9543892\r\n".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Expected array entry");
        }

        @Test
        void testParseAttributeWithValidInput() {
            byte[] input = "|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n$1\r\nb\r\n,0.0012\r\n*2\r\n:2039123\r\n:9543892\r\n".getBytes();
            RespAttribute expected =
                    new RespAttribute(
                            new RespMap(Map.of(
                                    new RespSimpleString("key-popularity"), new RespMap(
                                            Map.of(
                                                    new RespBulkString("a"), new RespDouble(0.1923),
                                                    new RespBulkString("b"), new RespDouble(0.0012))
                                    )
                            )),
                            new RespArray(List.of(
                                    new RespInteger(2039123),
                                    new RespInteger(9543892)
                            ))
                    );
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(expected);
        }

        @Test
        void testParseSet() {
            byte[] input = "~3\r\n+item1\r\n+item2\r\n+item3\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(
                    new RespSet(Set.of(
                            new RespSimpleString("item1"),
                            new RespSimpleString("item2"),
                            new RespSimpleString("item3")
                    )));
        }

        @Test
        void testParseSetWithNestedValues() {
            byte[] input = "~2\r\n+item1\r\n*2\r\n+nested1\r\n:666\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(
                    new RespSet(Set.of(
                            new RespSimpleString("item1"),
                            new RespArray(List.of(
                                    new RespSimpleString("nested1"),
                                    new RespInteger(666)
                            ))
                    )));
        }

        @Test
        void testParseSetWithEmptyInput() {
            byte[] input = "~0\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(new RespSet(Set.of()));
        }

        @Test
        void testParseSetWithInvalidInput() {
            byte[] input = "~2\r\n+item1\r\nnot a valid item\r\n".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid RESP format");
        }

        @Test
        void testParseSetWithDuplicateItems() {
            byte[] input = "~3\r\n+item1\r\n+item2\r\n+item1\r\n".getBytes();
            assertThatThrownBy(() -> parser.parse(input))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Duplicate value in set");
        }

        @Test
        void testParsePush() {
            byte[] input = ">3\r\n+item1\r\n+item2\r\n+item3\r\n".getBytes();
            assertThat(parser.parse(input)).containsExactlyInAnyOrder(
                    new RespPush(List.of(
                            new RespSimpleString("item1"),
                            new RespSimpleString("item2"),
                            new RespSimpleString("item3")
                    )));
        }
    }

    @Nested
    class SerializationTests {

        @Test
        void testSerializeRespNull() {
            RespNull respNull = new RespNull();
            assertThat(respNull.serialize()).isEqualTo("_\r\n".getBytes());
        }

        @Test
        void testSerializeRespSimpleString() {
            RespSimpleString respSimpleString = new RespSimpleString("hello world");
            assertThat(respSimpleString.serialize()).isEqualTo("+hello world\r\n".getBytes());
        }

        @Test
        void testSerializeRespInteger() {
            RespInteger respInteger = new RespInteger(42);
            assertThat(respInteger.serialize()).isEqualTo(":42\r\n".getBytes());
        }

        @Test
        void testSerializeLargeRespInteger() {
            RespInteger respInteger = new RespInteger(Long.MAX_VALUE);
            assertThat(respInteger.serialize()).isEqualTo(":9223372036854775807\r\n".getBytes());
        }

        @Test
        void testSerializeLargeNegativeRespInteger() {
            RespInteger respInteger = new RespInteger(Long.MIN_VALUE + 1);
            assertThat(respInteger.serialize()).isEqualTo(":-9223372036854775807\r\n".getBytes());
        }

        @Test
        void testSerializeRespIntegerMinValue() {
            RespInteger respInteger = new RespInteger(Long.MIN_VALUE);
            assertThat(respInteger.serialize()).isEqualTo(":-9223372036854775808\r\n".getBytes());
        }

        @Test
        void testSerializeRespBulkString() {
            RespBulkString respBulkString = new RespBulkString("hellohellohellohellohello");
            assertThat(respBulkString.serialize()).isEqualTo("$25\r\nhellohellohellohellohello\r\n".getBytes());
        }

        @Test
        void testSerializeRespBulkStringWithZeroLength() {
            RespBulkString respBulkString = new RespBulkString("");
            assertThat(respBulkString.serialize()).isEqualTo("$0\r\n\r\n".getBytes());
        }

        @Test
        void testSerializeRespBulkStringNull() {
            RespBulkString respBulkString = new RespBulkString(null);
            assertThat(respBulkString.serialize()).isEqualTo("$-1\r\n".getBytes());
        }

        @Test
        void testSerializeRespArrayEmpty() {
            RespArray respArray = new RespArray(List.of());
            assertThat(respArray.serialize()).isEqualTo("*0\r\n".getBytes());
        }

        @Test
        void testSerializeRespArray() {
            RespArray respArray = new RespArray(List.of(
                    new RespSimpleString("item1"),
                    new RespSimpleString("item2"),
                    new RespSimpleString("item3")
            ));
            assertThat(respArray.serialize()).isEqualTo("*3\r\n+item1\r\n+item2\r\n+item3\r\n".getBytes());
        }

        @Test
        void testSerializeRespArrayWithNestedValues() {
            RespArray respArray = new RespArray(List.of(
                    new RespSimpleString("item1"),
                    new RespArray(List.of(
                            new RespSimpleString("nested1"),
                            new RespInteger(666)
                    ))
            ));
            assertThat(respArray.serialize()).isEqualTo("*2\r\n+item1\r\n*2\r\n+nested1\r\n:666\r\n".getBytes());
        }

        @Test
        void testSerializeRespBooleanTrue() {
            RespBoolean respBoolean = new RespBoolean(true);
            assertThat(respBoolean.serialize()).isEqualTo("#t\r\n".getBytes());
        }

        @Test
        void testSerializeRespBooleanFalse() {
            RespBoolean respBoolean = new RespBoolean(false);
            assertThat(respBoolean.serialize()).isEqualTo("#f\r\n".getBytes());
        }

        @Test
        void testSerializeRespDouble() {
            RespDouble respDouble = new RespDouble(3.1415);
            assertThat(respDouble.serialize()).isEqualTo(",3.1415\r\n".getBytes());
        }

        @Test
        void testSerializePositiveInfinityRespDouble() {
            RespDouble respDouble = new RespDouble(Double.POSITIVE_INFINITY);
            assertThat(respDouble.serialize()).isEqualTo(",inf\r\n".getBytes());
        }

        @Test
        void testSerializeNegativeInfinityRespDouble() {
            RespDouble respDouble = new RespDouble(Double.NEGATIVE_INFINITY);
            assertThat(respDouble.serialize()).isEqualTo(",-inf\r\n".getBytes());
        }

        @Test
        void testSerializeNaNRespDouble() {
            RespDouble respDouble = new RespDouble(Double.NaN);
            assertThat(respDouble.serialize()).isEqualTo(",nan\r\n".getBytes());
        }

        @Test
        void testSerializeRespBigNumber() {
            RespBigNumber respBigNumber = new RespBigNumber(new BigDecimal("3492890328409238509324850943850943825024385"));
            assertThat(respBigNumber.serialize()).isEqualTo("(3492890328409238509324850943850943825024385\r\n".getBytes());
        }

        @Test
        void testSerializeRespBulkError() {
            RespBulkError respBulkError = new RespBulkError("errorerrorerrorerrorerror");
            assertThat(respBulkError.serialize()).isEqualTo("!25\r\nerrorerrorerrorerrorerror\r\n".getBytes());
        }

        @Test
        void testSerializeRespVerbatimString() {
            RespVerbatimString respVerbatimString = new RespVerbatimString("txt", "Some string");
            assertThat(respVerbatimString.serialize()).isEqualTo("=15\r\ntxt:Some string\r\n".getBytes());
        }

        @Test
        void testSerializeRespMap() {
            RespMap respMap = new RespMap(Map.of(
                    new RespSimpleString("first"), new RespInteger(1),
                    new RespSimpleString("second"), new RespInteger(2)
            ));
            assertThat(respMap.serialize()).containsExactlyInAnyOrder("%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n".getBytes());
        }

        @Test
        void testSerializeRespMapWithNestedValues() {
            RespMap respMap = new RespMap(Map.of(
                    new RespSimpleString("mapKey1"), new RespArray(List.of(
                            new RespSimpleString("val1"),
                            new RespSimpleString("val2")
                    )),
                    new RespSimpleString("mapKey2"), new RespArray(List.of(
                            new RespInteger(1),
                            new RespInteger(2)
                    ))
            ));
            assertThat(respMap.serialize()).containsExactlyInAnyOrder(
                    "%2\r\n+mapKey1\r\n*2\r\n+val1\r\n+val2\r\n+mapKey2\r\n*2\r\n:1\r\n:2\r\n".getBytes()
            );
        }

        @Test
        void testSerializeEmptyRespAttribute() {
            RespAttribute respAttribute = new RespAttribute(new RespMap(Map.of()), new RespArray(List.of()));
            assertThat(respAttribute.serialize()).containsExactlyInAnyOrder("|0\r\n".getBytes());
        }

        @Test
        void testSerializeRespAttribute() {
            RespAttribute respAttribute =
                    new RespAttribute(
                            new RespMap(Map.of(
                                    new RespSimpleString("key-popularity"), new RespMap(
                                            Map.of(
                                                    new RespBulkString("a"), new RespDouble(0.1923),
                                                    new RespBulkString("b"), new RespDouble(0.0012))
                                    )
                            )),
                            new RespArray(List.of(
                                    new RespInteger(2039123),
                                    new RespInteger(9543892)
                            ))
                    );
            assertThat(respAttribute.serialize()).containsExactlyInAnyOrder(
                    "|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n$1\r\nb\r\n,0.0012\r\n*2\r\n:2039123\r\n:9543892\r\n".getBytes()
            );
        }

        @Test
        void testSerializeRespSet() {
            RespSet respSet = new RespSet(Set.of(
                    new RespSimpleString("item1"),
                    new RespSimpleString("item2"),
                    new RespSimpleString("item3")
            ));
            assertThat(respSet.serialize()).containsExactlyInAnyOrder("~3\r\n+item1\r\n+item2\r\n+item3\r\n".getBytes());
        }

        @Test
        void testSerializeRespSetWithNestedValues() {
            RespSet respSet = new RespSet(Set.of(
                    new RespSimpleString("item1"),
                    new RespArray(List.of(
                            new RespSimpleString("nested1"),
                            new RespInteger(666)
                    ))
            ));
            assertThat(respSet.serialize()).containsExactlyInAnyOrder("~2\r\n+item1\r\n*2\r\n+nested1\r\n:666\r\n".getBytes());
        }

        @Test
        void testSerializeEmptyRespSet() {
            RespSet respSet = new RespSet(Set.of());
            assertThat(respSet.serialize()).isEqualTo("~0\r\n".getBytes());
        }

        @Test
        void testSerializeRespPush() {
            RespPush respPush = new RespPush(List.of(
                    new RespSimpleString("item1"),
                    new RespSimpleString("item2"),
                    new RespSimpleString("item3")
            ));
            assertThat(respPush.serialize()).containsExactlyInAnyOrder(">3\r\n+item1\r\n+item2\r\n+item3\r\n".getBytes());
        }
    }
}