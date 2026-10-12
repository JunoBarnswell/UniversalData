package org.universaldata;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.CharacterCodingException;
import java.util.Objects;

final class Utf8 {
    private Utf8() { }
    static byte[] bytes(String value) {
        Objects.requireNonNull(value, "value");
        try {
            var buffer = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value));
            byte[] result = new byte[buffer.remaining()]; buffer.get(result); return result;
        } catch (CharacterCodingException error) { throw new UniversalDataException(6, "Invalid UTF16 input"); }
    }
}
