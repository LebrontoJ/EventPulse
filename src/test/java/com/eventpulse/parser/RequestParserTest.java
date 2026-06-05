package com.eventpulse.parser;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RequestParserTest {
    private final RequestParser parser = new RequestParser();

    @Test
    void parsesValidRequest() throws Exception {
        ParsedRequest request = parser.parse("UPLOAD|filename=test.pdf|size=1024");

        assertEquals("UPLOAD", request.type());
        assertEquals("test.pdf", request.fields().get("filename"));
        assertEquals("1024", request.fields().get("size"));
    }

    @Test
    void rejectsStringWithoutFields() {
        assertThrows(RequestParseException.class, () -> parser.parse("INVALID_FORMAT_STRING"));
    }

    @Test
    void rejectsMalformedField() {
        assertThrows(RequestParseException.class, () -> parser.parse("LOGIN|user"));
    }

    @Test
    void rejectsDuplicateFields() {
        assertThrows(RequestParseException.class, () -> parser.parse("LOGIN|user=alice|user=bob"));
    }
}
