package com.eventpulse.parser;

import com.eventpulse.error.ErrorCode;
import com.eventpulse.error.HasErrorCode;

public class RequestParseException extends Exception implements HasErrorCode {
    public RequestParseException(String message) {
        super(message);
    }

    @Override
    public ErrorCode errorCode() {
        return ErrorCode.PARSE_ERROR;
    }
}
