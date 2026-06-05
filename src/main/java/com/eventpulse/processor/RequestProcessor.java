package com.eventpulse.processor;

import com.eventpulse.config.ReloadableValidationRulesProvider;
import com.eventpulse.parser.ParsedRequest;
import com.eventpulse.parser.RequestParseException;
import com.eventpulse.parser.RequestParser;
import com.eventpulse.validation.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(RequestProcessor.class);

    private final RequestParser parser;
    private final ReloadableValidationRulesProvider rulesProvider;

    public RequestProcessor(RequestParser parser, ReloadableValidationRulesProvider rulesProvider) {
        this.parser = parser;
        this.rulesProvider = rulesProvider;
    }

    public ProcessingResult process(String rawRequest) {
        try {
            ParsedRequest request = parser.parse(rawRequest);
            rulesProvider.validator().validate(request);
            log.info("Processed request type={} fields={}", request.type(), request.fields().keySet());
            return ProcessingResult.success();
        } catch (RequestParseException exception) {
            log.warn("Parsing error for request='{}': {}", rawRequest, exception.getMessage());
            return ProcessingResult.failure(ProcessingStatus.PARSING_ERROR, exception.getMessage());
        } catch (ValidationException exception) {
            log.warn("Validation error for request='{}': {}", rawRequest, exception.getMessage());
            return ProcessingResult.failure(ProcessingStatus.VALIDATION_ERROR, exception.getMessage());
        } catch (RuntimeException exception) {
            log.error("Runtime error for request='{}'", rawRequest, exception);
            return ProcessingResult.failure(ProcessingStatus.RUNTIME_ERROR, exception.getMessage());
        }
    }
}
