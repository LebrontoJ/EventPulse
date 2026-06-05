package com.eventpulse.config;

import com.eventpulse.validation.ConfigurationException;
import com.eventpulse.validation.ValidationRules;

import java.nio.file.Path;

public interface ValidationConfigLoader {
    ValidationRules load(Path path) throws ConfigurationException;
}
