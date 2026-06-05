package com.eventpulse.config;

import com.eventpulse.validation.ConfigurationException;
import com.eventpulse.validation.RequestValidator;
import com.eventpulse.validation.ValidationRules;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class ReloadableValidationRulesProvider {
    private final Path path;
    private final ValidationConfigLoader loader;
    private final AtomicReference<ValidationRules> currentRules = new AtomicReference<>();

    public ReloadableValidationRulesProvider(Path path, ValidationConfigLoader loader) throws ConfigurationException {
        this.path = Objects.requireNonNull(path, "path");
        this.loader = Objects.requireNonNull(loader, "loader");
        reload();
    }

    public void reload() throws ConfigurationException {
        currentRules.set(loader.load(path));
    }

    public RequestValidator validator() {
        return new RequestValidator(currentRules.get());
    }
}
