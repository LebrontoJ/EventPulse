# EventPulse V1 Design

## Scope

V1 focuses on the core request processing path:

1. Simulated request generator creates valid and intentionally invalid requests.
2. Kafka producer publishes generated requests to the request topic.
3. Kafka consumer polls request messages.
4. Each batch is submitted to a configurable thread pool.
5. Parser converts raw strings into structured requests.
6. YAML or JSON validation rules enforce allowed request types and field constraints.
7. Processing failures are classified and logged without stopping the consumer loop.

Prometheus metrics, Docker (both the app image and Kafka via docker-compose), and a dead letter queue
with retry/rebalance handling have since been added on top of this V1 scope (see the README's V1
Implementation Status). Grafana, the upload API, and Kubernetes are still reserved for later versions.

## Request Format

Requests use pipe-delimited text:

```text
TYPE|key=value|key=value
```

Parser rules:

- Type must match `[A-Z][A-Z0-9_]*`.
- At least one key-value field is required.
- Field keys must match `[A-Za-z][A-Za-z0-9_]*`.
- Duplicate fields are rejected.
- Empty values are syntactically allowed, then rejected by validation when the field is required.

Example:

```text
MESSAGE|from=bob|to=taylor|content=hello
```

## Validation Configuration

Rules can be loaded from YAML or JSON.

Default files:

- `src/main/resources/validation-rules.yml`
- `src/main/resources/validation-rules.json`

YAML example:

```yaml
requestTypes:
  UPLOAD:
    filename:
      required: true
      minLength: 1
      maxLength: 255
    size:
      required: true
      min: 1
      max: 104857600
```

Supported checks:

- Allowed request types
- Required fields
- Numeric `min` / `max`
- String `minLength` / `maxLength`

Manual reload is supported through `ReloadableValidationRulesProvider.reload()`.

The application selects the loader from the `validation.rules.path` file extension:

- `.yml` / `.yaml`: YAML
- `.json`: JSON

## Kafka Producer and Generator

Runtime settings:

- `kafka.bootstrap.servers`
- `kafka.topic`
- `kafka.producer.client.id`
- `generator.interval.ms`
- `generator.invalid.rate.percent`
- `generator.max.requests`

Generation behavior:

- `generator.invalid.rate.percent` defaults to `5`.
- `generator.max.requests=0` means run until stopped.
- `MESSAGE` requests include `from`, `to`, and `content`.

## Thread Pool

Runtime settings:

- `threadpool.core.size`
- `threadpool.max.size`
- `threadpool.queue.capacity`

Exposed runtime values:

- active thread count
- queued task count
- completed task count

## Kafka Consumer

Runtime settings:

- `kafka.bootstrap.servers`
- `kafka.topic`
- `kafka.group.id`
- `kafka.poll.timeout.ms`

Offset behavior:

- Auto commit is disabled.
- Each polled batch is processed concurrently.
- Offsets are committed only after the batch futures complete.
- Per-record parse and validation failures are classified by `RequestProcessor` and do not fail the batch.

## Error Classification

- `PARSING_ERROR`: malformed request string
- `VALIDATION_ERROR`: unsupported type or rule violation
- `RUNTIME_ERROR`: unexpected processing exception
- `CONFIGURATION_ERROR`: invalid YAML or invalid validation schema, raised during rule loading
