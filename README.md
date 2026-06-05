# EventPulse

EventPulse is a lightweight event-driven request processing platform built around Apache Kafka.

The project simulates a production-style backend architecture where incoming requests are pushed into Kafka, consumed asynchronously, processed by a thread pool, validated through external configuration, and later exposed through observability tooling such as Prometheus and Grafana.

## Architecture

```text
Request Generator
    ↓
Kafka Producer
    ↓
Kafka Topic
    ↓
Kafka Consumer
    ↓
Thread Pool
    ↓
Request Parser
    ↓
Request Validator
    ↓
Metrics Collection
    ↓
Prometheus
    ↓
Grafana Dashboard
```

## Full Project Goals

- Generate simulated request traffic at configurable intervals.
- Publish requests to Kafka through a producer.
- Consume Kafka messages without crashing on individual request failures.
- Process requests asynchronously through a configurable thread pool.
- Parse pipe-delimited request strings into structured objects.
- Validate requests through external YAML or JSON configuration.
- Support validation rule reloads and custom configuration uploads.
- Classify parsing, validation, configuration, and runtime errors.
- Collect request, error, Kafka, thread pool, and latency metrics.
- Visualize traffic, errors, Kafka health, thread pool state, and latency in Grafana.
- Run automated build, tests, integration tests, and coverage reporting in CI.

## Request Format

Requests use this format:

```text
TYPE|key=value|key=value
```

Examples:

```text
SIGNUP|user=alice|ip=192.168.1.1
LOGIN|user=alice|ip=192.168.1.1
MESSAGE|from=bob|to=taylor|content=hello
QUERY|keyword=kafka
UPLOAD|filename=test.pdf|size=1024
```

Malformed or unsupported requests are rejected and classified as failures:

```text
LOGIN|user=
UNKNOWN|abc
INVALID_FORMAT_STRING
```

## Configuration

Default YAML validation rules live in `src/main/resources/validation-rules.yml`.

A JSON version is also available at `src/main/resources/validation-rules.json`.

Runtime settings can be supplied through JVM system properties:

```text
kafka.bootstrap.servers=localhost:9092
kafka.topic=requests
kafka.group.id=eventpulse-v1
kafka.poll.timeout.ms=1000
validation.rules.path=/path/to/validation-rules.yml
threadpool.core.size=4
threadpool.max.size=8
threadpool.queue.capacity=1000
generator.interval.ms=1000
generator.invalid.rate.percent=5
generator.max.requests=0
```

Use a JSON validation file by pointing `validation.rules.path` to a `.json` file:

```bash
mvn compile exec:java -Dvalidation.rules.path=/Users/lebronjames/Documents/EventPulse/src/main/resources/validation-rules.json
```

## Install and Verify Kafka

On macOS, install Kafka with Homebrew:

```bash
brew install kafka
```

Verify the installed Kafka CLI tools:

```bash
kafka-topics --version
kafka-console-producer --version
```

Start Kafka as a background service:

```bash
brew services start kafka
```

Or start it in the foreground:

```bash
$(brew --prefix kafka)/bin/kafka-server-start $(brew --prefix)/etc/kafka/server.properties
```

Verify the broker is reachable:

```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

Stop the background service when needed:

```bash
brew services stop kafka
```

## Start Kafka

Kafka must be running before the EventPulse consumer starts.

Create the request topic:

```bash
kafka-topics --bootstrap-server localhost:9092 --create --topic requests --partitions 1 --replication-factor 1
```

If the topic already exists, Kafka will report that and you can continue.

## Run Tests

```bash
cd /Users/lebronjames/Documents/EventPulse
mvn test
```

## Start EventPulse

Start the Kafka consumer service in one terminal:

```bash
cd /Users/lebronjames/Documents/EventPulse
mvn compile exec:java
```

Start with explicit runtime settings:

```bash
cd /Users/lebronjames/Documents/EventPulse
mvn compile exec:java \
  -Dkafka.bootstrap.servers=localhost:9092 \
  -Dkafka.topic=requests \
  -Dkafka.group.id=eventpulse-v1 \
  -Dthreadpool.core.size=4 \
  -Dthreadpool.max.size=8 \
  -Dthreadpool.queue.capacity=1000
```

## Start Request Generator

Start the Kafka producer and simulated request generator in another terminal:

```bash
cd /Users/lebronjames/Documents/EventPulse
mvn -Pgenerator compile exec:java
```

Generate requests every 500 milliseconds, with 10% invalid traffic, then stop after 100 requests:

```bash
cd /Users/lebronjames/Documents/EventPulse
mvn -Pgenerator compile exec:java \
  -Dkafka.bootstrap.servers=localhost:9092 \
  -Dkafka.topic=requests \
  -Dgenerator.interval.ms=500 \
  -Dgenerator.invalid.rate.percent=10 \
  -Dgenerator.max.requests=100
```

`generator.max.requests=0` means the generator keeps running until stopped.

## Send Test Messages

Use Kafka's console producer:

```bash
kafka-console-producer --bootstrap-server localhost:9092 --topic requests
```

Then enter messages:

```text
MESSAGE|from=bob|to=taylor|content=hello
LOGIN|user=alice|ip=192.168.1.1
UPLOAD|filename=test.pdf|size=1024
```

Invalid examples:

```text
MESSAGE|from=bob|to=taylor
LOGIN|user=
INVALID_FORMAT_STRING
```

## V1 Implementation Status

Implemented in V1:

- Kafka producer and configurable simulated request generator.
- Kafka consumer subscribing to a configurable topic.
- Configurable thread pool with core size, max size, and queue capacity.
- Request parser for pipe-delimited request strings.
- YAML validation loader.
- JSON validation loader.
- Validation engine for allowed request types, required fields, numeric ranges, and string length constraints.
- Manual validation rule reload support through `ReloadableValidationRulesProvider.reload()`.
- Error classification for parsing, validation, configuration, and runtime failures.
- Unit tests for parser, validation, configuration loading, and processing error classification.
- GitHub Actions CI workflow for Maven tests.

Not implemented yet:

- Prometheus metrics.
- Grafana dashboard.
- User-facing configuration upload API.
- Integration tests with a real Kafka broker.
- Coverage threshold enforcement.
- Dead Letter Queue, Redis, Docker, Kubernetes, rate limiting, authentication, and frontend UI.
