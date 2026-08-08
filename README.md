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

## Quick Start

There are two ways to run EventPulse. Pick one — mixing them causes port conflicts on `9404`/`9405`
and duplicate consumers fighting over the same Kafka consumer group.

**Option A: everything in Docker (fastest way to see it working)**

```bash
cd /Users/lebronjames/Documents/EventPulse
docker compose --profile app up -d --build
docker compose logs -f app generator
```

This starts Kafka, the consumer (`app`), and the request generator (`generator`) together. Rerun the
same command with `--build` after any code change to rebuild the image.

**Option B: Kafka in Docker, apps run locally via Maven (for iterating on code)**

```bash
cd /Users/lebronjames/Documents/EventPulse
docker compose up -d          # Kafka only
mvn compile exec:java         # terminal A: consumer
mvn -Pgenerator compile exec:java   # terminal B: generator
```

See [Start Kafka with Docker Compose](#start-kafka-with-docker-compose),
[Run the Application in Docker](#run-the-application-in-docker), and
[Start EventPulse](#start-eventpulse) below for details and options on both paths.

### View It Running

- Logs: `docker compose logs -f app generator` (Docker) or the terminal output (Maven)
- Consumer metrics: `curl http://localhost:9404/metrics`
- Generator metrics: `curl http://localhost:9405/metrics`
- Kafka topic browser: `docker compose --profile ui up -d`, then open `http://localhost:8080`

## Project Structure

```text
EventPulse/
├── pom.xml                         Maven dependencies, plugins, and run profiles
├── Dockerfile                      Multi-stage build producing a runnable image for both apps
├── .dockerignore                   Excludes target/, .git/, docs/, etc. from the Docker build context
├── docker-compose.yml              One-command local Kafka (KRaft) + optional Kafka UI/Prometheus/app containers
├── prometheus.yml                  Prometheus scrape config for the optional docker-compose service
├── docs/
│   └── v1-design.md                V1 architecture and design decisions
├── src/
│   ├── main/
│   │   ├── java/com/eventpulse/
│   │   │   ├── app/                Consumer and generator application entry points
│   │   │   ├── config/             Application config loading and YAML/JSON validation configuration loading
│   │   │   ├── dlq/                Dead letter queue settings, envelope, and publisher
│   │   │   ├── error/              ErrorCode taxonomy and the HasErrorCode exception interface
│   │   │   ├── generator/          Simulated valid and invalid request generation
│   │   │   ├── kafka/              Kafka producer, consumer, and their settings
│   │   │   ├── metrics/            Prometheus metrics collection and HTTP exposition
│   │   │   ├── parser/             Raw request parsing and parsed request model
│   │   │   ├── processor/          Parse-and-validate processing pipeline
│   │   │   ├── threading/          Configurable request processing thread pool
│   │   │   └── validation/         Validation rules, engine, and exceptions
│   │   └── resources/
│   │       ├── application.properties
│   │       ├── validation-rules.yml
│   │       └── validation-rules.json
│   └── test/java/com/eventpulse/   Unit tests organized by application package
├── .github/workflows/ci.yml        GitHub Actions build and test workflow
└── target/                         Maven-generated classes, reports, and coverage
```

### Main Packages

| Package | Key files and responsibilities |
| --- | --- |
| `app` | `EventPulseApplication` starts the Kafka consumer and processing pipeline. `RequestGeneratorApplication` starts the simulated request producer. |
| `config` | `ApplicationConfig` resolves layered runtime settings (bundled defaults, external file, `-D` overrides). Also loads YAML or JSON validation files, verifies their schema, selects the loader by file extension, and supports manual rule reloads. |
| `dlq` | `DeadLetterPublisher` publishes permanently-failed messages (as a `DeadLetterEnvelope` JSON payload) to the configured dead letter queue topic. |
| `error` | `ErrorCode` gives every failure mode a stable code, description, and retryability flag. `HasErrorCode` lets exceptions expose their code uniformly. |
| `generator` | Stores generator settings and creates realistic requests with a configurable invalid-request percentage. |
| `kafka` | Contains Kafka producer/consumer implementations and their validated runtime settings. The consumer retries retryable failures, routes permanent failures to the DLQ, and commits offsets before giving up partitions in a rebalance. |
| `metrics` | `EventPulseMetrics` collects Prometheus counters/histogram/gauges and optionally serves them over HTTP at `/metrics`. |
| `parser` | Converts strings such as `MESSAGE\|from=bob\|to=taylor\|content=hello` into structured `ParsedRequest` objects. |
| `processor` | Coordinates parsing and validation, then classifies results as success, parsing error, validation error, or runtime error. |
| `threading` | Creates the configurable thread pool and exposes active, queued, and completed task counts. |
| `validation` | Defines validation rules and checks allowed request types, required fields, numeric ranges, and string lengths. |

### Supporting Files

| Path | Purpose |
| --- | --- |
| `docker-compose.yml` | Starts a single-node KRaft Kafka broker on `localhost:9092`, auto-creates the `requests` and `requests-dlq` topics, and optionally starts Kafka UI and/or Prometheus. |
| `prometheus.yml` | Scrape config used by the optional `prometheus` docker-compose service. |
| `src/main/resources/application.properties` | Bundled default runtime settings (Kafka, retry/DLQ, thread pool, generator, validation path, metrics). |
| `src/main/resources/validation-rules.yml` | Default YAML request validation rules. |
| `src/main/resources/validation-rules.json` | Equivalent JSON request validation rules. |
| `src/test/java/com/eventpulse/` | Unit tests for parsing, generation, processing, validation and application configuration, settings records, the thread pool, Kafka producer/consumer, and Prometheus metrics. |
| `docs/v1-design.md` | More detailed V1 behavior and architecture notes. |
| `.github/workflows/ci.yml` | Runs `mvn verify` (tests + JaCoCo coverage gate + shaded-jar build), uploads test/coverage reports, and builds the Docker image, on pushes and pull requests. |
| `target/` | Generated by Maven; contains compiled classes, test reports, and JaCoCo coverage reports. |

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

Runtime settings (`kafka.*`, `threadpool.*`, `generator.*`, `validation.rules.path`) are resolved by
`ApplicationConfig` in this order, lowest to highest precedence:

1. the bundled defaults in `src/main/resources/application.properties`
2. an external `.properties` file, if configured via `-Dconfig.file=/path/to/file.properties` or the
   `EVENTPULSE_CONFIG_FILE` environment variable
3. individual `-D` JVM system properties, which always win

This means the common case is maintaining one config file per environment instead of passing every
setting on the command line, while `-D` overrides still work for quick, one-off changes (e.g. in CI).

Recognized keys and their defaults:

```text
kafka.bootstrap.servers=localhost:9092
kafka.topic=requests
kafka.group.id=eventpulse-v1
kafka.poll.timeout.ms=1000
kafka.producer.client.id=eventpulse-generator
kafka.processing.max.attempts=3
kafka.dlq.topic=requests-dlq
kafka.dlq.producer.client.id=eventpulse-dlq-producer
validation.rules.path=
threadpool.core.size=4
threadpool.max.size=8
threadpool.queue.capacity=1000
generator.interval.ms=1000
generator.invalid.rate.percent=5
generator.max.requests=0
metrics.enabled=true
metrics.port=9404
generator.metrics.port=9405
```

Only keys already present in `application.properties` can be overridden by an external file or a
`-D` property, so that file is the single place documenting every supported setting.

### Reliability: Retries, Dead Letter Queue, and Error Codes

Every failure mode has a precise `ErrorCode` (`com.eventpulse.error.ErrorCode`), each carrying a
stable code, a description, and whether it's worth retrying:

| Error code | Meaning | Retried? |
| --- | --- | --- |
| `EP-1000` `PARSE_ERROR` | Request could not be parsed | No - deterministic, same input always fails the same way |
| `EP-2000` `VALIDATION_ERROR` | Request failed validation rules | No - deterministic |
| `EP-3000` `RUNTIME_ERROR` | Unexpected error while processing | Yes - may be transient |
| `EP-4000` `CONFIGURATION_ERROR` | Application configuration could not be loaded | N/A - startup failure |
| `EP-5000` `DEAD_LETTER_PUBLISH_ERROR` | Failed to publish a message to the DLQ itself | N/A |

`RequestParseException`, `ValidationException`, and `ConfigurationException` all implement
`HasErrorCode`, so callers can get a precise code without inspecting the exception's class or message.

Only `RUNTIME_ERROR` results are retried, up to `kafka.processing.max.attempts` (default 3) - parsing
and validation errors skip straight to the dead letter queue since retrying them would just reproduce
the same failure. Whatever the final outcome, any non-success result is published to the
`kafka.dlq.topic` topic (default `requests-dlq`) as a JSON `DeadLetterEnvelope`: original topic,
partition, offset, key, raw request, error code, error message, attempt count, and failure timestamp -
enough context to inspect or manually replay the message later. The offset is still committed either
way, so a bad message never blocks the partition.

The consumer also registers a `ConsumerRebalanceListener`: when a partition is about to be revoked
(the consumer group rebalances), it commits the current offsets synchronously first - the standard
Kafka pattern for avoiding reprocessing of already-completed work once another consumer picks up the
partition. Both halves of a rebalance are recorded as `eventpulse_consumer_rebalance_events_total`.

### Prometheus Metrics

When `metrics.enabled=true` (the default), each app starts its own Prometheus HTTP endpoint:

- `EventPulseApplication` (consumer): `http://localhost:9404/metrics`
- `RequestGeneratorApplication` (generator): `http://localhost:9405/metrics`

Different default ports let both apps run on the same host at the same time, matching the two-terminal
workflow below. Metrics exposed:

- `eventpulse_requests_total{status}` - requests processed, by outcome (`success`, `parsing_error`,
  `validation_error`, `runtime_error`)
- `eventpulse_request_processing_duration_seconds` - time spent parsing and validating a request
- `eventpulse_kafka_commits_total` / `eventpulse_kafka_commit_failures_total`
- `eventpulse_generator_requests_sent_total` - requests sent by the generator
- `eventpulse_threadpool_active_threads` / `_queued_tasks` / `_completed_tasks`
- `eventpulse_dead_letter_total{error_code}` - messages routed to the dead letter queue
- `eventpulse_dead_letter_publish_failures_total` - failures publishing to the DLQ itself
- `eventpulse_processing_retries_total` - request processing retry attempts
- `eventpulse_consumer_rebalance_events_total{event_type}` - Kafka consumer group rebalance events
- standard `jvm_*` / `process_*` metrics

Disable the HTTP endpoint (metrics are still collected in-process, just not exposed) with
`-Dmetrics.enabled=false`.

Example: point at an external config file instead of using `-D` flags:

```bash
mvn compile exec:java -Dconfig.file=/path/to/eventpulse.properties
```

Use a JSON validation file by pointing `validation.rules.path` to a `.json` file (works the same way
whether set in a config file or via `-D`):

```bash
mvn compile exec:java -Dvalidation.rules.path=/Users/lebronjames/Documents/EventPulse/src/main/resources/validation-rules.json
```

## Start Kafka with Docker Compose

The simplest way to get Kafka running locally is `docker-compose.yml` at the project root. It starts a
single-node KRaft broker, reachable at `localhost:9092`, and automatically creates the `requests` topic.

```bash
cd /Users/lebronjames/Documents/EventPulse
docker compose up -d
```

Check that the broker is healthy:

```bash
docker compose ps
```

Optionally start Kafka UI (topic/message browser at `http://localhost:8080`):

```bash
docker compose --profile ui up -d
```

Optionally start Prometheus (`http://localhost:9090`), configured to scrape the EventPulse apps
running on the host at `metrics.port` / `generator.metrics.port` (see `prometheus.yml`):

```bash
docker compose --profile metrics up -d
```

Stop everything:

```bash
docker compose down
```

## Run the Application in Docker

`Dockerfile` builds a single image that can run either app; the same jar serves both entry points, and
`docker-compose.yml` starts them alongside Kafka via a dedicated `app` profile.

```bash
docker compose --profile app up -d --build
```

This starts (in addition to Kafka) an `eventpulse-app` container running `EventPulseApplication`
(metrics on `localhost:9404`) and an `eventpulse-generator` container running
`RequestGeneratorApplication` (metrics on `localhost:9405`), both pointed at the containerized broker
via `JAVA_TOOL_OPTIONS=-Dkafka.bootstrap.servers=kafka:19092`. `prometheus.yml` scrapes both the
container service names and `host.docker.internal`, so `docker compose --profile metrics up -d` works
whether the apps run in Docker or directly on the host.

Don't also run `mvn compile exec:java` / `mvn -Pgenerator compile exec:java` on the host while these
containers are up — both bind the same `9404`/`9405` metrics ports and join the same Kafka consumer
group, so the second process to start will fail with `Address already in use`. Stop the containers
first (`docker compose stop app generator`) if you want to switch to running the apps locally.

After changing code, rebuild the image before restarting: `docker compose --profile app up -d --build`.
Tail logs with `docker compose logs -f app generator`.

To build and run the image manually instead:

```bash
docker build -t eventpulse:local .

# Consumer (default CMD)
docker run --rm -p 9404:9404 -e JAVA_TOOL_OPTIONS="-Dkafka.bootstrap.servers=host.docker.internal:9092" eventpulse:local

# Generator (override the entry point class)
docker run --rm -p 9405:9405 -e JAVA_TOOL_OPTIONS="-Dkafka.bootstrap.servers=host.docker.internal:9092" eventpulse:local com.eventpulse.app.RequestGeneratorApplication
```

Any `-D` setting normally passed on the command line (e.g. `-Dconfig.file=...`, `-Dmetrics.port=...`)
can be supplied the same way via `JAVA_TOOL_OPTIONS`. The image runs as a non-root user and the runnable
jar is built by the `maven-shade-plugin`, which only runs during `mvn package` (not `mvn test`), so it
doesn't affect the CI test workflow.

## Install and Verify Kafka (without Docker)

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

Create the request and dead letter queue topics (already done automatically by `docker compose up -d`;
only needed for the Homebrew path):

```bash
kafka-topics --bootstrap-server localhost:9092 --create --topic requests --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic requests-dlq --partitions 1 --replication-factor 1
```

If a topic already exists, Kafka will report that and you can continue.

## Run Tests

```bash
cd /Users/lebronjames/Documents/EventPulse
mvn test
```

## Start EventPulse

Alternative to `docker compose --profile app up -d` (Option B in Quick Start): run the apps directly
on the host against the Dockerized (or Homebrew) Kafka broker. Don't run both at once — see the note
in [Run the Application in Docker](#run-the-application-in-docker).

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
- Layered application configuration (`ApplicationConfig`): bundled defaults, external config file, `-D` overrides.
- Docker Compose for one-command local Kafka (KRaft) with automatic topic creation and optional Kafka UI.
- Prometheus metrics (`EventPulseMetrics`): request outcome counts, processing latency, Kafka commit
  success/failure, thread pool gauges, and generator throughput, exposed over HTTP and scrapeable via
  the optional `docker compose --profile metrics` Prometheus service.
- Docker image for the application itself (`Dockerfile`, multi-stage build via `maven-shade-plugin`),
  runnable standalone or via the `docker compose --profile app` services.
- Unit tests for parser, validation, configuration loading, application configuration, settings records, the thread pool, Kafka producer/consumer (via `MockProducer`/`MockConsumer`), Prometheus metrics, and processing error classification.
- GitHub Actions CI workflow: `mvn verify` (tests + JaCoCo coverage threshold gate + shaded-jar build),
  test/coverage report artifact upload, and a Docker image build check, on every push and pull request.
- Coverage threshold enforcement (`jacoco-maven-plugin` `check` goal, bound to the `verify` phase,
  minimum 50% line coverage; application entry-point `main()` classes are excluded since they require a
  live Kafka broker and aren't realistically unit-testable).
- Dead letter queue (`DeadLetterPublisher`): runtime errors are retried up to `kafka.processing.max.attempts`
  (default 3), parsing/validation errors skip straight to the DLQ, and any permanently-failed message is
  published as a JSON `DeadLetterEnvelope` to the `requests-dlq` topic.
- Precise error taxonomy (`com.eventpulse.error.ErrorCode`, `HasErrorCode`): every exception exposes a
  stable code, description, and retryability flag instead of callers inferring failure type from the
  exception class or message.
- `ConsumerRebalanceListener` on the Kafka consumer: commits offsets synchronously before partitions are
  revoked, avoiding reprocessing after a consumer group rebalance.

Not implemented yet:

- Grafana dashboard.
- User-facing configuration upload API.
- Integration tests with a real Kafka broker (e.g. via Testcontainers).
- Redis, Kubernetes, rate limiting, authentication, and frontend UI.
