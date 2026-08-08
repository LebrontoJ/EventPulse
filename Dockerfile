# syntax=docker/dockerfile:1

# ---- Build stage: compile and package a single runnable jar with all dependencies ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Cache dependencies in their own layer so `docker build` doesn't re-download them on every
# source change.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q -DskipTests package

# ---- Runtime stage: small JRE-only image ----
FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S eventpulse && adduser -S eventpulse -G eventpulse
WORKDIR /app
COPY --from=build /build/target/eventpulse-*.jar /app/app.jar
USER eventpulse

# Same jar serves both entry points; pick one at `docker run` time by overriding the command,
# e.g.: docker run eventpulse com.eventpulse.app.RequestGeneratorApplication
ENTRYPOINT ["java", "-cp", "/app/app.jar"]
CMD ["com.eventpulse.app.EventPulseApplication"]
