# Operations Runbook

## Purpose

This runbook provides standard operational procedures for the SmartHealth platform, including startup, health validation, troubleshooting, and recovery for common production issues.

It is intended for engineering and site-reliability use and should be kept current with the live system environment.

## System Overview

SmartHealth is a FastAPI-based healthcare platform with:

- PostgreSQL persistence
- Redis-backed idempotency and cache access
- Celery background workers
- Kafka event streaming
- Temporal workflow orchestration
- Prometheus metrics exposure
- structured JSON logging with correlation IDs

## Service Topology

The principal runtime services are:

- API service: provides the HTTP API and Swagger docs
- Celery worker: processes asynchronous tasks
- analytics consumer: consumes stream events for analytics processing
- PostgreSQL: application database
- Redis: idempotency and task cache state
- Kafka: event publishing and consumer integration
- Temporal: service publish workflow orchestration

## Health Checks

### API health

```bash
curl http://localhost:8000/health
```

Expected result:

```json
{"status": "ok"}
```

### Metrics endpoint

```bash
curl http://localhost:8000/metrics
```

This should return Prometheus-formatted metrics and is the primary endpoint for scraping.

### Service dependencies

Verify:

- PostgreSQL on port 5432
- Redis on port 6379
- Kafka on port 9092 / 29092
- Temporal on port 7233

---

## Log and Trace Review

### Correlation ID usage

All request and workflow flows should carry a correlation ID. Validate logs using the same request ID or correlation ID across application and worker components.

### JSON log expectations

Logs should be structured JSON and must not contain PHI or personal user content. Use the correlation ID and request ID fields to trace activity across the system.

### Examples

Look for fields such as:

- timestamp
- level
- logger
- message
- operation
- correlation_id
- request_id
- user_id
- appointment_id

---

## Common Incident Response

### Integration verification

Run the application-level checks first:

```bash
pytest -q
```

Run integration tests with the required backend dependencies available:

```bash
alembic upgrade head
pytest -q tests/integration
```

For Temporal recovery, interrupt a publish or booking worker, inspect workflow history, restart the worker, and verify the workflow reaches its terminal state without duplicate appointments or content chunks.

For Kafka failure recovery, stop Kafka and trigger a domain event. Confirm the event is in `outbox_events` with `status = 'PENDING'` and a stable `event_id`. Restart Kafka and run:

Run the configured outbox publisher task through the Celery worker.

Confirm the same `event_id` is published once and the row has `status = 'PUBLISHED'` and a non-null `published_at`.

For PostgreSQL booking contention, use the PostgreSQL `DATABASE_URL` and run the concurrency test with at least 50 workers. The expected result is one confirmed appointment, one reserved slot, and all other attempts rejected.

### 1. API is not responding

Check:

1. Docker containers state
2. process logs for the API service
3. environment variables and database reachability
4. database connectivity and migration state

Commands:

Inspect the API process logs and dependency health endpoints.

### 2. Database connection errors

Verify:

- Postgres container is running
- .env values match the expected database settings
- migrations are applied

Command:

```bash
alembic upgrade head
```

### 3. Celery worker stuck or not processing jobs

Check:

Inspect the Celery worker logs.

Also verify:

- Redis is available
- PostgreSQL is reachable
- broker settings are valid
- worker startup command is correct

### 4. Temporal workflow errors

Check:

- Temporal container health
- workflow execution logs and workflow status
- service publish workflow activity failures

Inspect the Temporal workflow history and task queue state.

### 5. Metrics endpoint empty or not scraping

Check:

- app is running and /metrics is enabled
- Prometheus target config is correct
- HTTP middleware is not skipping required endpoints unintentionally
- the service is exposing counters and histograms without registration errors

---

## Recovery Procedures

### Process recovery

Restart only the affected API, worker, consumer, or workflow worker after capturing logs and checking in-flight work. A restart must not create a new appointment or event identity.

### Database recovery

If the schema is inconsistent:

```bash
alembic current
alembic upgrade head
```

Do not drop tables or delete data to resolve an application symptom. Use an Alembic migration or an audited repository-level repair.

---

## Monitoring and Alerting Checklist

The following should be monitored in normal operation:

- API latency and error rates
- task queue backlogs
- worker crashes or restarts
- database connection issues
- Kafka consumer lag or delivery failures
- workflow failures in Temporal
- Prometheus scrape success for /metrics

## Escalation Guide

Escalate to the engineering owner when:

- API is unavailable for a sustained period
- repeated workflow failures block core business operations
- data integrity issues appear in appointments, billing, or slot availability
- metrics are missing or stale for production monitoring
- event consumers fail to process the critical event stream

## Documentation Maintenance

This runbook should be reviewed when:

- services are added or removed
- backend runtime components change
- observability pipelines change
- event contracts or background job flows change
- monitoring or alert rules change

## Related Documentation

- [design.md](design.md)
- [events.md](events.md)
- [STRUCTURED_LOGGING.md](STRUCTURED_LOGGING.md)
