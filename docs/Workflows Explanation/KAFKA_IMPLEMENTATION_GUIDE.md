# Kafka Implementation Guide - SmartHealth Project

## Executive Summary

SmartHealth uses Apache Kafka as an **event streaming platform** to publish domain events and consume them for analytics processing. This enables asynchronous, scalable, and reliable event-driven workflows.

---

## Kafka Concepts & SmartHealth Implementation

### 1. **Event / Message**
**Definition:** A record containing a key, value, timestamp, and optional headers (e.g., `OrderPlaced: { "orderId": 101, "total": 50 }`).

**SmartHealth Implementation:**
- Location: [app/integrations/kafka_client.py](../app/integrations/kafka_client.py)
- All events follow a standardized envelope format defined in [docs/events.md](./events.md)

**Event Structure:**
```json
{
  "event_id": "uuid",
  "event_type": "appointment.created",
  "occurred_at": "2026-08-21T10:30:00Z",
  "version": 1,
  "schema_version": 1,
  "source": "smarthealth-api",
  "entity_type": "appointment",
  "entity_id": "101",
  "correlation_id": "correlation-id",
  "request_id": "request-id",
  "data": {
    "appointment_id": 101,
    "patient_id": 23,
    "provider_id": 7,
    "service_id": 14,
    "slot_id": 55,
    "status": "CONFIRMED"
  }
}
```

**Key Features:**
- **PHI Protection:** Denylist validates that patient names, emails, diagnoses, etc. are never included
- **Immutable:** Events represent completed business facts and are never modified
- **Traceable:** Every event includes `event_id`, `correlation_id`, `request_id` for end-to-end observability

---

### 2. **Topic**
**Definition:** A category or feed name to which records are published. Think of a topic as a folder in a filesystem, where events are files.

**SmartHealth Implementation:**
- Location: [app/workers/analytics_consumer.py](../app/workers/analytics_consumer.py#L42)
- Configuration: [app/core/settings.py](../app/core/settings.py#L32-L35)

**Topic Naming Convention:**
- Format: `{KAFKA_TOPIC_PREFIX}.{event_type}`
- Default prefix: `app` (configured via `KAFKA_TOPIC_PREFIX` env var)
- Topics are auto-created if enabled in Kafka config

**Topics Published by SmartHealth:**
```python
# From app/workers/analytics_consumer.py
topics = [
    "app.appointment.created",
    "app.appointment.cancelled",
    "app.appointment.rescheduled",
    "app.appointment.visit_status_changed",
    "app.service.published",
]
```

**Other Topics:**
- `app.service.created`
- `app.service.unpublished`
- `app.billing.*` (various billing events)

---

### 3. **Partition**
**Definition:** Topics are divided into partitions spread across different machines (brokers). Partitions are Kafka's unit of parallelism and scalability.

**SmartHealth Implementation:**
- Location: [docker-compose.yml](../docker-compose.yml#L212-L234) - Single broker setup
- Broker Configuration:
  ```yaml
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
  ```

**Current Setup:**
- **Single Broker:** For development. Production should use cluster (KRaft recommended)
- **Replication Factor:** 1 (configured in docker-compose)
- **Message Ordering:** Within a partition, messages are strictly ordered (important for state transitions)

---

### 4. **Offset**
**Definition:** A sequential, unique ID assigned to each message within a partition. It acts as a bookmark indicating where a consumer or partition currently is.

**SmartHealth Implementation:**
- Location: [app/workers/analytics_consumer.py](../app/workers/analytics_consumer.py#L29-L40)
- Consumer tracks offsets automatically with `enable_auto_commit=False` (manual commits for idempotency)

**Offset Management:**
```python
class AnalyticsConsumer:
    @property
    def consumer(self) -> KafkaConsumer:
        self._consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",      # Start from beginning if no offset stored
            enable_auto_commit=False,            # Manual commit for safety
            group_id=self.consumer_group,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
```

**Idempotency Pattern:**
```python
# From app/models/processed_event.py
class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    __table_args__ = (UniqueConstraint("event_id", "consumer", name="uq_processed_event_consumer"),)
    
    event_id = Column(String(128), nullable=False)
    consumer = Column(String(128), nullable=False)  # Consumer group name
    processed_at = Column(DateTime(timezone=True))
```

**Consumer commits offset only AFTER successfully processing:**
```python
# From app/workers/analytics_consumer.py
for message in self.consumer:
    try:
        payload = message.value
        self.process_message(payload, message.topic)
        self.consumer.commit()  # Commit only after success
    except Exception as exc:
        logger.exception("Failed to process analytics event")
        continue
```

---

### 5. **Producer**
**Definition:** Applications that publish (write) events to Kafka topics.

**SmartHealth Implementation:**
- Location: [app/integrations/kafka_client.py](../app/integrations/kafka_client.py#L76)
- Class: `KafkaEventPublisher`

**Producer Configuration:**
```python
class KafkaEventPublisher:
    def _get_producer(self) -> KafkaProducer | None:
        if not self.enabled or KafkaProducer is None:
            return None
        
        self._producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
            acks="all",              # Wait for all replicas
            retries=3,               # Retry on failure
            retry_backoff_ms=250,    # 250ms between retries
        )
```

**Key Settings:**
- `acks="all"` → Ensures durability; wait for broker acknowledgment
- `retries=3` → Automatic retry on transient failures
- `retry_backoff_ms=250` → Exponential backoff between retries

**Where Events Are Published:**

1. **Appointment Events** - [app/services/appointment_service.py](../app/services/appointment_service.py#L101-L110)
   ```python
   self.events.publish_appointment_event(
       "appointment.created",
       appointment_id=appointment.id,
       patient_id=patient.id,
       provider_id=appointment.provider_id,
       service_id=appointment.service_id,
       slot_id=appointment.slot_id,
       status=appointment.status.value,
   )
   ```

2. **Service Events** - [app/services/service_management.py](../app/services/service_management.py#L57)
   ```python
   HealthcareEventService(self.db).publish_service_event(
       "service.created",
       service_id=created.id,
       department_id=created.department_id,
       status=created.status.value
   )
   ```

3. **Billing Events** - [app/services/healthcare_event_service.py](../app/services/healthcare_event_service.py#L210-L211)
   ```python
   def publish_billing_event(
       self, event_type: str, *, billing_id: int, 
       appointment_id: int, amount: float, status: str
   ):
       return self.publish_resource_event(...)
   ```

---

### 6. **Consumer & Consumer Groups**
**Definition:** 
- A **Consumer** reads data from topics.
- A **Consumer Group** consists of multiple consumers working together. Each partition in a topic is consumed by exactly one consumer in the group, enabling horizontal scaling.

**SmartHealth Implementation:**

**Analytics Consumer (Main Consumer):**
- Location: [app/workers/analytics_consumer.py](../app/workers/analytics_consumer.py)
- Consumer Group Name: `app-analytics` (from `KAFKA_CONSUMER_GROUP` env var)
- Runs as a separate worker: `python -m app.workers.analytics_consumer`

**Consumer Subscription:**
```python
class AnalyticsConsumer:
    def _topics(self) -> list[str]:
        return [
            f"{self.topic_prefix}.appointment.created",
            f"{self.topic_prefix}.appointment.cancelled",
            f"{self.topic_prefix}.appointment.rescheduled",
            f"{self.topic_prefix}.appointment.visit_status_changed",
            f"{self.topic_prefix}.service.published",
        ]
    
    def run(self) -> None:
        if not settings.kafka_enabled:
            logger.info("Kafka analytics consumer is disabled")
            return
        
        self.consumer.subscribe(self._topics())
        logger.info("Analytics consumer started for topics: %s", self._topics())
```

**Message Processing:**
```python
def process_message(self, message: dict[str, Any], topic: str) -> None:
    # 1. Validate it's a JSON object
    if not isinstance(message, dict):
        raise ConsumerConfigError("Kafka payload must be a JSON object")
    
    # 2. Validate no PHI in payload
    if not self._is_safe_payload(message):
        raise ConsumerConfigError("Kafka payload contains forbidden PHI fields")
    
    # 3. Extract event_id for idempotency
    event_id = message.get("event_id")
    if not event_id:
        raise ConsumerConfigError("Kafka payload missing event_id")
    
    db = SessionLocal()
    try:
        repository = AnalyticsRepository(db)
        
        # 4. Track processed event (prevents duplicate processing)
        repository.stage_processed_event(
            str(event_id),
            str(message.get("event_type", "unknown")),
            topic,
            message,
        )
        
        # 5. Update metrics based on event type
        if "appointment" in topic:
            self._update_appointment_metrics(db, message)
        elif "service" in topic:
            self._update_service_metrics(db, message)
        
        repository.commit()
```

**Horizontal Scaling Example:**
- If you have 5 partitions and 2 consumers in `app-analytics` group:
  - Consumer 1 → partitions 0, 1, 2
  - Consumer 2 → partitions 3, 4
  - If a 3rd consumer joins → automatic rebalancing

---

### 7. **Broker & Cluster**
**Definition:** A single Kafka server is a **Broker**. Multiple brokers form a **Cluster**, coordinated via metadata quorum (KRaft).

**SmartHealth Implementation:**

**Development Setup (docker-compose.yml):**
- **Brokers:** 1 (single node)
- **Zookeeper:** Used for coordination (transitional - KRaft is modern alternative)
- **Network:** Internal (kafka:29092) and External (localhost:9092)

**Broker Configuration:**
```yaml
kafka:
  image: confluentinc/cp-kafka:7.5.0
  environment:
    KAFKA_BROKER_ID: 1
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
    KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
    KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

**Bootstrap Servers:**
- Internal (service-to-service): `kafka:29092`
- External (local dev): `localhost:9092`

**Health Check:**
- Location: [app/api/v1/endpoints/health.py](../app/api/v1/endpoints/health.py#L37-L48)
```python
def _check_kafka_connection() -> bool:
    if KafkaProducer is None:
        return False
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        request_timeout_ms=5000,
    )
    connected = producer.bootstrap_connected() is True
    producer.close(timeout=3)
    return connected
```

---

## Event Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      SmartHealth API                            │
│  (POST /api/v1/appointments, PATCH /api/v1/appointments/:id)   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│               AppointmentService.create()                       │
│  1. Run appointment saga (Temporal workflow)                    │
│  2. Publish event via HealthcareEventService                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│            KafkaEventPublisher.publish_event()                  │
│  1. Validate event (no PHI)                                      │
│  2. Build topic name: app.{event_type}                          │
│  3. Send to Kafka broker                                        │
│  4. Fallback: Store in outbox_events table if Kafka down        │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Kafka Broker                                  │
│  Topic: app.appointment.created (Partitions: 0, 1, ...)         │
│  Stores event durably on disk                                   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│           AnalyticsConsumer (python -m app.workers...)          │
│  Consumer Group: app-analytics                                  │
│  1. Subscribe to app.appointment.* and app.service.*            │
│  2. Receive message from broker (with offset)                   │
│  3. Validate payload safety (no PHI)                            │
│  4. Check idempotency (processed_events table)                  │
│  5. Update analytics_daily table                                │
│  6. Commit offset → move bookmark forward                       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│              Analytics Data Models                              │
│  - analytics_daily: Daily counters (created, cancelled, etc.)   │
│  - processed_events: Deduplication (event_id, consumer group)   │
│  - analytics_processed_events: Audit trail (topic, payload)     │
└──────────────────────────────────────────────────────────────────┘
```

---

## Key Files & Locations

| Component | File Path | Purpose |
|-----------|-----------|---------|
| **Producer** | [app/integrations/kafka_client.py](../app/integrations/kafka_client.py) | Publishes events to Kafka |
| **Event Service** | [app/services/healthcare_event_service.py](../app/services/healthcare_event_service.py) | High-level event API (appointment, service, billing) |
| **Consumer** | [app/workers/analytics_consumer.py](../app/workers/analytics_consumer.py) | Subscribes to topics, processes analytics |
| **Settings** | [app/core/settings.py](../app/core/settings.py) | Kafka configuration (broker, group, prefix) |
| **Models** | [app/models/outbox.py](../app/models/outbox.py), [app/models/processed_event.py](../app/models/processed_event.py) | Outbox pattern, idempotency tracking |
| **Docker** | [docker-compose.yml](../docker-compose.yml) | Kafka & Zookeeper setup |
| **Docs** | [docs/events.md](./events.md) | Event contracts and catalog |
| **Tests** | [tests/integration/test_docker_infrastructure.py](../tests/integration/test_docker_infrastructure.py) | Integration tests (pub/sub round-trip) |

---

## Configuration (Environment Variables)

| Variable | Default | Purpose |
|----------|---------|---------|
| `KAFKA_ENABLED` | `false` | Enable/disable Kafka |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker address |
| `KAFKA_CONSUMER_GROUP` | `app-analytics` | Consumer group name |
| `KAFKA_TOPIC_PREFIX` | `app` | Topic prefix (→ `app.appointment.created`) |

---

## Error Handling & Resilience

### 1. **Producer Failures**
```python
# Automatic retry with exponential backoff
acks="all"              # Wait for broker ACK
retries=3               # Retry 3 times
retry_backoff_ms=250    # 250ms delay, exponential
```

### 2. **Broker Unavailability → Outbox Pattern**
```python
# If Kafka is down, events fall back to database
class HealthcareEventService:
    def _save_outbox(self, event_type, entity_type, entity_id, payload, error):
        # Store in outbox_events table for later delivery
        self.outbox.add(OutboxEvent(...))
```

### 3. **Consumer Idempotency**
```python
# Duplicate events are skipped
# Unique constraint: (event_id, consumer_group)
UniqueConstraint("event_id", "consumer", name="uq_processed_event_consumer")
```

### 4. **PHI Protection**
```python
# Denylist prevents sensitive data
_DENYLIST_KEYS = {
    "name", "email", "phone", "dob", "address",
    "diagnosis", "symptoms", "notes", "medical_history"
}
```

---

## Security & Privacy

1. **No Authentication** (Current)
   - Note: `docker-compose.yml` uses `PLAINTEXT` protocol
   - Production requires: TLS/SSL, SASL authentication

2. **PHI Redaction**
   - Allowlist approach: Only whitelisted fields published
   - Denylist validation: Block sensitive keys
   - Correlation IDs separate from patient data

3. **Data Retention**
   - Kafka: Configurable retention (default: 7 days)
   - Analytics: Aggregated metrics, no raw data retention

---

## Monitoring & Debugging

### Check Kafka Health
```bash
# Via health endpoint
curl http://localhost:8000/api/v1/health

# Output includes kafka_status
{
  "status": "ok",
  "checks": {
    "kafka": "connected"
  }
}
```

### List Topics
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Monitor Consumer Lag
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group app-analytics \
  --describe
```

### View Events in Topic
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic app.appointment.created \
  --from-beginning \
  --max-messages 5
```

---

## Scalability & Production Considerations

### Current (Development)
- **Brokers:** 1 (single point of failure)
- **Partitions:** 1 per topic (sequential only)
- **Replication:** 1 (no fault tolerance)
- **Consumer:** 1 instance (no parallelism)

### Production Recommendations
1. **Cluster:** 3+ brokers with KRaft consensus
2. **Partitions:** 3-5 per topic (horizontal scaling)
3. **Replication Factor:** 3 (fault tolerance)
4. **Consumers:** 1 per partition max for parallelism
5. **Monitoring:** Prometheus + Grafana for broker/consumer metrics
6. **Retention:** Configure by size/time per business requirements
7. **Backup:** Regular snapshots of broker data

---

## Example: Creating a New Event Type

### Step 1: Define the Event
```python
# In app/services/healthcare_event_service.py
def publish_prescription_event(self, event_type: str, *, prescription_id: int, **metadata):
    return self.publish_resource_event(
        event_type,
        entity_type="prescription",
        entity_id=prescription_id,
        **metadata
    )
```

### Step 2: Publish on Business Change
```python
# In app/services/prescription_service.py
def create(self, payload):
    prescription = self.repo.create(payload)
    
    self.events.publish_prescription_event(
        "prescription.created",
        prescription_id=prescription.id,
        patient_id=prescription.patient_id,
        medication=prescription.medication
    )
    return prescription
```

### Step 3: Subscribe in Consumer
```python
# In app/workers/analytics_consumer.py
def _topics(self):
    topics = [
        ...existing topics...,
        f"{self.topic_prefix}.prescription.created",  # ADD THIS
    ]
    return topics

def process_message(self, message, topic):
    ...
    if "prescription" in topic:
        self._update_prescription_metrics(db, message)
```

### Step 4: Document
```markdown
# In docs/events.md
### prescription.created
Producer: Prescription service
Trigger: A new prescription is issued
Payload: prescription_id, patient_id, medication, dosage
Usage: Analytics, reminder notifications
```

---

## Summary

| Kafka Concept | SmartHealth Implementation |
|---------------|---------------------------|
| **Event** | Standardized JSON envelope with metadata, no PHI |
| **Topic** | `app.{entity}.{action}` (e.g., `app.appointment.created`) |
| **Partition** | Single broker/partition in dev; multi-broker in production |
| **Offset** | Tracked automatically; manual commit after processing |
| **Producer** | `KafkaEventPublisher` in every service layer |
| **Consumer** | `AnalyticsConsumer` with consumer group `app-analytics` |
| **Broker** | Single Zookeeper-managed broker in dev; KRaft cluster in prod |
| **Resilience** | Retry, outbox pattern, idempotency, PHI validation |

---

## Questions to Ask Your Supervisor

1. **Event Retention:** How long should events be retained in Kafka?
2. **Consumer Scalability:** Do we need multiple consumer instances?
3. **Security:** Should we add TLS and SASL authentication?
4. **Monitoring:** Should we integrate with existing observability stack?
5. **Production Deployment:** KRaft or Zookeeper for coordination?
