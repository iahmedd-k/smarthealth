# Complete End-to-End Application Flows - SmartHealth

## Overview

This document shows complete journeys through the SmartHealth system, including:
1. Appointment booking (Temporal saga)
2. Service publication (Temporal workflow + Celery)
3. Reminder notifications (Celery scheduled task)
4. Analytics processing (Kafka consumer)

---

## Flow 1: Patient Books Appointment (Complete Journey)

Patients use `POST /api/v1/appointments` to book an available slot. The saga reserves the slot internally. There is no separate patient-facing `/slots/{slot_id}/reserve` step; patients use the waitlist flow when the slot is unavailable.

### Timeline & Components

```
T=0s    USER ACTION
        Patient opens web app, selects:
        - Provider: Dr. Smith
        - Service: Dental Cleaning  
        - Slot: 2026-09-15 10:00 AM
        - Idempotency Key: "booking-abc-123"

T=0.1s  HTTP REQUEST
        POST /api/v1/appointments
        Headers: {
          X-Correlation-ID: corr-xyz-789,
          X-Request-ID: req-123-abc,
          Idempotency-Key: booking-abc-123
        }
        Body: {slot_id: 55, ...}

T=0.2s  API ROUTE HANDLER (app/api/v1/endpoints/appointments.py)
        └─ appointmentService.create(payload, current_user, idempotency_key)

T=0.3s  IDEMPOTENCY CHECK (Redis)
        └─ idempotency_store.get(user_id=42, idempotency_key="booking-abc-123")
           ├─ Not found (first request)
           └─ Proceed to saga

T=0.4s  APPOINTMENT SERVICE (app/services/appointment_service.py)
        ├─ Validate patient exists
        ├─ Validate slot is AVAILABLE
        └─ Call: await run_appointment_saga(workflow_payload)

T=0.5s  TEMPORAL CLIENT
        └─ Connect to Temporal server (temporal:7233)
        └─ Submit workflow:
           {
             workflow: AppointmentSagaWorkflow.execute,
             workflow_id: "appointment_saga_42_55_xyz123",
             task_queue: "smarthealth",
             args: {
               patient_id: 42,
               slot_id: 55,
               correlation_id: corr-xyz-789,
               request_id: req-123-abc,
               idempotency_key: booking-abc-123,
               provider_id: 7,
               service_id: 14,
             }
           }

T=1s    TEMPORAL SERVER (Stores history entry)
        ├─ WorkflowStarted
        ├─ Creates task for worker
        └─ Waits for worker to pick up

T=1.5s  TEMPORAL WORKER (app/workers/service_publish_worker.py)
        └─ Polls task queue "smarthealth"
        └─ Sees: AppointmentSagaWorkflow.execute()
        └─ Starts workflow execution

T=2s    ACTIVITY 1: validate_appointment_data
        ├─ Activity code (app/temporal/activities.py)
        ├─ Query DB: SELECT patient WHERE id=42
        ├─ Query DB: SELECT slot WHERE id=55
        ├─ Validate: slot.status = AVAILABLE ✅
        ├─ Return: {patient_id: 42, slot_id: 55, provider_id: 7, service_id: 14}
        └─ Temporal records: ActivityCompleted

T=3s    ACTIVITY 2: reserve_slot
        ├─ Activity code (app/temporal/activities.py)
        ├─ Query DB: SELECT slot WHERE id=55
        ├─ UPDATE slot SET status='RESERVED' WHERE id=55
        ├─ COMMIT transaction
        ├─ Return: {status: "RESERVED"}
        └─ Temporal records: ActivityCompleted

T=4s    ACTIVITY 3: create_pending_appointment
        ├─ Activity code (app/temporal/activities.py)
        ├─ INSERT INTO appointments (patient_id, slot_id, status='REQUESTED')
        ├─ COMMIT transaction
        ├─ Return: {appointment_id: 456}
        └─ Temporal records: ActivityCompleted

T=5s    ACTIVITY 4: run_billing_precheck
        ├─ Activity code (app/temporal/activities.py)
        ├─ Query: Patient insurance, appointment service price
        ├─ Check: Coverage available? ✅
        ├─ INSERT INTO billing (appointment_id, amount, status='APPROVED')
        ├─ COMMIT transaction
        ├─ Return: {status: "APPROVED", amount: 150.00}
        └─ Temporal records: ActivityCompleted

T=6s    ACTIVITY 5: send_reminder
        ├─ Activity code (app/temporal/activities.py)
        ├─ INSERT INTO notifications (appointment_id, type='APPOINTMENT_REMINDER')
        ├─ COMMIT transaction
        ├─ Return: {notification_id: 789}
        └─ Temporal records: ActivityCompleted

T=7s    ACTIVITY 6: confirm_appointment
        ├─ Activity code (app/temporal/activities.py)
        ├─ UPDATE appointments SET status='CONFIRMED' WHERE id=456
        ├─ COMMIT transaction
        ├─ Return: {status: "CONFIRMED"}
        └─ Temporal records: ActivityCompleted

T=8s    ACTIVITY 7: publish_appointment_created_event
        ├─ Activity code (app/temporal/activities.py)
        ├─ Call: HealthcareEventService(db).publish_appointment_event(...)
        │   └─ event_type: "appointment.created"
        │   └─ entity_type: "appointment"
        │   └─ entity_id: 456
        │   └─ correlation_id: corr-xyz-789
        │   └─ data: {appointment_id: 456, patient_id: 42, ...}
        │
        ├─ Call: KafkaEventPublisher.publish_event(...)
        │   ├─ Validate no PHI ✅
        │   ├─ Build topic: "app.appointment.created"
        │   ├─ Serialize event
        │   └─ SEND to Kafka broker ✅
        │       ├─ Broker ACKs
        │       └─ Records at offset 12345, partition 0
        │
        └─ Temporal records: ActivityCompleted

T=9s    WORKFLOW COMPLETE
        └─ Temporal records: WorkflowCompleted
        └─ Returns result to client: {appointment_id: 456, status: "CONFIRMED"}

T=9.5s  BACK IN API HANDLER
        ├─ Receive result: {appointment_id: 456}
        ├─ Cache in Redis: idempotency_store.set(42, "booking-abc-123", {appointment_id: 456})
        └─ Return HTTP 200 to client

T=10s   HTTP RESPONSE TO PATIENT
        ├─ Status: 200 OK
        ├─ Body: {
        │   appointment_id: 456,
        │   provider_id: 7,
        │   slot_id: 55,
        │   status: "CONFIRMED",
        │   scheduled_at: "2026-09-15T10:00:00Z"
        │ }
        └─ Estimated round-trip: ~10 seconds ✅

========== PARALLEL ASYNC PROCESSING ==========

T=2s    KAFKA CONSUMER (app/workers/analytics_consumer.py)
        └─ Subscribed to: app.appointment.* topics
        └─ Polls broker periodically
        └─ (Currently idle, waiting for messages)

T=8.5s  KAFKA MESSAGE RECEIVED
        ├─ Topic: app.appointment.created
        ├─ Offset: 12345
        ├─ Message: {event_id: "uuid-123", appointment_id: 456, ...}
        │
        ├─ Validate: Is JSON? ✅
        ├─ Validate: No PHI? ✅
        ├─ Validate: Has event_id? ✅
        │
        ├─ Idempotency check:
        │  └─ SELECT * FROM processed_events
        │     WHERE event_id='uuid-123' AND consumer='app-analytics'
        │  └─ Not found (first time)
        │
        ├─ Process message:
        │  ├─ INSERT INTO processed_events (event_id, consumer, processed_at)
        │  ├─ UPDATE analytics_daily:
        │  │  ├─ date: 2026-08-29
        │  │  ├─ event_type: "appointment.created"
        │  │  ├─ count: +1
        │  │  └─ (Updates daily counters for dashboard)
        │  └─ COMMIT
        │
        └─ Commit offset: 12345 → ready for next message

========== SCHEDULED TASKS (Running in Background) ==========

T=15m (900s later)

        CELERY BEAT SCHEDULER triggers:
        └─ Task: enqueue_due_appointment_reminders
        
        TASK EXECUTION (app/workers/tasks/appointment_tasks.py):
        ├─ Query DB: SELECT appointments 
        │            WHERE slot.start_datetime BETWEEN NOW AND NOW+24h
        │            AND reminder_status NOT SENT
        │
        ├─ Found: appointment 456 (scheduled 2026-09-15 10:00, today 2026-08-29)
        │         → Due in 17 days (within 24h window)
        │
        ├─ Enqueue: send_appointment_reminder.delay(456)
        │
        └─ Task message added to Redis queue:
           ├─ Broker: redis://redis:6379/0
           ├─ Queue: celery
           ├─ Task: {
           │   "task": "send_appointment_reminder",
           │   "args": [456],
           │   "headers": {"X-Correlation-ID": "auto-generated"}
           │ }
           └─ before_task_publish signal fires ✅

========== CELERY WORKER PICKS UP REMINDER TASK ==========

T=15m+5s

        CELERY WORKER (python -m celery -A app.celery_app worker)
        ├─ Polls Redis queue
        ├─ Sees: send_appointment_reminder(456)
        ├─ task_prerun signal fires:
        │  └─ Extract correlation_id from headers
        │  └─ Set in logging context
        │
        ├─ Execute: send_appointment_reminder(456)
        │  ├─ Query appointment 456 from DB
        │  ├─ Get notification service
        │  ├─ Generate reminder message (SMS/email)
        │  ├─ Send notification ✅
        │  ├─ Check idempotency: reminder:456:2026-09-15
        │  │  ├─ Claim atomically
        │  │  └─ Cached in Redis (TTL: 2 days)
        │  └─ Return: {status: "sent"}
        │
        ├─ task_postrun signal fires:
        │  └─ Log: "Task completed"
        │
        └─ Result stored in Redis backend (if enabled)
           └─ Key: celery-task-meta-{task_id}

========== SYSTEM STATE AT END ==========

Database (PostgreSQL):
├─ appointments: {id: 456, patient_id: 42, status: "CONFIRMED", ...}
├─ slots: {id: 55, status: "RESERVED", ...}
├─ billing: {id: 123, appointment_id: 456, amount: 150.00, status: "APPROVED"}
├─ notifications: {id: 789, appointment_id: 456, type: "REMINDER", status: "SENT"}
├─ processed_events: {event_id: "uuid-123", consumer: "app-analytics", processed_at: T+8.5s}
├─ analytics_daily: {date: 2026-08-29, event_type: "appointment.created", count: 1}
└─ status_history: Complete audit trail of all state changes

Temporal (Workflow History):
├─ AppointmentSagaWorkflow
│  └─ workflow_id: appointment_saga_42_55_xyz123
│     ├─ WorkflowStarted (T=1s)
│     ├─ Activity 1: validate_appointment_data ✅
│     ├─ Activity 2: reserve_slot ✅
│     ├─ Activity 3: create_pending_appointment ✅
│     ├─ Activity 4: run_billing_precheck ✅
│     ├─ Activity 5: send_reminder ✅
│     ├─ Activity 6: confirm_appointment ✅
│     ├─ Activity 7: publish_appointment_created_event ✅
│     └─ WorkflowCompleted (T=9s)

Redis:
├─ appointments:idempotency:42:booking-abc-123 = {appointment_id: 456}
├─ reminders:456:2026-09-15 = "sent" (TTL: 2 days)
└─ (no rate limiting triggered in this request)

Kafka:
├─ Topic: app.appointment.created
│  └─ Offset 12345 (partition 0): Event payload + headers

Logs (With Correlation):
├─ [corr-xyz-789] POST /appointments
├─ [corr-xyz-789] Running appointment saga
├─ [corr-xyz-789] Workflow started: appointment_saga_42_55_xyz123
├─ [corr-xyz-789] Activity: validate_appointment_data
├─ [corr-xyz-789] Activity: reserve_slot
├─ ... (all activities include correlation_id)
├─ [corr-xyz-789] Event published to Kafka: app.appointment.created
├─ [corr-xyz-789] Response: 200 OK
└─ (Later, async)
   ├─ [corr-xyz-789] Kafka message consumed
   ├─ [corr-xyz-789] Analytics updated
   └─ [auto-gen-id] Reminder task executed
```

---

## Flow 2: Service Publication (Temporal Workflow + Kafka)

```
T=0s    CLINIC ADMIN ACTION
        Upload new service: "Professional Teeth Whitening"
        POST /api/v1/services
        Body: {
          name: "Professional Teeth Whitening",
          department_id: 5,
          description: "90-minute professional bleaching session...",
          preparation_instructions: "No food 2 hours before...",
          specialty: "Cosmetic",
          price: 299.99
        }

T=0.5s  SERVICE MANAGEMENT SERVICE (app/services/service_management.py)
        ├─ Validate input
        ├─ INSERT service into DB (status: 'DRAFT')
        ├─ Publish event: "service.created"
        └─ Call: await service_publish_workflow(service_id=10)

T=1s    TEMPORAL WORKFLOW SUBMITTED
        └─ ServicePublishWorkflow.execute(service_id=10)
           └─ Workflow ID: service_publish_10

T=2s    TEMPORAL WORKER PICKS UP
        └─ Starts: ServicePublishWorkflow

T=3s    ACTIVITY 1: validate_service
        ├─ Check: description ✅
        ├─ Check: preparation_instructions ✅
        ├─ Check: department ✅
        ├─ Mark status: "PUBLISHING"
        └─ Return service data

T=4s    ACTIVITY 2: structure_service
        ├─ Normalize fields
        ├─ Format metadata
        └─ Return structured data

T=5s    ACTIVITY 3: chunk_service
        ├─ Split description into 120-char chunks
        ├─ Example:
        │  ├─ Chunk 0: "90-minute professional bleaching session..."
        │  ├─ Chunk 1: "Results visible immediately. Lasts 6-12..."
        │  ├─ Chunk 2: "...no food 2 hours before appointment"
        │  └─ (3 chunks total)
        └─ Return: [{chunk_index, content, content_hash}]

T=6s    ACTIVITY 4: embed_chunks
        ├─ Call ML model (pgvector, embeddings)
        ├─ Generate vector for each chunk
        │  └─ Example: [0.123, -0.456, 0.789, ...] (768 dimensions)
        └─ Return: {embedded_chunks: [{chunk, embedding}]}

T=8s    ACTIVITY 5: store_embedded_chunks
        ├─ INSERT INTO content_chunks (service_id, chunk_index, embedding)
        ├─ INSERT INTO service_embeddings (service_id, embedding)
        └─ COMMIT

T=9s    ACTIVITY 6: mark_service_published
        ├─ UPDATE services SET status='PUBLISHED'
        ├─ COMMIT
        └─ Workflow complete: {status: "published"}

T=9.5s  KAFKA EVENT PUBLISHED
        ├─ Event: "service.published"
        └─ Topic: app.service.published
           └─ Offset: 12346

T=10s   KAFKA CONSUMER RECEIVES
        ├─ Topic: app.service.published
        ├─ Message: {event_id, service_id: 10, status: "PUBLISHED"}
        ├─ Process: UPDATE analytics_daily (services_published_count++)
        └─ Commit offset

T=11s   ADMIN SEES
        ├─ Service visible in public catalog
        ├─ Embeddings indexed for search
        └─ Available for patients to book
```

---

## Flow 3: Multi-Tenant Correlation (E2E Traceability)

```
User's Journey:
1. Books appointment (API request with X-Correlation-ID: abc-123)
2. Temporal saga runs (all activities log with abc-123)
3. Event published to Kafka (event includes correlation_id: abc-123)
4. Analytics consumer processes (logs use abc-123)
5. 15 minutes later, Celery task sends reminder (auto-generated ID)

COMPLETE TRACE IN LOGS:
grep "abc-123" all-logs.txt
├─ T=0s: [HTTP] POST /appointments [abc-123]
├─ T=0.5s: [SERVICE] Running saga [abc-123]
├─ T=1s: [TEMPORAL] WorkflowStarted [abc-123]
├─ T=2s: [TEMPORAL] validate_appointment_data [abc-123]
├─ T=3s: [TEMPORAL] reserve_slot [abc-123]
├─ T=4s: [TEMPORAL] create_pending_appointment [abc-123]
├─ T=5s: [TEMPORAL] run_billing_precheck [abc-123]
├─ T=6s: [TEMPORAL] send_reminder [abc-123]
├─ T=7s: [TEMPORAL] confirm_appointment [abc-123]
├─ T=8s: [TEMPORAL] publish_appointment_created_event [abc-123]
├─ T=8.5s: [KAFKA] Event published: app.appointment.created [abc-123]
├─ T=8.7s: [KAFKA] Consumer received message [abc-123]
├─ T=8.8s: [ANALYTICS] Updated daily_counters [abc-123]
└─ (Later) [CELERY] Reminder sent (different task_id, but tracks back to abc-123)

RESULT: Operator can follow ENTIRE appointment journey with single grep!
```

---

## Component Interaction Matrix

```
┌─────────────────┬──────────┬─────────┬──────────┬────────┬─────┐
│     Component   │ Temporal │ Celery  │  Redis   │ Kafka  │ PG  │
├─────────────────┼──────────┼─────────┼──────────┼────────┼─────┤
│ AppointmentAPI  │    ✅    │   ✅    │    ✅    │   ❌   │  ✅ │
├─────────────────┼──────────┼─────────┼──────────┼────────┼─────┤
│ Temporal        │    ✅    │   ❌    │    ❌    │   ✅   │  ✅ │
│ (Saga)          │ (exec)   │         │          │ (event)│ (db)│
├─────────────────┼──────────┼─────────┼──────────┼────────┼─────┤
│ Celery Worker   │    ❌    │   ✅    │    ✅    │   ✅   │  ✅ │
│ (Reminders)     │          │ (queue) │ (idempo) │ (event)│ (db)│
├─────────────────┼──────────┼─────────┼──────────┼────────┼─────┤
│ Kafka Consumer  │    ❌    │   ❌    │    ❌    │   ✅   │  ✅ │
│ (Analytics)     │          │         │          │ (msg)  │ (db)│
├─────────────────┼──────────┼─────────┼──────────┼────────┼─────┤
│ Celery Beat     │    ❌    │   ✅    │    ✅    │   ❌   │  ✅ │
│ (Scheduler)     │          │ (task)  │ (lock)   │        │ (q) │
└─────────────────┴──────────┴─────────┴──────────┴────────┴─────┘
```

---

## Data Flow Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                     API / HTTP Request                           │
│           (User books appointment, publishes service)            │
└──────────┬───────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────┐
│              Service Layer (Business Logic)                      │
│   ├─ AppointmentService.create()                                 │
│   └─ ServiceManagement.publish()                                 │
└──────────┬──────────────────────────────────────────────────────┘
           │
           │ ┌──────────────────────────────────────────┐
           │ │ TEMPORAL SAGA / WORKFLOW EXECUTION      │
           │ │ Orchestrates: validation, reservation,  │
           │ │ creation, billing, confirmation         │
           │ │                                          │
           │ └──────────┬───────────────────────────────┘
           │            │
           │            ▼
           │ ┌──────────────────────────────────────────┐
           │ │ PostgreSQL (Transactional)              │
           │ │ ├─ Appointments (CONFIRMED)             │
           │ │ ├─ Slots (RESERVED)                     │
           │ │ ├─ Billing (APPROVED)                   │
           │ │ └─ Notifications (SENT)                 │
           │ └──────────────────────────────────────────┘
           │
           ├──────────────────────────────────────────┐
           │                                          │
           ▼                                          ▼
┌──────────────────────────┐         ┌──────────────────────────┐
│  KAFKA EVENT PUBLISH     │         │   CACHE IN REDIS         │
│  ├─ Topic: app.*         │         │  ├─ Idempotency cache    │
│  ├─ Partition: 0         │         │  ├─ Reminder idempotency │
│  ├─ Offset: 12345        │         │  └─ Rate limit counters  │
│  └─ Message: Event       │         └──────────────────────────┘
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────┐
│           KAFKA CONSUMER (Analytics)                             │
│  ├─ Receives: app.appointment.created                            │
│  ├─ Validates: No PHI, has event_id                              │
│  ├─ Deduplicates: Check processed_events table                   │
│  └─ Processes: Update analytics_daily                            │
└──────────┬───────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────┐
│                  CELERY SCHEDULER (Beat)                         │
│  Every 15 minutes:                                               │
│  ├─ enqueue_due_appointment_reminders()                          │
│  │  └─ Find appointments due for reminders                       │
│  │  └─ Queue: send_appointment_reminder.delay()                  │
│  │                                                               │
│  Every 30 seconds:                                               │
│  ├─ publish_pending_events()                                     │
│  │  └─ Find events in outbox (if Kafka was down)                 │
│  │  └─ Try publish to Kafka                                      │
│  └─ Update status: PENDING → PUBLISHED                           │
└──────────┬───────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────┐
│                  CELERY WORKER (Task Executor)                   │
│  Picks up tasks from Redis queue:                                │
│  ├─ send_appointment_reminder(appointment_id)                    │
│  │  └─ Query DB for appointment details                          │
│  │  └─ Send SMS/email notification                               │
│  │  └─ Mark in idempotency store (prevent duplicates)            │
│  └─ Correlate with original request (X-Correlation-ID)           │
└──────────────────────────────────────────────────────────────────┘
```

---

## Failure Recovery Scenarios

### Scenario 1: Temporal Worker Crashes During Saga

```
T=5s    Temporal worker executing "run_billing_precheck"
        ├─ Worker process crashes ❌
        └─ Activity in-progress, not completed

T=10s   Worker process restarted
        └─ Temporal sees: Activity not completed
        └─ History: Activities 1-3 done, Activity 4 in-progress
        └─ New worker picks up: Resume from Activity 4 ✅
           ├─ Replay Activities 1-3 (deterministic, no side effects)
           ├─ Execute Activity 4 again (billing precheck)
           │  └─ DB query: Is billing already done? ✅ Yes
           │  └─ Return same result (idempotent)
           └─ Continue with Activities 5-7

Result: Workflow completes successfully despite crash
```

### Scenario 2: Kafka Broker Down When Publishing Event

```
T=8s    Activity: publish_appointment_created_event
        ├─ Call: KafkaEventPublisher.publish_event()
        ├─ Try to connect Kafka
        ├─ ❌ Broker unavailable
        ├─ Exception raised: KafkaProducerError
        └─ Fallback: HealthcareEventService._save_outbox()
           ├─ INSERT INTO outbox_events (status='PENDING')
           └─ Activity completes ✅

T=10s   API response sent to user ✅ (appointment confirmed despite Kafka down)

T=30s   CELERY BEAT: publish_pending_events task runs
        ├─ Query: SELECT * FROM outbox_events WHERE status='PENDING'
        ├─ Found: Our event
        ├─ Try Kafka again
        ├─ ✅ Broker now up!
        ├─ Publish event successfully
        └─ Update: status='PUBLISHED'

Result: Event eventually published when broker recovers
```

### Scenario 3: Duplicate Reminder Sends

```
T=15m   Celery Beat enqueues: send_appointment_reminder.delay(456)
        ├─ Task message goes to Redis queue

T=15m+2s Celery Worker 1 picks up task
        ├─ Execute: send_appointment_reminder(456)
        ├─ Idempotency key: reminder:456:2026-09-15
        ├─ Claim: idempotency_store.claim(...) ✅ Success
        ├─ Send SMS ✅
        ├─ Set in Redis: reminder:456:2026-09-15 (TTL: 2 days)
        └─ Task completes

T=15m+3s Worker crashes ❌
        └─ Offset not committed (before crash)

T=15m+5s Worker 2 joins
        ├─ Redis reconnects
        ├─ Task in queue again (offset reset)
        ├─ Execute: send_appointment_reminder(456)
        ├─ Idempotency key: reminder:456:2026-09-15
        ├─ Try claim: idempotency_store.claim(...) ❌ Already exists!
        ├─ Skip sending
        └─ Return: {status: "already_sent"}

Result: SMS sent only once despite worker crash and retry!
```

---

## Summary: Component Responsibilities

| Component | Responsibility | Failure Mode | Recovery |
|-----------|---|---|---|
| **API** | Accept requests, delegate to services | 5xx error | Retry from client |
| **Service** | Business logic orchestration | Exception | Saga rollback (Temporal) |
| **Temporal** | Durable workflow execution | Worker crash | Resume from checkpoint |
| **PostgreSQL** | Persist state | Connection error | Retry with backoff |
| **Kafka** | Event streaming | Broker down | Outbox pattern (Celery) |
| **Celery** | Async task execution | Worker crash | Retry from queue |
| **Redis** | Caching & queuing | Connection error | Fallback to in-memory |
| **Prometheus** | Metrics collection | - | Non-blocking (best effort) |

---

## Deployment Topology

```
┌─────────────────────────────────────────────────────────┐
│              DOCKER COMPOSE (Development)              │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │ API Service (FastAPI)                            │  │
│  │ Port: 8000                                       │  │
│  │ Workers: 1 (configurable)                        │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │ PostgreSQL Database                              │  │
│  │ Port: 5432                                       │  │
│  │ Data: /var/lib/postgresql/data                   │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Redis Cache & Queue                              │  │
│  │ Port: 6379                                       │  │
│  │ Databases: 0=broker, 1=results, 2=idempotent... │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Kafka + Zookeeper                                │  │
│  │ Port: 9092 (external), 29092 (internal)          │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Temporal Server                                  │  │
│  │ Port: 7233 (gRPC)                                │  │
│  │ UI: 8080                                         │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Celery Worker                                    │  │
│  │ Command: celery -A app.celery_app worker         │  │
│  │ Processes: 1-4 (configurable)                    │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Celery Beat (Scheduler)                          │  │
│  │ Command: celery -A app.celery_app beat           │  │
│  │ Processes: 1 (must be unique)                    │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Analytics Consumer (Kafka)                       │  │
│  │ Command: python -m app.workers.analytics_consumer│  │
│  │ Processes: 1-N (one per partition)               │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

