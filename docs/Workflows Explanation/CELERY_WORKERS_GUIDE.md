# Celery Workers & Background Tasks Guide - SmartHealth

## Overview

SmartHealth uses **Apache Celery** with **Redis** as the message broker for asynchronous background tasks. Celery allows long-running operations to run in the background without blocking HTTP responses.

---

## What is Celery?

Celery is a distributed task queue that allows you to:
1. Queue work (task → broker → queue)
2. Execute asynchronously (worker picks up and runs)
3. Schedule recurring tasks (Celery Beat)
4. Track results (optional backend)
5. Retry on failure (automatic or manual)

```
┌──────────┐
│   API    │ "Send reminder for appointment 123"
└────┬─────┘
     │ .delay()
     ▼
┌────────────────────┐
│  Message Broker    │ (Redis)
│  Queue: celery     │ Stores: {task: "send_reminder", args: [123]}
└────┬───────────────┘
     │ Worker polls
     ▼
┌──────────────────────┐
│  Celery Worker       │ Picks up task, executes
│ send_reminder(123)   │
└──────────────────────┘
     │ Done
     ▼
┌──────────────────────┐
│  Result Backend      │ (Redis) - Optional
│ task_id → {result}   │
└──────────────────────┘
```

---

## Architecture Components

### 1. Message Broker (Redis)
**Location:** [docker-compose.yml](../../docker-compose.yml) - `redis` service
**Purpose:** Store task queue

```yaml
redis:
  image: redis:7-alpine
  ports:
    - "6379:6379"
```

### 2. Worker
**Location:** [app/workers/](../../app/workers/)
**Purpose:** Execute tasks

**Startup:**
```bash
celery -A app.celery_app worker --loglevel=info
```

### 3. Scheduler (Celery Beat)
**Location:** [app/celery_app.py](../../app/celery_app.py) - `beat_schedule`
**Purpose:** Schedule recurring tasks

**Startup:**
```bash
celery -A app.celery_app beat --loglevel=info
```

---

## Task Definition

### Example: Send Appointment Reminder

**File:** [app/workers/tasks/appointment_tasks.py](../../app/workers/tasks/appointment_tasks.py)

```python
@celery_app.task(
    bind=True,  # Receive self reference
    name="app.workers.tasks.appointment_tasks.send_appointment_reminder",
    autoretry_for=(ConnectionError, TimeoutError),  # Auto-retry on these
    retry_backoff=True,                              # Exponential backoff
    retry_jitter=True,                               # Add randomness (avoid thundering herd)
    max_retries=3,                                   # Maximum retry attempts
)
def send_appointment_reminder(self, appointment_id: int) -> dict[str, object]:
    """
    Send appointment reminder notification.
    
    Args:
        self: Task instance (bind=True)
        appointment_id: ID of appointment
    
    Returns:
        {status: "sent", appointment_id: 123}
    """
    
    # Log task metadata
    logger.info(
        "Starting appointment reminder task",
        extra={
            "task_id": self.request.id,
            "task_name": self.name,
            "appointment_id": appointment_id,
        }
    )
    
    db = SessionLocal()
    try:
        # Get appointment from database
        appointment = AppointmentRepository(db).get_one_or_none_by_id(appointment_id)
        if not appointment:
            raise AppError("Appointment not found")
        
        # Idempotency: Check if reminder already sent for this date
        delivery_key = f"reminder:{appointment_id}:{appointment.slot.start_datetime.date().isoformat()}"
        if not idempotency_store.claim(appointment.patient_id, delivery_key):
            return {"appointment_id": appointment_id, "status": "already_sent"}
        
        # Send notification (SMS, email, etc.)
        service = NotificationService(db)
        result = service.send_appointment_reminder(appointment_id)
        
        logger.info("Appointment reminder sent", extra={"appointment_id": appointment_id})
        return result
        
    except AppError as exc:
        # Record failure for monitoring
        FailedJobService(db).record_failure(...)
        logger.error("Reminder task failed", exc_info=True)
        raise
        
    except (ConnectionError, TimeoutError) as exc:
        # Will auto-retry
        if self.request.retries < self.max_retries:
            raise self.retry(exc=exc, countdown=30)  # Retry in 30s
        raise
        
    finally:
        db.close()
```

---

## Task Lifecycle

```
┌─────────────────────────────────────────────────────────────┐
│ 1. ENQUEUE                                                  │
│    send_appointment_reminder.delay(appointment_id=123)      │
│    → Task message sent to Redis queue                       │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. WORKER PICKUP                                            │
│    Worker polls queue, sees task, starts execution          │
│    Event: task_prerun signal fired                          │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. EXECUTION                                                │
│    Run task code (send_appointment_reminder)                │
│    ├─ Query database ✅                                     │
│    ├─ Check idempotency ✅                                  │
│    └─ Send notification ✅                                  │
└────────────────────┬────────────────────────────────────────┘
                     │
                  ✅ SUCCESS
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. COMPLETE                                                 │
│    Event: task_postrun signal fired                         │
│    Result stored in Redis backend (if configured)           │
│    Task marked as done                                      │
└─────────────────────────────────────────────────────────────┘

                  OR ❌ FAILURE
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 5a. RETRY (if configured)                                  │
│    Example: ConnectionError → auto-retry                   │
│    Wait exponential backoff (30s, 60s, 120s)               │
│    Attempt counter incremented                             │
│    Go back to step 2                                       │
└────────────────────┬────────────────────────────────────────┘
                     │
              (after max_retries)
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 5b. FAILURE RECORDED                                        │
│    Insert into failed_jobs table                            │
│    Correlation ID logged for tracing                        │
│    Alert/monitoring notified                                │
└─────────────────────────────────────────────────────────────┘
```

---

## Scheduled Tasks (Celery Beat)

**File:** [app/celery_app.py](../../app/celery_app.py) - `beat_schedule`

```python
celery_app.conf.update(
    beat_schedule={
        # Run every 15 minutes (900 seconds)
        "enqueue-due-appointment-reminders": {
            "task": "app.workers.tasks.appointment_tasks.enqueue_due_appointment_reminders",
            "schedule": 900.0,
        },
        # Run every 30 seconds
        "publish-pending-events": {
            "task": "app.workers.tasks.outbox_tasks.publish_pending_events",
            "schedule": 30.0,
        },
    },
)
```

### Task 1: Enqueue Due Reminders

**File:** [app/workers/tasks/appointment_tasks.py](../../app/workers/tasks/appointment_tasks.py)

```python
@celery_app.task(name="app.workers.tasks.appointment_tasks.enqueue_due_appointment_reminders")
def enqueue_due_appointment_reminders() -> dict[str, int]:
    """
    Find appointments with reminders due in next 24 hours.
    Enqueue reminder tasks for each.
    
    Runs: Every 15 minutes (900 seconds)
    """
    db = SessionLocal()
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        
        # Find appointments with reminders due in [now, now+24h]
        due = AppointmentRepository(db).iter_due_confirmed_reminders(
            now,
            now + datetime.timedelta(hours=24)
        )
        
        enqueued = 0
        for appointment in due:
            # Queue reminder task (not execute immediately)
            send_appointment_reminder.delay(appointment.id)
            enqueued += 1
        
        logger.info(f"Enqueued {enqueued} appointment reminders")
        return {"enqueued": enqueued}
    finally:
        db.close()
```

**Flow:**
```
Celery Beat Scheduler (every 15 min)
    │
    ▼
Run enqueue_due_appointment_reminders()
    │
    ├─ Query: Appointments with reminders due in [now, now+24h]
    │
    ├─ For each appointment:
    │  └─ send_appointment_reminder.delay(appointment_id)
    │     → Enqueue to queue
    │
    ▼
Worker picks up send_appointment_reminder tasks
    │
    └─ Execute send_appointment_reminder(appointment_id)
       → Send SMS/email notifications
```

### Task 2: Publish Pending Events (Outbox Pattern)

**File:** [app/workers/tasks/outbox_tasks.py](../../app/workers/tasks/outbox_tasks.py)

```python
@celery_app.task(
    bind=True,
    name="app.workers.tasks.outbox_tasks.publish_pending_events",
    autoretry_for=(ConnectionError, TimeoutError),
    retry_backoff=True,
    max_retries=3,
)
def publish_pending_events(self, limit: int = 100) -> dict[str, int]:
    """
    Publish events from outbox table to Kafka.
    
    Purpose: If Kafka is down when event published, store in DB.
    This task retries publishing periodically.
    
    Runs: Every 30 seconds
    """
    db = SessionLocal()
    published = 0
    failed = 0
    
    try:
        repository = OutboxRepository(db)
        
        # Get pending events (status=PENDING, attempts < limit)
        events = repository.list_pending(limit)
        
        publisher = KafkaEventPublisher()
        
        for event in events:
            try:
                # Try to publish to Kafka
                publisher.publish_event(
                    event_type=event.event_type,
                    entity_type=event.entity_type,
                    entity_id=event.entity_id,
                    **event.payload,
                )
                
                # Mark as published
                repository.mark_published(event, datetime.datetime.now(datetime.timezone.utc))
                published += 1
                
            except KafkaProducerError as exc:
                # Still can't publish, record failure
                repository.record_failed_event(event, ...)
                failed += 1
        
        repository.commit()
        return {"published": published, "failed": failed}
        
    finally:
        db.close()
```

**Flow:**
```
Kafka is DOWN (broker unavailable)
    │
    ▼
Event published via KafkaEventPublisher
    │
    ├─ Try Kafka: ❌ Connection failed
    │
    └─ Fallback: Insert into outbox_events table
       {event_id, event_type, payload, status: 'PENDING'}
    
Later, Celery Beat (every 30s)
    │
    ▼
publish_pending_events task runs
    │
    ├─ Query: SELECT * FROM outbox_events WHERE status = 'PENDING'
    │
    ├─ For each event:
    │  ├─ Try Kafka: ✅ Success
    │  └─ Update status: 'PUBLISHED'
    │
    ▼
After Kafka recovered: All pending events published!
```

---

## Signal Handlers (Correlation Context)

**File:** [app/celery_app.py](../../app/celery_app.py)

### Before Task Publish

```python
@signals.before_task_publish.connect
def before_task_publish(sender=None, body=None, **kwargs):
    """
    Attach correlation ID and request ID to task headers.
    Runs BEFORE task is sent to broker.
    """
    from app.core.logging import get_correlation_id, get_request_id
    
    correlation_id = get_correlation_id()  # From HTTP context
    request_id = get_request_id()
    
    if body is not None:
        if correlation_id:
            body["headers"] = body.get("headers", {})
            body["headers"]["X-Correlation-ID"] = correlation_id
        if request_id:
            body["headers"] = body.get("headers", {})
            body["headers"]["X-Request-ID"] = request_id
```

**Effect:**
```
HTTP Request (with X-Correlation-ID: abc123)
    │
    ▼
AppointmentService.create()
    │ Gets correlation_id from context
    ▼
enqueue_reminder_task()
    │
    ├─ before_task_publish signal
    │
    └─ Adds X-Correlation-ID: abc123 to task headers
    
Task stored in Redis:
{
  "task": "send_reminder",
  "args": [123],
  "headers": {"X-Correlation-ID": "abc123"}
}
```

### Before Task Execution

```python
@signals.task_prerun.connect
def task_prerun(sender=None, task_id=None, task=None, args=None, kwargs=None, **kw):
    """
    Extract correlation ID and request ID from task headers.
    Runs BEFORE task execution in worker.
    """
    from app.core.logging import get_correlation_id, get_request_id, set_correlation_id, set_request_id
    
    # Get headers passed by before_task_publish
    headers = kw.get("headers", {}) or {}
    
    correlation_id = headers.get("X-Correlation-ID") or generate_request_id()
    request_id = headers.get("X-Request-ID") or task_id or generate_request_id()
    
    # Set in context for logging
    set_correlation_id(correlation_id)
    set_request_id(request_id)
    
    logger.info(
        "Task started",
        extra={
            "task_name": task.name,
            "task_id": task_id,
            "correlation_id": correlation_id,
        }
    )
```

**Effect:**
```
Worker receives task with headers
    │
    ├─ task_prerun signal
    │
    ├─ Extract X-Correlation-ID: abc123
    │
    └─ Set in context for all logging in this task
    
Inside task:
  logger.info("Sending reminder", extra={...})
    → Automatically includes correlation_id from context
    
All logs from HTTP request → Celery task use same correlation_id!
```

---

## Configuration

**File:** [app/celery_app.py](../../app/celery_app.py)

```python
celery_app = Celery(
    "smarthealth",
    broker=settings.celery_broker_url,              # "redis://redis:6379/0"
    backend=settings.celery_result_backend,         # "redis://redis:6379/1"
)

celery_app.conf.update(
    task_serializer="json",                          # Serialize with JSON
    result_serializer="json",
    accept_content=["json"],
    imports=(                                         # Import task modules
        "app.workers.tasks.appointment_tasks",
        "app.workers.tasks.analytics_tasks",
        "app.workers.tasks.outbox_tasks",
    ),
    timezone="UTC",
    enable_utc=True,
    broker_connection_retry_on_startup=True,        # Retry broker connection
    task_always_eager=settings.celery_task_always_eager,  # Run sync in tests
    task_eager_propagates=True,
    beat_schedule={...},  # Scheduled tasks
)
```

---

## All Celery Tasks

### Task 1: Enqueue Reminders
```
Name: app.workers.tasks.appointment_tasks.enqueue_due_appointment_reminders
Trigger: Celery Beat (every 15 min)
Logic: Find appointments due for reminders, enqueue send_appointment_reminder
Return: {enqueued: int}
```

### Task 2: Send Reminder
```
Name: app.workers.tasks.appointment_tasks.send_appointment_reminder
Trigger: Manual .delay() or enqueue_due_appointment_reminders
Logic: Send SMS/email notification
Retries: 3x on ConnectionError/TimeoutError
Return: {status: "sent", appointment_id: int}
```

### Task 3: Publish Pending Events
```
Name: app.workers.tasks.outbox_tasks.publish_pending_events
Trigger: Celery Beat (every 30 sec)
Logic: Publish events from outbox table to Kafka
Retries: 3x on broker error
Return: {published: int, failed: int}
```

### Task 4: Analytics Tasks
```
File: app/workers/tasks/analytics_tasks.py
(Additional analytics aggregation tasks)
```

---

## Task Retry Strategy

### Automatic Retries

```python
@celery_app.task(
    autoretry_for=(ConnectionError, TimeoutError),  # Retry on these exceptions
    retry_backoff=True,                              # Exponential backoff
    retry_jitter=True,                               # Add randomness
    max_retries=3,                                   # Max 3 retries
)
def task_with_retries():
    # Attempt 1: ❌ ConnectionError
    #            → Wait 2s (1 * 2^1)
    # Attempt 2: ❌ TimeoutError
    #            → Wait 4s (2 * 2^2)
    # Attempt 3: ❌ Still failing
    #            → Wait 8s (3 * 2^3)
    # Attempt 4: ✅ Success!
    pass
```

### Manual Retries

```python
@celery_app.task(bind=True, max_retries=3)
def task_with_manual_retry(self):
    try:
        # Do work
        connect_to_external_service()
    except ConnectionError as exc:
        if self.request.retries < self.max_retries:
            # Retry in 30 seconds
            raise self.retry(exc=exc, countdown=30)
        else:
            # Max retries exceeded
            FailedJobService(...).record_failure(...)
            raise
```

---

## Monitoring & Debugging

### Check Worker Status
```bash
# Connect to Redis and see pending tasks
redis-cli
> LLEN celery  # Queue length
> LRANGE celery 0 10  # Show first 10 tasks
```

### View Task Results
```bash
# Query result backend
redis-cli
> GET celery-task-meta-{task_id}
# Shows: {status: "SUCCESS", result: {...}}
```

### Failed Job Tracking
**Table:** `failed_jobs`
```sql
SELECT * FROM failed_jobs
WHERE task_name LIKE '%send_reminder%'
ORDER BY created_at DESC;
```

### Task Metrics
**File:** [app/core/metrics.py](../../app/core/metrics.py)
```python
celery_task_executions_total = Counter(
    "celery_task_executions_total",
    "Total number of Celery task executions",
    ["task_name", "status"],
)
```

---

## Docker Compose Setup

```yaml
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data

  # Celery Worker
  celery-worker:
    build: .
    command: celery -A app.celery_app worker --loglevel=info
    depends_on:
      - redis
      - postgres
    environment:
      - CELERY_BROKER_URL=redis://redis:6379/0
      - CELERY_RESULT_BACKEND=redis://redis:6379/1

  # Celery Beat (Scheduler)
  celery-beat:
    build: .
    command: celery -A app.celery_app beat --loglevel=info
    depends_on:
      - redis
      - postgres
    environment:
      - CELERY_BROKER_URL=redis://redis:6379/0
```

---

## Task Flow Diagram

```
┌──────────────────────────────────────────────────┐
│                   API REQUEST                    │
│        POST /appointments (user books)           │
└──────────────────┬───────────────────────────────┘
                   │ Get correlation_id from header
                   ▼
┌──────────────────────────────────────────────────┐
│         AppointmentService.create()              │
│  1. Validate & create appointment (DB commit ✅)│
│  2. Publish event to Kafka                      │
│  3. Schedule reminder task                      │
└──────────────────┬───────────────────────────────┘
                   │ schedule_reminder.delay(appt_id)
                   │ + headers: {X-Correlation-ID}
                   ▼
┌──────────────────────────────────────────────────┐
│    before_task_publish signal                    │
│    Attach correlation_id to task headers         │
└──────────────────┬───────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────┐
│         Redis Message Broker                     │
│  Queue: {task: send_reminder, args: [123]}       │
└──────────────────┬───────────────────────────────┘
                   │ Worker polls
                   ▼
┌──────────────────────────────────────────────────┐
│    task_prerun signal                            │
│    Extract correlation_id from headers           │
│    Set in logging context                        │
└──────────────────┬───────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────┐
│  send_appointment_reminder(123)                  │
│  1. Query appointment from DB                    │
│  2. Check idempotency (Redis)                    │
│  3. Send notification (SMS/email)                │
│  → All logs include correlation_id              │
└──────────────────┬───────────────────────────────┘
                   │ ✅ Success
                   ▼
┌──────────────────────────────────────────────────┐
│    task_postrun signal                           │
│    Log completion with correlation_id            │
│    Store result in Redis (if backend enabled)    │
└──────────────────────────────────────────────────┘
```

---

## Summary

| Component | Purpose | Location |
|-----------|---------|----------|
| **Broker** | Message queue | Redis (docker) |
| **Worker** | Task executor | celery worker |
| **Beat** | Scheduler | celery beat |
| **Task** | Unit of work | app/workers/tasks/*.py |
| **Signal** | Event hook | app/celery_app.py |
| **Config** | Settings | app/celery_app.py |
| **Metrics** | Monitoring | app/core/metrics.py |

