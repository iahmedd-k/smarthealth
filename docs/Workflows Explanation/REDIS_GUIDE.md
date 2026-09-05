# Redis Guide - SmartHealth

## Overview

SmartHealth uses **Redis** for:
1. **Celery Broker:** Message queue for background tasks
2. **Celery Result Backend:** Store task results
3. **Idempotency Store:** Track request deduplication
4. **Rate Limiting Storage:** API rate limit counters

---

## Redis Architecture

```
┌──────────────────────────────────────────────────┐
│              Redis Instance                      │
│           (Single instance in dev)               │
│                                                  │
│  Database 0: Celery Broker Queue                 │
│  Database 1: Task Results                        │
│  Database 2: Idempotency Store                   │
│  Database 3: Rate Limiting                       │
└──────────────────────────────────────────────────┘
     ↑           ↑            ↑            ↑
     │           │            │            │
  Celery      Celery        Idempotency   Slowapi
  Worker      Result         Store        Rate Limiter
```

---

## 1. Celery Broker (Message Queue)

### Purpose
Store task messages that Celery workers pick up and execute.

### Configuration
**File:** [app/core/settings.py](../../app/core/settings.py)

```python
celery_broker_url: str = Field(
    default="redis://localhost:6379/0",
    alias="CELERY_BROKER_URL"
)
```

### Flow

```
┌──────────────────────┐
│  AppointmentService  │
└──────────┬───────────┘
           │ send_reminder.delay(123)
           ▼
┌──────────────────────────────┐
│   before_task_publish        │
│   (Add correlation headers)  │
└──────────┬───────────────────┘
           │
           ▼
┌──────────────────────────────────────────────┐
│     Redis Database 0 (Celery Broker)         │
│                                              │
│  List: celery (task queue)                   │
│  └─ Push: {                                  │
│       "task": "send_reminder",               │
│       "args": [123],                         │
│       "headers": {                           │
│         "X-Correlation-ID": "abc-123"        │
│       }                                      │
│     }                                        │
└──────────┬───────────────────────────────────┘
           │
           │ Worker polls (BLPOP celery)
           ▼
┌──────────────────────────┐
│   Celery Worker          │
│   Picks up task          │
│   Executes send_reminder │
└──────────────────────────┘
```

**How it Works:**
```bash
# Internally, Celery uses Redis data structures:

# Store task message
LPUSH celery "{serialized_task}"

# Worker polls for tasks
BRPOP celery 1  # Block for 1 second

# Task acknowledged (removed from queue)
# (Auto-acked when worker starts)
```

---

## 2. Celery Result Backend

### Purpose
Store task execution results (optional - for tracking completion).

### Configuration
```python
celery_result_backend: str = Field(
    default="redis://localhost:6379/1",
    alias="CELERY_RESULT_BACKEND"
)
```

### Data Stored
```
Key: celery-task-meta-{task_id}
Value: {
  "status": "SUCCESS",
  "result": {
    "appointment_id": 123,
    "status": "CONFIRMED"
  },
  "traceback": null
}

TTL: Expires after result_expires (default: 1 day)
```

### Example
```bash
# After task completes
redis> GET celery-task-meta-abc-123-def-456
{
  "status": "SUCCESS",
  "result": {"status": "sent"},
  "traceback": null
}
```

---

## 3. Idempotency Store

### Purpose
Prevent duplicate operations (API idempotency).

**File:** [app/core/idempotency.py](../../app/core/idempotency.py)

```python
class RedisIdempotencyStore:
    def get(self, user_id: int, idempotency_key: str) -> dict | None:
        """Retrieve cached result for this idempotency key"""
        
    def set(self, user_id: int, idempotency_key: str, value: dict, ttl_seconds: int = 86400):
        """Cache result for this idempotency key (24h default)"""
        
    def claim(self, user_id: int, idempotency_key: str, ttl_seconds: int = 300) -> bool:
        """Atomically claim a key (prevent concurrent execution)"""
```

### Use Case: Appointment Booking

**File:** [app/services/appointment_service.py](../../app/services/appointment_service.py)

```python
async def create(self, payload, current_user, idempotency_key=None):
    """
    Create appointment with idempotency.
    
    If same patient submits same request twice:
    1. First request → Executes saga, caches result
    2. Second request → Returns cached result (no re-execution)
    """
    
    if idempotency_key:
        # Check if we've seen this request before
        cached = idempotency_store.get(current_user.id, idempotency_key)
        if cached:
            logger.info("Idempotent appointment retrieval", 
                       extra={"appointment_id": cached["appointment_id"]})
            appointment = self.appointments.get_by_id(cached["appointment_id"])
            if appointment:
                return appointment  # Return cached result
    
    # First time seeing this request - execute saga
    workflow_result = await run_appointment_saga(workflow_payload)
    
    # Cache result for future identical requests
    if idempotency_key:
        idempotency_store.set(
            current_user.id,
            idempotency_key,
            {"appointment_id": appointment.id},
            ttl_seconds=86400  # 24 hours
        )
    
    return appointment
```

### Redis Storage
```
Key: appointments:idempotency:{user_id}:{idempotency_key}
Value: {
  "appointment_id": 123
}

TTL: 86400 seconds (24 hours)

Example:
Key: appointments:idempotency:42:my-unique-key-abc
Value: {"appointment_id": 456}
```

### Race Condition Prevention

```
Problem: Two concurrent requests with same idempotency_key
Solution: Use Redis SETNX (SET if Not eXists)

Request 1: idempotency_store.claim(user=42, key="book")
           → SET key "IN_PROGRESS" NX EX 300
           → ✅ Success (acquired lock)
           → Execute saga (10 seconds)
           → SET key "DONE" (release lock)

Request 2: (concurrent, 5 seconds into Request 1's saga)
           idempotency_store.claim(user=42, key="book")
           → SET key "IN_PROGRESS" NX EX 300
           → ❌ Fails (key already exists)
           → Return False (don't start saga again)
```

### Fallback to In-Memory

If Redis is unavailable:
```python
class RedisIdempotencyStore:
    def __init__(self):
        self._redis = None
        self._fallback_store: dict[tuple[int, str], dict] = {}
        self._init_client()
    
    def get(self, user_id, idempotency_key):
        if self._redis is not None:
            # Use Redis
            return redis_get(...)
        else:
            # Fallback to in-memory dict
            return self._fallback_store.get((user_id, idempotency_key))
```

---

## 4. Rate Limiting Storage

### Purpose
Track API request counts per IP address to enforce rate limits.

**File:** [app/core/rate_limit.py](../../app/core/rate_limit.py)

```python
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(
    key_func=get_remote_address,        # Rate limit by IP
    storage_uri=settings.redis_url,     # Use Redis for counts
)
```

### Usage in Routes

```python
from app.core.rate_limit import limiter

@router.post("/appointments")
@limiter.limit("10/minute")  # Max 10 requests per minute
async def create_appointment(payload, current_user):
    return await appointment_service.create(payload, current_user)
```

### How It Works

```
Request 1 (IP: 192.168.1.5) at 12:00:01
│
└─ Check: rate_limit:192.168.1.5 count
   └─ Not in Redis yet
   └─ Create: rate_limit:192.168.1.5 = 1
   └─ Set TTL: 60 seconds
   └─ ✅ Request allowed (1/10)

Request 2 (IP: 192.168.1.5) at 12:00:05
│
└─ Check: rate_limit:192.168.1.5 count
   └─ Value: 1
   └─ Increment: rate_limit:192.168.1.5 = 2
   └─ ✅ Request allowed (2/10)

...

Request 11 (IP: 192.168.1.5) at 12:00:55
│
└─ Check: rate_limit:192.168.1.5 count
   └─ Value: 10
   └─ Would increment to 11
   └─ ❌ Request rejected (10/10 limit exceeded)
   └─ Response: 429 Too Many Requests

At 12:01:01 (TTL expires):
│
└─ Key: rate_limit:192.168.1.5 deleted
└─ New requests start fresh (1/10)
```

### Redis Storage
```
Key: rate_limit:{ip_address}
Value: {count}
TTL: 60 seconds (window size)

Example:
Key: rate_limit:192.168.1.5
Value: 8
TTL: 45 seconds (remaining)
```

---

## Appointment Reminder Idempotency (Complex Case)

**File:** [app/workers/tasks/appointment_tasks.py](../../app/workers/tasks/appointment_tasks.py)

```python
def send_appointment_reminder(self, appointment_id: int):
    """Send reminder - ensure sent only once per appointment per day"""
    
    appointment = AppointmentRepository(db).get_by_id(appointment_id)
    
    # Idempotency key: reminder:{appt_id}:{date}
    # Prevents sending duplicate reminders for same appointment on same day
    delivery_key = f"reminder:{appointment_id}:{appointment.slot.start_datetime.date().isoformat()}"
    
    # Claim key atomically
    if not idempotency_store.claim(
        appointment.patient_id,
        delivery_key,
        ttl_seconds=172800  # 2 days
    ):
        # Already sent, skip
        return {"appointment_id": appointment_id, "status": "already_sent"}
    
    # First time sending this reminder
    service = NotificationService(db)
    result = service.send_appointment_reminder(appointment_id)
    
    return result
```

### Example Scenario

```
User's appointment: 2026-08-31 10:00 (appointment_id: 123)

Celery Beat (every 15 min):
│
└─ Check reminders due in [now, now+24h]
   └─ Found: appointment 123
   └─ Enqueue: send_appointment_reminder.delay(123)

Worker 1:
│
└─ send_appointment_reminder(123)
   ├─ Claim: reminder:123:2026-08-31 ✅ Success
   ├─ Send SMS ✅
   └─ Set in Redis: reminder:123:2026-08-31 (TTL: 2 days)

(Same appointment enqueued again by Celery Beat)

Worker 2 (or same worker, 1 hour later):
│
└─ send_appointment_reminder(123)
   ├─ Try claim: reminder:123:2026-08-31 ❌ Already exists
   ├─ Skip sending
   └─ Return: {status: "already_sent"}

Result: SMS sent only once, even if task retried multiple times!
```

---

## Docker Compose Setup

**File:** [docker-compose.yml](../../docker-compose.yml)

```yaml
services:
  redis:
    image: redis:7-alpine
    container_name: smarthealth-redis
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes  # Enable AOF persistence
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

volumes:
  redis-data:
```

---

## Configuration (Environment Variables)

| Variable | Default | Purpose |
|----------|---------|---------|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection (also used for rate limiting) |
| `CELERY_BROKER_URL` | `redis://localhost:6379/0` | Celery message broker |
| `CELERY_RESULT_BACKEND` | `redis://localhost:6379/1` | Task result storage |

---

## Monitoring & Debugging

### Check Redis Connection
```bash
# From app
curl http://localhost:8000/api/v1/health
# Look for "redis": "ok"

# Direct connection
redis-cli ping
# Output: PONG
```

### View Queued Tasks
```bash
redis-cli
> LLEN celery  # Queue length
> LRANGE celery 0 5  # Show first 5 tasks
> LPOP celery  # Remove first task (worker does this)
```

### View Task Results
```bash
redis-cli
> KEYS celery-task-meta-*  # Find all task results
> GET celery-task-meta-{task_id}  # View specific result
```

### View Idempotency Cache
```bash
redis-cli
> KEYS appointments:idempotency:*  # Find all cached requests
> GET appointments:idempotency:42:my-key
# Output: {"appointment_id": 123}

> TTL appointments:idempotency:42:my-key  # Check time-to-live
# Output: 86341 (seconds remaining)
```

### View Rate Limit Counters
```bash
redis-cli
> KEYS rate_limit:*  # Find all rate limit keys
> GET rate_limit:192.168.1.5
# Output: 8 (requests in current window)

> TTL rate_limit:192.168.1.5  # Check window expiration
# Output: 42 (seconds remaining in window)
```

### Redis Memory Usage
```bash
redis-cli
> INFO memory
# Output: used_memory_human:10.5M, maxmemory_policy:noeviction
```

---

## Data Structures Used

### List (Celery Queue)
```
Key: celery
Type: List
Operations: LPUSH (enqueue), BRPOP (worker pickup)
```

### String (Task Results, Idempotency, Rate Limiting)
```
Key: celery-task-meta-{id}
Key: appointments:idempotency:{user}:{key}
Key: rate_limit:{ip}
Type: String
Operations: SET, GET, SETNX (atomic)
```

### Hash (Future Use)
```
Potential: User session data, request context
```

---

## Persistence

### AOF (Append-Only File)
```yaml
# docker-compose.yml
command: redis-server --appendonly yes
# All writes logged to disk
# Survives crashes (replayed on startup)
```

### RDB (Snapshot)
```bash
# Manual snapshot
redis-cli SAVE

# Automatic snapshots (default)
# Every 60s if 1000 keys changed
# Every 300s if 10 keys changed
```

---

## Failure Scenarios

### Scenario 1: Redis Down (Celery)

```
User books appointment:
│
└─ enqueue_reminder_task.delay(123)
   ├─ Try to connect Redis
   ├─ ❌ Connection failed
   ├─ Exception: "Cannot connect to broker"
   └─ Celery app config: broker_connection_retry_on_startup=True
      └─ App waits and retries connection
      └─ Task enqueuing deferred until Redis back
```

**Impact:**
- New tasks cannot be enqueued
- Existing queued tasks (if Redis had partial data) preserved if using AOF
- User sees 503 Service Unavailable

### Scenario 2: Redis Down (Idempotency)

```
First request with idempotency_key:
│
└─ idempotency_store.get(...) 
   ├─ Try Redis
   ├─ ❌ Connection failed
   └─ Use fallback: in-memory dict
      └─ Not cached (fresh dict)
      └─ Execute saga
      └─ Cache in memory (only in this process)

Second request with same idempotency_key (to different pod):
│
└─ idempotency_store.get(...) 
   ├─ Try Redis ❌
   └─ Use fallback: in-memory dict (different dict!)
      └─ Not cached (different process)
      └─ Execute saga AGAIN! ❌ Duplicate booking

Result: Idempotency broken when Redis down
(In production, use Redis + backup cache)
```

---

## Best Practices

1. **Monitor Queue Length:** Alerts if queue growing (workers slow)
2. **Set Result TTL:** Don't keep results forever (save memory)
3. **Use AOF Persistence:** Don't lose queued tasks
4. **Separate Databases:** Use different databases for different purposes
5. **Rate Limit Tuning:** Adjust limits based on usage patterns
6. **Idempotency Key Format:** Make them user-friendly and unique
7. **Backups:** Regular Redis snapshots for disaster recovery

---

## Summary

| Use Case | Data Type | TTL | Volume |
|----------|-----------|-----|--------|
| **Celery Broker** | List | None (until consumed) | Low-Medium |
| **Task Results** | String | 1 day (configurable) | Medium |
| **Idempotency** | String | 24h (configurable) | Medium |
| **Rate Limiting** | String | 60s (window) | High |

