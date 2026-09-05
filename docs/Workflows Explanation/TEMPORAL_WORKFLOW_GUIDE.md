# Temporal Workflow Implementation Guide - SmartHealth

## Overview

SmartHealth uses **Apache Temporal** for durable, reliable workflow orchestration. Temporal is a platform that ensures workflows execute reliably even if services crash or network fails - it's like a "workflow database" that can replay executions.

> **Layering note:** Current code lives under `app/workers/temporal/`.
> Workflows orchestrate deterministically, activities are thin adapters,
> services contain business rules, and repositories contain persistence code.

---

## What is Temporal?

Temporal enables **Durable Execution** - workflows remember their progress and can resume from where they stopped if anything fails.

```
Traditional Saga (without Temporal):
Request → Service A ✅ → Service B ✅ → Service C ❌ (crash)
                                         ↓
                                    Retry from start (lost context)
                                    ❌ May fail again


With Temporal:
Request → Workflow starts (recorded in database)
          ├─ Activity A ✅ (recorded)
          ├─ Activity B ✅ (recorded)
          ├─ Activity C ❌ (crash, recorded)
          ├─ Worker recovers
          └─ Resume from Activity C (saved state replayed)
             ├─ Activity C retry ✅
             └─ Workflow complete ✅
```

---

## Key Concepts

### 1. **Workflow**
A workflow is the orchestration logic - it decides what to do. Like a state machine.

**SmartHealth Workflows:**
- Location: [app/workers/temporal/workflows/appointment_saga.py](../../app/workers/temporal/workflows/appointment_saga.py)
- Location: [app/workers/temporal/workflows/service_publish.py](../../app/workers/temporal/workflows/service_publish.py)

**Characteristics:**
- **Deterministic:** Same input always produces same decisions (no random, no time calls)
- **Long-running:** Can take minutes, hours, days
- **Stateless:** Temporal stores all state (workflow can be replayed from history)
- **Idempotent:** Activities can be retried without side effects

### 2. **Activity**
An activity does the actual work - database queries, API calls, external services.

**SmartHealth Activities:**
- Location: [app/temporal/activities.py](../../app/temporal/activities.py) (all activities listed)

**Characteristics:**
- **Real work:** DB queries, API calls, external services
- **Retryable:** Can fail and retry automatically
- **Timeout:** Must have explicit timeout
- **Non-deterministic:** Can use random, time, external services

### 3. **Workflow ID**
A unique identifier for a workflow execution. Used to deduplicate concurrent requests.

**Example:**
```
workflow_id = f"appointment_saga_{patient_id}_{idempotency_key}"
```

If same workflow ID submitted twice:
- First execution starts
- Second request → Conflict! → Either use existing or reject

### 4. **History**
Temporal records every step the workflow takes. If worker crashes, it replays from history.

```
History entries:
1. WorkflowStarted
2. ActivityScheduled (validate_appointment_data)
3. ActivityCompleted (returned patient_id, slot_id)
4. ActivityScheduled (reserve_slot)
5. ActivityCompleted (slot reserved)
6. ActivityScheduled (confirm_appointment)
7. ActivityFailed (crash happens here)
   ↓
   [Worker restarts]
   ↓
   Replays 1-6, then retries 7
```

---

## Appointment Saga Workflow (Main Workflow)

### Purpose
**Orchestrates the entire appointment booking process atomically:**
1. Validate patient and slot exist
2. Reserve slot
3. Create appointment record
4. Pre-check billing
5. Send reminder notification
6. Confirm appointment
7. Publish event to Kafka

### Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│          API: POST /appointments                                 │
│          (Calls: run_appointment_saga(payload))                 │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│   app/workers/temporal/workflows/appointment_saga.py              │
│   class AppointmentSagaWorkflow:                                │
│     @workflow.run                                               │
│     async def execute(appointment_data: dict)                   │
│                                                                 │
│   Workflow ID: appointment_{patient_id}_{slot_id}_{timestamp}   │
│   (Deduplication: if same ID resubmitted, reuses result)        │
└──────────────────────┬──────────────────────────────────────────┘
                       │
        ┌──────────────┼──────────────┬──────────────┐
        │              │              │              │
        ▼              ▼              ▼              ▼
    Activity 1    Activity 2    Activity 3    Activity 4
    ─────────────────────────────────────────────────────
    validate_    reserve_      create_       run_billing_
    appointment_ slot          pending_      precheck
    data                        appointment

    Retry: 3x       Retry: 3x      Retry: 1x       Retry: 1x
    Timeout: 10s    Timeout: 10s   Timeout: 10s    Timeout: 30s
        │              │              │              │
        ▼              ▼              ▼              ▼
    ✅ OK         ✅ OK          ✅ OK          ✅ OK
        │              │              │              │
        └──────────────┼──────────────┴──────────────┘
                       │
                       ▼
        ┌──────────────┬──────────────┐
        │              │              │
        ▼              ▼              ▼
    Activity 5    Activity 6    Activity 7
    ─────────────────────────────────────────────
    send_        confirm_      publish_
    reminder     appointment   appointment_
                               created_
                               event

    Retry: 5x      Retry: 3x      Retry: 1x
    Timeout: 30s   Timeout: 10s   Timeout: 10s
        │              │              │
        ▼              ▼              ▼
    ✅ OK         ✅ OK          ✅ OK
        │              │              │
        └──────────────┼──────────────┘
                       │
                       ▼
        ┌──────────────────────────────────┐
        │  Workflow Complete               │
        │  Return: {appointment_id: 123}   │
        └──────────────────────────────────┘
```

### Code Structure

```python
@workflow.defn
class AppointmentSagaWorkflow:
    @workflow.run
    async def execute(self, appointment_data: dict[str, Any]) -> dict[str, object]:
        """
        Workflow execution entry point.
        
        Args:
            appointment_data: {
                patient_id, slot_id, provider_id, service_id,
                correlation_id, request_id, idempotency_key, ...
            }
        
        Returns:
            {appointment_id: int, status: str}
        """
        
        # Step 1: Validate
        validated = await workflow.execute_activity(
            validate_appointment_data,
            appointment_data,
            retry_policy=TRANSIENT_ACTIVITY_RETRY,  # Retry 3x (transient errors)
            start_to_close_timeout=timedelta(seconds=10),
        )
        
        # Step 2: Reserve slot
        reserved = await workflow.execute_activity(
            reserve_slot,
            {**appointment_data, **validated},
            retry_policy=BUSINESS_ACTIVITY_RETRY,  # No retry (permanent errors)
            start_to_close_timeout=timedelta(seconds=10),
        )
        
        # ...more activities...
        
        # Step N: Publish event
        await workflow.execute_activity(
            publish_appointment_created_event,
            final_data,
            retry_policy=TRANSIENT_ACTIVITY_RETRY,
            start_to_close_timeout=timedelta(seconds=10),
        )
        
        return {"appointment_id": appointment_id, "status": "CONFIRMED"}
```

---

## Retry Policies (Defined in temporal_policies.py)

| Policy | Use Case | Retries | Backoff | Max Interval |
|--------|----------|---------|---------|--------------|
| **TRANSIENT_ACTIVITY_RETRY** | Network/timeout errors | 3x | 2s → 4s → 8s | 30s |
| **BUSINESS_ACTIVITY_RETRY** | Permanent errors (AppError) | 1x | None | None |
| **COMPENSATION_RETRY** | Compensating actions (rollback) | 5x | 2s → 4s → 8s | 30s |
| **WORKFLOW_RETRY** | Workflow itself fails | 3x | 5s → 10s → 20s | 1m |
| **WORKER_INTERRUPTION_RETRY** | Worker restart demo | 3x | 1s → 2s → 4s | 5s |

### Example: TRANSIENT_ACTIVITY_RETRY

```python
TRANSIENT_ACTIVITY_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),      # First retry: wait 2s
    backoff_coefficient=2.0,                     # Double each time
    maximum_interval=timedelta(seconds=30),     # Max wait: 30s
    maximum_attempts=3,                          # Retry at most 3 times
    non_retryable_error_types=["AppError"],     # Don't retry business errors
)
```

**In Action:**
```
Attempt 1: Execute activity ❌ (network error)
           Wait 2s
Attempt 2: Execute activity ❌ (timeout)
           Wait 4s
Attempt 3: Execute activity ❌ (connection refused)
           Wait 8s
Attempt 4: Execute activity ✅ (recovery!)

If business error (AppError):
Attempt 1: Execute activity ❌ (AppError: "Slot booked")
           → DON'T retry, fail immediately
```

---

## Service Publish Workflow

### Purpose
**Publishes a service to the public catalog:**
1. Validate service completeness
2. Structure service metadata
3. Chunk description for embeddings
4. Generate vector embeddings
5. Store chunks in database
6. Mark service as published

### Activities

```python
@activity.defn
async def validate_service(service_id: int) -> dict[str, Any]:
    """Check service has required fields (description, department, etc.)"""
    # Returns: {status: "PUBLISHING", service: {...}}
    # OR: {status: "PUBLISHED", service: null} if already published

@activity.defn
async def structure_service(service_data: dict) -> dict[str, Any]:
    """Normalize/format service data"""
    # Returns: structured service with all metadata

@activity.defn
async def chunk_service(service_struct: dict) -> list[dict[str, Any]]:
    """Split description into 120-char chunks for embeddings"""
    # Returns: [{chunk_index: 0, content: "...", content_hash: "..."}]

@activity.defn
async def embed_chunks(chunks: list[dict]) -> dict[str, Any]:
    """Generate vector embeddings for each chunk using ML model"""
    # Returns: {embedded_chunks: [...], model_id: "..."}

@activity.defn
async def store_embedded_chunks(embedded: dict) -> dict[str, Any]:
    """Save chunks + embeddings to database"""
    # Returns: {stored: N}
```

### Workflow Definition

```python
@workflow.defn
class ServicePublishWorkflow:
    @workflow.run
    async def execute(self, service_id: int) -> dict[str, str]:
        """Validate → Structure → Chunk → Embed → Store → Publish"""
        
        validated = await workflow.execute_activity(
            validate_service,
            service_id,
            retry_policy=BUSINESS_ACTIVITY_RETRY,
            start_to_close_timeout=timedelta(seconds=30),
        )
        
        if validated["status"] == "PUBLISHED":
            return {"status": "already_published"}
        
        # ... continue with chunking, embedding, storage ...
        
        return {"status": "published", "service_id": service_id}
```

---

## Calling Workflows from API

### From AppointmentService

```python
# File: app/services/appointment_service.py

workflow_result = await run_appointment_saga(workflow_payload)
# Where workflow_payload = {
#   "patient_id": 23,
#   "slot_id": 55,
#   "correlation_id": "from-http-header",
#   "request_id": "unique-request-id",
#   "idempotency_key": "user-provided-key",
#   ...
# }

# run_appointment_saga() location: app/workers/temporal/workflows/appointment_saga.py
async def run_appointment_saga(payload: dict) -> dict[str, object]:
    workflow_id = f"appointment_saga_{payload['patient_id']}_{payload['slot_id']}_{uuid4()}"
    
    temporal_client = await client.Client.connect("temporal:7233")
    
    result = await temporal_client.execute_workflow(
        AppointmentSagaWorkflow.execute,
        payload,
        id=workflow_id,
        task_queue="smarthealth",
        retry_policy=WORKFLOW_RETRY,
        workflow_id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    )
    
    return result
```

### From ServiceManagement

```python
# File: app/services/service_management.py

# Publish service asynchronously via Temporal
await temporal_client.execute_workflow(
    ServicePublishWorkflow.execute,
    service_id,
    id=f"service_publish_{service_id}",
    task_queue="smarthealth",
)
```

---

## Worker Process

### Purpose
Executes activities and manages workflows.

**File:** [app/workers/service_publish_worker.py](../../app/workers/service_publish_worker.py)

```python
async def main() -> None:
    # Connect to Temporal server
    temporal_client = await client.Client.connect(
        settings.temporal_host,          # "temporal:7233"
        namespace=settings.temporal_namespace  # "default"
    )
    
    # Create worker with sandboxed environment
    temporal_worker = worker.Worker(
        temporal_client,
        workflow_runner=SandboxedWorkflowRunner(
            restrictions=SandboxRestrictions.default.with_passthrough_modules(
                "numpy",
                "pgvector",
                "sqlalchemy",
                "httpx",
            )
        ),
        task_queue=settings.temporal_task_queue,  # "smarthealth"
        workflows=[ServicePublishWorkflow, AppointmentSagaWorkflow],  # Registered workflows
        activities=[                                                   # Registered activities
            validate_service,
            structure_service,
            chunk_service,
            embed_chunks,
            # ... all 17 activities ...
        ],
    )
    
    # Start polling for work
    await temporal_worker.run()
```

**Process:**
1. Worker connects to Temporal server at `temporal:7233`
2. Joins task queue `smarthealth`
3. Polls for pending activities/workflows
4. When activity scheduled in workflow → Worker picks it up
5. Executes activity code
6. Records result back to Temporal
7. Workflow continues

---

## Docker Setup

```yaml
# docker-compose.yml
temporal:
  image: temporalio/auto:latest
  environment:
    DB: postgresql
    DB_PORT: 5432
    POSTGRES_USER: temporal
    POSTGRES_PASSWORD: temporal
    POSTGRES_DB: temporal
  depends_on:
    - postgres
  ports:
    - "7233:7233"  # gRPC port (worker connects here)

temporal-ui:
  image: temporalio/ui:latest
  ports:
    - "8080:8080"  # UI to view workflows
  depends_on:
    - temporal
```

---

## Workflow States

```
┌─────────┐
│  PENDING  (Not yet started)
└────┬────┘
     │ Worker picks up
     ▼
┌──────────────┐
│   RUNNING    │ (Executing activities)
└────┬────┬────┘
     │    │
  ✅ │    └─ ❌ Activity fails
     │       └─ Retry? 
     │          ├─ Yes → Retry
     │          └─ No → FAILED
     ▼
┌──────────────┐
│  COMPLETED   │ (All activities done)
└──────────────┘
```

---

## Determinism Rule

**Workflows must be deterministic:**

```python
# ❌ WRONG in workflow
async def execute(self, data: dict):
    timestamp = datetime.now()  # ❌ Non-deterministic!
    random_value = random.random()  # ❌ Non-deterministic!
    
# ✅ CORRECT in workflow
async def execute(self, data: dict):
    timestamp = await workflow.execute_activity(
        get_timestamp,  # Delegate to activity
        retry_policy=...,
    )
```

**Why?** Temporal replays workflow history. If same workflow deterministic, replayed = same decisions.

---

## Configuration

| Setting | Default | Purpose |
|---------|---------|---------|
| `TEMPORAL_HOST` | `localhost:7233` | Temporal server address |
| `TEMPORAL_NAMESPACE` | `default` | Namespace (isolation) |
| `TEMPORAL_TASK_QUEUE` | `smarthealth` | Queue name for workers |

---

## Monitoring & Debugging

### Temporal UI
```
Open browser: http://localhost:8080
├─ View running workflows
├─ See workflow history
├─ Inspect activity results
└─ Check errors and retries
```

### View Workflow Status
```bash
# CLI to check workflow
tctl workflow describe \
  --workflow-id appointment_saga_23_55_abc123 \
  --namespace default
```

### Worker Logs
```bash
# Check worker is connected and running
docker logs temporal-worker
# Look for: "Namespace: default" and "Task queue: smarthealth"
```

---

## Comparison: Saga Pattern vs Temporal

| Aspect | Without Temporal | With Temporal |
|--------|---|---|
| **Crash Recovery** | Retry from start | Resume from checkpoint |
| **State Management** | Developer-managed | Temporal-managed |
| **Visibility** | Logs scattered | Centralized history |
| **Deduplication** | Manual (workflow_id) | Built-in |
| **Retries** | Manual try/catch | Declarative retry policies |
| **Complexity** | High | Lower |
| **Debugging** | Hard (lost context) | Easy (full history) |

---

## Summary

| Concept | Purpose | Location |
|---------|---------|----------|
| **Workflow** | Orchestration logic | app/workers/temporal/workflows/{appointment_saga,service_publish}.py |
| **Activity** | Real work | app/temporal/activities.py |
| **Worker** | Executor | app/workers/service_publish_worker.py |
| **Retry Policy** | Automatic retries | app/workers/temporal/policies.py |
| **Server** | Stores history | temporal:7233 (Docker) |
| **UI** | Visibility | localhost:8080 |

