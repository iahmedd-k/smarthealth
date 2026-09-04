# SmartHealth Product Requirements

## 1. Product purpose

SmartHealth is a healthcare scheduling and operations demonstration. It lets staff configure providers, departments, services, and discrete appointment slots. Patients can discover published services, book appointments safely during contention, follow a validated visit lifecycle, and receive consistent state and billing outcomes.

The system is scoped to one clinic and demonstrates authorization, transactional persistence, durable Temporal workflows, atomic slot claims, bounded background retries, event-driven analytics, and traceable logs.

## 2. Use cases

1. Staff register a provider, assign a department and specialty, create a service, publish it, and verify searchable content chunks.
2. A patient lists available slots and submits a booking. The saga validates eligibility, atomically reserves the slot, performs billing pre-check, schedules a reminder, and confirms the appointment.
3. Multiple patients race for one slot. Exactly one request succeeds because the database conditional update is authoritative.
4. A client retries a booking with the same idempotency key. The original appointment returns without another appointment or billing row.
5. A forced billing failure compensates the saga, preserves history, releases the slot, and allows a waiting patient to be promoted.
6. Front desk staff check in, start, and complete a visit. Invalid jumps and repeated transitions are rejected or idempotent.
7. Operators reconcile analytics and follow a booking across API, Temporal, Celery, and Kafka logs by correlation ID.

## 3. Functional requirements

- Authenticate users with bcrypt password hashes and JWT access tokens.
- Support patient, provider, front_desk, and admin roles with server-side PHI authorization.
- Manage departments, provider profiles, specialties, services, schedules, slots, appointments, billing, visits, and waitlists.
- Expose authenticated, paginated catalog and schedule APIs; patients see only published services and available slots.
- Publish service content through Temporal activities: validate, structure, chunk, embed, and persist.
- Run booking as a Temporal saga with validation, atomic reservation, billing, reminder, confirmation, and compensation activities.
- Record appointment status history and domain audit records for operational changes.
- Publish versioned Kafka envelopes and consume them idempotently into analytics tables.
- Retry transient Celery failures with bounded backoff and record terminal failures in `failed_jobs`.

## 4. Non-functional requirements

- No double booking under concurrent requests.
- Workflow activities are idempotent and chunk replacement is atomic at the service persistence boundary.
- Database mutations use repository methods and commit audit rows with the business mutation.
- Events carry identifiers and correlation metadata but no patient names, contact details, or clinical PHI.
- The backend exposes health, readiness, metrics, and version checks; tests run with `pytest -q` or `make test`.
- Health, metrics, structured JSON logs, and a recovery runbook are available.

## 5. Milestones

| Milestone | Delivered capability |
| --- | --- |
| Week 1 | Authentication, roles, departments, providers, services, slots, migrations |
| Week 2 | Published service chunks, semantic search, Temporal publishing workflow |
| Week 3 | Scheduling saga, atomic reservation, billing compensation, visits, Kafka/Celery analytics, observability |

## 6. Traceability Matrix

| Feature | Requirement | Implementation | Verification |
| --- | --- | --- | --- |
| **Authentication** | Registration/login with bcrypt hashes and JWT tokens | `app/api/v1/endpoints/auth.py`, `app/core/security.py` | `tests/integration/test_api.py::test_*_auth*` |
| **Authorization** | Role-based access control (patient, provider, admin) | `app/core/authorization/`, endpoint decorators | Protected endpoint tests verify role enforcement |
| **PHI Protection** | Patient data authorization at repository level | `app/core/authorization/service.py`, query filtering | Authorization tests; endpoint integration tests |
| **Departments** | Create, list, and reference departments | `app/repositories/departments.py`, endpoints | CRUD and query tests |
| **Providers** | Manage provider profiles and specialties | `app/repositories/providers.py` | Provider lifecycle tests |
| **Services** | Publish/unpublish service offerings | `app/services/` and endpoints | Service publication workflow tests |
| **Service Chunks** | Embed content into searchable chunks | `app/workers/temporal/workflows/service_publish.py` | Chunk generation and embedding tests |
| **Semantic Search** | Vector search over service content | `ContentChunkRepository`, search endpoint | Retrieval evaluation in `scripts/eval_retrieval.py` |
| **Slots** | Create and manage available appointment slots | `app/repositories/slots.py` | Slot reservation and availability tests |
| **Atomic Booking** | No double-booking under concurrent load | `SlotRepository.reserve_for_patient` with DB constraint | Race condition demo in `tests/demo_tasks/race_demo.py` |
| **Booking Idempotency** | Retry safety via idempotency keys | `app/core/idempotency.py`, `RedisIdempotencyStore` | `test_appointment_idempotency` integration test |
| **Booking Saga** | Multi-step appointment workflow | `AppointmentSagaWorkflow`, Temporal activities | Temporal workflow tests |
| **Billing Safety** | Pre-check and compensation on failure | `BillingChecker`, compensation activities | Forced-failure integration scenario |
| **Visit Lifecycle** | Check-in, start, complete state transitions | Appointment endpoints and state machine | Visit state transition tests |
| **Analytics Events** | Publish events to Kafka; consume idempotently | `KafkaEventPublisher`, `AnalyticsConsumer` | Kafka event integration tests |
| **Celery Retries** | Bounded retry + failed job logging | Celery config, `FailedJobService` | Task execution tests |
| **Audit Logs** | Track all business mutations | `AuditLog` model + repository audit methods | Audit log presence tests |
| **Observability** | Correlation IDs, structured logs, metrics | `CorrelationIdMiddleware`, `HTTPMetricsMiddleware`, `AIInteraction` logging | Log structure validation; metrics endpoint tests |
| **AI Assistant - Safety** | Refuse medical advice, emergency escalation | `SafetyService`, `decision.refused`, `decision.acute` | `test_assistant_refuses_*` in `test_ai_layer_comprehensive.py` |
| **AI Assistant - PHI** | Hash questions, redact responses, filter retrieval | Question hashing, response redaction in `AssistantService` | `test_ai_assistant_phi_*` tests |
| **AI Assistant - Intent** | Route to appointment, preparation, availability, navigation | Intent classification in `SafetyService` | Intent routing tests |
| **AI Assistant - Streaming** | SSE with text tokens, citations, done event | `AssistantService.stream_answer` async generator | `test_streaming_*` format and shape tests |
| **AI Assistant - Caching** | Cache navigation answers in Redis | `AIRedisStore.cache_answer/get_cached_answer` | Caching behavior tests |
| **AI Assistant - Timeout** | Timeout protection for LLM calls | `asyncio.timeout(LLM_TIMEOUT_SECONDS)` | Timeout error handling tests |
| **AI Assistant - Testing** | No-network test suite with FakeLLM | `tests/conftest_llm.py`, `FakeLLM` class | Full `test_ai_layer_comprehensive.py` suite |
| **Health & Readiness** | Service liveness and dependency checks | `GET /health`, `GET /health/ready` endpoints | Health endpoint tests |
| **Metrics** | Prometheus metrics for observability | `GET /metrics`, prometheus-client integration | Metrics endpoint query tests |

## 7. Out of scope

The following are **explicitly out of scope** and should not be implemented:

- **Real email/SMS delivery:** No SMTP, SendGrid, Twilio, or similar integrations. Notifications are tracked in the database with status markers (PENDING → SENT) but no actual delivery mechanism exists.
- **Real payment/insurance integrations:** Billing pre-check is a mock that always approves or fails based on configuration.
- **Calendar synchronization:** No Google Calendar, Outlook, or iCalendar integration.
- **Document parsing:** No PDF/image OCR or scanning. All operational content is text-based.
- **Multi-clinic tenancy:** Single clinic scope only. No cross-facility inventory or schedule synchronization.
- **Clinical records management:** No EHR integration or medical record storage.
- **Provider time-off rules:** No provider unavailability or vacation schedules.
- **Production secret management:** Secrets are hardcoded for demo purposes. Use a vault in production.
