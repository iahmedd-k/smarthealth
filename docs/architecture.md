# SmartHealth Backend Architecture

## Purpose and Scope

SmartHealth is a single-clinic healthcare operations backend. It provides authenticated APIs for patient access, provider scheduling, service discovery, appointments, billing pre-checks, visit progression, notifications, analytics, and a safety-first assistant.

This document describes backend runtime and ownership boundaries. PostgreSQL is the system of record. Redis, Temporal, Kafka, Celery, and model providers support the system but do not replace its transactional source of truth.

## Architectural Boundaries

```mermaid
graph TB
    subgraph API["API Layer (FastAPI)"]
        Auth["Authentication"]
        Endpoints["REST Endpoints"]
        Assistant["AI Endpoints<br/>Search, Assistant, Generation<br/>SSE"]
        Middleware["Middleware<br/>Correlation ID, Metrics, CORS"]
    end
    
    subgraph Services["Service Layer"]
        AppService["Appointment Service"]
        AuthService["Auth Service"]
        HealthService["Healthcare Event Service"]
        AssistantService["Assistant Service<br/>Navigation, Preparation, Availability"]
        SearchService["Search Service<br/>Grounded Results"]
        GenerationService["Communication Service<br/>Summary, Follow-up, Reports"]
    end
    
    subgraph Workflow["Workflow Orchestration (Temporal)"]
        AppointmentSaga["Appointment Saga"]
        ServicePublish["Service Publish Workflow"]
        Activities["Activities<br/>Billing, Notification, Slot, etc."]
    end
    
    subgraph Workers["Background Workers"]
        Celery["Celery Workers<br/>Task Queue"]
        KafkaConsumer["Kafka Analytics Consumer"]
        TemporalWorker["Temporal Worker"]
    end
    
    subgraph Persistence["Data Persistence"]
        PostgreSQL["PostgreSQL<br/>Domain Models, Audit"]
        Redis["Redis<br/>Idempotency, Caching<br/>Rate Limiting"]
        Kafka["Kafka<br/>Event Stream"]
    end
    
    subgraph Safety["AI Safety & Compliance"]
        SafetyService["Safety Service<br/>Refusal Detection"]
        PHIProtection["PHI Protection<br/>Question Hashing<br/>Response Redaction"]
        FakeLLM["FakeLLM<br/>Offline Testing"]
    end
    
    subgraph External["External Services"]
        LLM["LLM Provider<br/>Groq/OpenAI-compatible"]
        EmbeddingProvider["Embedding Provider<br/>HuggingFace"]
    end
    
    subgraph Monitoring["Observability"]
        Logs["Structured Logs<br/>JSON, Correlation ID"]
        Metrics["Prometheus Metrics"]
        Interactions["AI Interactions DB<br/>Audit & Analytics"]
    end
    
    Caller[Authenticated API caller] -->|REST + SSE| API
    API -->|Routing| Endpoints
    API -->|SSE: text/results, metadata, citations, done| Assistant
    Endpoints -->|Business Logic| Services
    Assistant -->|Question Processing| AssistantService
    AssistantService -->|Safety Check| SafetyService
    AssistantService -->|Cache/LLM| Redis
    AssistantService -->|Semantic Search| SearchService
    AssistantService -->|LLM Call| LLM
    Endpoints -->|SSE generation| GenerationService
    GenerationService -->|Grounded data| PostgreSQL
    GenerationService -->|Optional drafting| LLM
    AssistantService -->|Log Interaction| Interactions
    Services -->|Orchestrate| Workflow
    Workflow -->|Execute| Activities
    Activities -->|Database Access| PostgreSQL
    Services -->|Repository Pattern| PostgreSQL
    Services -->|Publish Events| Kafka
    Services -->|Idempotency| Redis
    Services -->|Rate Limit| Redis
    Celery -->|Background Tasks| PostgreSQL
    Celery -->|Publish Events| Kafka
    KafkaConsumer -->|Consume Events| Kafka
    KafkaConsumer -->|Store Analytics| PostgreSQL
    TemporalWorker -->|Run Workflows| Workflow
    SafetyService -->|Classification| AssistantService
    PHIProtection -->|Hash/Redact| AssistantService
    FakeLLM -->|Mock Responses| AssistantService
    EmbeddingProvider -->|Embeddings| SearchService
    PostgreSQL -->|Metrics| Metrics
    Services -->|Logs| Logs
    Workflow -->|Logs| Logs
    Celery -->|Logs| Logs
    KafkaConsumer -->|Logs| Logs
    
    style Caller fill:#e1f5ff
    style API fill:#fff3e0
    style Services fill:#f3e5f5
    style Workflow fill:#e8f5e9
    style Workers fill:#fce4ec
    style Persistence fill:#ede7f6
    style Safety fill:#fff9c4
    style External fill:#ffebee
    style Monitoring fill:#e0f2f1
```

## Request Flow: Appointment Booking

Clients create appointments directly with `POST /appointments`. Slot reservation is an internal step of the appointment saga; clients should not reserve a slot first and then create an appointment. If no slot is available, the waitlist flow is used instead.

```mermaid
sequenceDiagram
    participant Client
    participant API as API Server
    participant AppService as Appointment Service
    participant Idempotency as Redis Idempotency
    participant Temporal as Temporal Workflow
    participant DB as PostgreSQL
    participant Billing as Billing Activity
    participant Notification as Notification Activity
    
    Client->>API: POST /appointments (with Idempotency-Key)
    API->>AppService: create_appointment()
    AppService->>Idempotency: check(user_id, key)
    alt Cached Result
        Idempotency-->>AppService: return cached appointment
        AppService-->>API: appointment (cached)
    else New Booking
        AppService->>Idempotency: claim(user_id, key)
        AppService->>DB: create PENDING appointment
        AppService->>Temporal: start_appointment_saga()
        Temporal->>DB: validate patient & slot
        Temporal->>DB: ATOMIC: reserve slot
        Temporal->>Billing: charge_activity()
        Billing->>DB: create billing record
        Temporal->>Notification: send_confirmation()
        Notification->>DB: create notification
        Temporal->>DB: CONFIRM appointment
        AppService->>Idempotency: set(user_id, key, appointment_id)
        AppService-->>API: appointment (confirmed)
    end
    API-->>Client: 200 OK + appointment details
```

## Request Flow: AI Assistant Query

```mermaid
sequenceDiagram
    participant Client
    participant API as API Server
    participant AssistantService as Assistant Service
    participant SafetyService as Safety Service
    participant Redis as Redis Cache
    participant SearchService as Search Service
    participant LLMProvider as LLM Provider
    participant DB as AIInteraction Log
    
    Client->>API: POST /assistant/ask (SSE)
    API->>AssistantService: stream_answer(question)
    AssistantService->>SafetyService: normalize(question)
    SafetyService->>SafetyService: check length, gibberish
    AssistantService->>SafetyService: classify(question)
    
    alt Medical Advice Detected
        SafetyService-->>AssistantService: refused=True
        AssistantService->>API: SSE: refusal message
        AssistantService->>DB: log interaction (refused=True)
    else Safe Question
        AssistantService->>Redis: get_cached_answer(question_hash)
        alt Cache Hit
            Redis-->>AssistantService: cached answer
            AssistantService->>API: SSE: stream cached answer
            AssistantService->>DB: log (cache_hit=True)
        else Cache Miss
            AssistantService->>SearchService: semantic_search()
            SearchService->>SearchService: embed query
            SearchService->>DB: cosine_similarity search
            DB-->>SearchService: top-k results
            AssistantService->>LLMProvider: stream(prompt + context)
            LLMProvider-->>AssistantService: token stream
            AssistantService->>API: SSE: stream tokens
            AssistantService->>Redis: cache_answer()
            AssistantService->>API: SSE: citations event
            AssistantService->>DB: log interaction
        end
    end
    
    AssistantService->>API: SSE: done event
    API-->>Client: stream complete
```

## Data Model: Key Entities

```mermaid
erDiagram
    USER ||--o{ APPOINTMENT : books
    USER ||--o{ PATIENT : is
    USER ||--o{ PROVIDER : is
    PROVIDER ||--o{ SERVICE : offers
    PROVIDER ||--o{ SLOT : provides
    DEPARTMENT ||--o{ PROVIDER : has
    DEPARTMENT ||--o{ SERVICE : publishes
    SERVICE ||--o{ SLOT : has_slots
    SERVICE ||--o{ CONTENT_CHUNK : includes
    SLOT ||--o{ APPOINTMENT : reserved_by
    APPOINTMENT ||--o{ BILLING : has
    APPOINTMENT ||--o{ VISIT : includes
    APPOINTMENT ||--o{ APPOINTMENT_STATUS_HISTORY : tracks
    APPOINTMENT ||--o{ NOTIFICATION : triggers
    APPOINTMENT ||--o{ AI_INTERACTION : references
    PATIENT ||--o{ WAITLIST : joins
    CONTENT_CHUNK ||--o{ EMBEDDING : has
    
    USER {
        int id PK
        string email UK
        string hashed_password
        enum role "patient | provider | admin | front_desk"
        datetime created_at
    }
    
    PATIENT {
        int id PK
        int user_id FK
        string medical_record_number
        datetime created_at
    }
    
    PROVIDER {
        int id PK
        int user_id FK
        string bio
        string specialty
        datetime created_at
    }
    
    DEPARTMENT {
        int id PK
        string name UK
        string description
        datetime created_at
    }
    
    SERVICE {
        int id PK
        int department_id FK
        int provider_id FK
        string name
        string description
        enum status "DRAFT | PUBLISHED | ARCHIVED"
        boolean is_published
        datetime created_at
    }
    
    SLOT {
        int id PK
        int provider_id FK
        int service_id FK
        datetime start_datetime
        datetime end_datetime
        enum status "AVAILABLE | RESERVED | COMPLETED | CANCELLED"
        datetime created_at
    }
    
    APPOINTMENT {
        int id PK
        int patient_id FK
        int provider_id FK
        int service_id FK
        int slot_id FK
        enum status "PENDING | CONFIRMED | CANCELLED | NO_SHOW | COMPLETED"
        string idempotency_key
        datetime created_at
    }
    
    APPOINTMENT_STATUS_HISTORY {
        int id PK
        int appointment_id FK
        enum old_status
        enum new_status
        datetime changed_at
    }
    
    BILLING {
        int id PK
        int appointment_id FK
        decimal amount
        enum status "PENDING | CHARGED | REFUNDED | FAILED"
        datetime created_at
    }
    
    VISIT {
        int id PK
        int appointment_id FK
        enum status "SCHEDULED | CHECKED_IN | IN_PROGRESS | COMPLETED"
        datetime checked_in_at
        datetime started_at
        datetime completed_at
    }
    
    NOTIFICATION {
        int id PK
        int user_id FK
        int appointment_id FK
        enum type "REMINDER | CONFIRMATION | CANCELLATION"
        enum status "PENDING | SENT | FAILED"
        datetime created_at
    }
    
    CONTENT_CHUNK {
        int id PK
        int service_id FK
        int chunk_index
        string content
        string content_hash
        boolean published
        datetime created_at
    }
    
    EMBEDDING {
        int id PK
        int chunk_id FK
        vector embedding
        float similarity_score
        datetime created_at
    }
    
    AI_INTERACTION {
        int id PK
        int user_id FK
        string question_hash "sha256"
        string intent
        string answer
        boolean refused
        boolean cache_hit
        int latency_ms
        int input_tokens
        int output_tokens
        array retrieved_ids
        datetime created_at
    }
    
    WAITLIST {
        int id PK
        int patient_id FK
        int service_id FK
        datetime created_at
    }
```

## Temporal Workflow: Appointment Saga with Compensation

```mermaid
graph TD
    Start([Start Saga]) --> Validate["Validate Patient<br/>& Slot"]
    Validate -->|Valid| Reserve["ATOMIC:<br/>Reserve Slot"]
    Validate -->|Invalid| Reject["Reject Appointment"]
    Reserve --> Charge["Charge Billing"]
    Charge -->|Success| Notify["Send Confirmation"]
    Charge -->|Failed| Compensate["Compensation:<br/>Release Slot"]
    Notify -->|Success| Confirm["Confirm Appointment"]
    Notify -->|Failed| CompensateBilling["Compensate:<br/>Refund"]
    CompensateBilling --> Compensate
    Compensate --> Reject
    Confirm --> Success([Appointment Created])
    Reject --> Fail([Booking Failed])
    
    style Start fill:#e8f5e9
    style Success fill:#c8e6c9
    style Fail fill:#ffcdd2
    style Reserve fill:#fff9c4
    style Compensate fill:#ffe0b2
    style Charge fill:#f3e5f5
```




