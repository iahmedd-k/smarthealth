# AI Layer: Assistant, Safety, Embeddings, and Vector Search

## Healthcare Assistant

The AI assistant provides intelligent, safety-checked answers to healthcare-related questions while protecting patient privacy and preventing harmful medical advice.

### Endpoint Access Matrix

| Endpoint | Patient | Provider | Front desk | Admin |
| --- | --- | --- | --- | --- |
| `POST /assistant/ask` | Yes, own appointments only | Yes, own provider scope | Yes | Yes |
| `POST /search` | Yes, published services only | Yes | Yes | Yes |
| `POST /api/v1/appointments/{id}/generate/summary` | No | No | Yes | Yes |
| `POST /api/v1/appointments/{id}/generate/followup` | No | No | Yes | Yes |
| `POST /api/v1/reports/generate/utilisation` | No | No | Yes | Yes |
| `GET /api/v1/tasks/{task_id}` | No | No | Yes, own task | Yes, own task |
| `GET /api/v1/analytics/ai` | No | No | Yes | Yes |

Authentication is required for all AI routes, and generation/report routes use
the `ANALYTICS_READ` or staff-only permission boundary. Patient appointment
answers are filtered by the authenticated patient; provider appointment answers
are limited to appointments associated with that provider.

### Business Users and Workflows

These endpoints are backend capabilities normally called by the web application,
not routes that every user is expected to call directly:

| Business need | Endpoint | Primary users | Practical use |
| --- | --- | --- | --- |
| Patient self-service | `POST /assistant/ask` | Patient, provider, staff | Find offered services, preparation information, availability, or the caller's own appointments. |
| Service discovery | `POST /search` | Patient, provider, staff | Search the published service catalog when the UI needs matching services and citations. |
| Visit communication | `POST /api/v1/appointments/{id}/generate/summary` | Front desk, admin | Prepare a patient-facing appointment summary or confirmation message. |
| Follow-up communication | `POST /api/v1/appointments/{id}/generate/followup` | Front desk, admin | Draft a post-visit or appointment follow-up message. |
| Operational reporting | `POST /api/v1/reports/generate/utilisation` | Front desk, admin | Generate a utilization report for a selected date range immediately. |
| Report status | `GET /api/v1/tasks/{task_id}` | Front desk, admin | Retrieve a queued report owned by the requesting staff user. |
| Operational dashboard | `GET /api/v1/analytics/summary` | Front desk, admin | View booking, visit, cancellation, and utilization metrics. |
| AI monitoring | `GET /api/v1/analytics/ai` | Front desk, admin | Monitor assistant request, refusal, latency, and cache metrics. |

Providers use the normal appointment, slot, and visit endpoints to manage care
operations. They can use the assistant for their own provider-scoped appointment
questions, but generated appointment summaries, follow-ups, utilization reports,
and analytics are intentionally restricted to front-desk and admin staff.

### Safety Architecture

All user questions pass through a **SafetyService** that:

1. **Normalizes input**: validates length (2-2000 chars), removes excess whitespace, checks for gibberish
2. **Classifies intent**: determines question type (appointment, preparation, availability, medical advice, general navigation)
3. **Detects medical advice**: refuses diagnosis, medication, treatment, or symptom questions with automatic escalation for acute conditions

```python
# Classification outcomes:
- intent: "appointment", "preparation", "availability", "navigation", "utilisation_report"
- refused: True/False (medical advice, diagnosis, symptom requests)
- acute: True/False (detected emergency language like "urgent", "bleeding", "can't breathe")
```

If medical advice is detected, the assistant immediately returns a standardized refusal message with a disclaimer, regardless of LLM availability. The refusal is persisted with `refused=True` in the database for audit and training purposes.

### PHI Protection

Protected Health Information (PHI) is scoped at multiple layers:

1. **Question hashing**: user questions are stored as SHA-256 hashes, not plaintext
2. **Response redaction**: user-scoped appointment details are redacted as `[USER_SCOPED_CONTENT_REDACTED]`
3. **Retrieval filtering**: search results exclude patient, billing, and authentication tables; only approved service-catalog fields are returned
4. **Field blacklisting**: response filtering blocks known PHI terms (patient name, email, phone, DOB, medical history)

### Answer Routing

After safety checks pass, the assistant gathers the appropriate grounded context and sends the question through the configured LLM provider for natural-language composition:

- **Appointment intent**: queries the authenticated user's own appointment history and supplies those records to the LLM
- **Preparation intent**: retrieves service preparation instructions and supplies them to the LLM
- **Availability intent**: retrieves matching published services and available slots and supplies them to the LLM
- **Service listing and navigation**: retrieves the published catalog and uses the LLM to answer naturally
- **Booking, cancellation, and rescheduling**: supplies the supported operation and authenticated context; the LLM explains the next action without claiming completion

The database and authorization layers provide facts; they do not replace the conversational model. The only intentionally hard-coded responses are safety refusals and infrastructure failure messages.

### Streaming and Caching

Responses stream Server-Sent Events (SSE) for real-time display:

```
event: text
data: response token

event: text
data: another token

event: citations
data: [{"service_id": 1, ...}]

event: done
data: null
```

For navigation queries, Redis caches complete answers using a SHA-256 key built from the normalized question, authenticated user scope, model identifier, and prompt version, with TTL `AI_CACHE_TTL_SECONDS`. Service-listing queries bypass the cache so catalog changes are reflected immediately.

### Error Handling and Timeouts

The stream is protected by `asyncio.timeout(LLM_TIMEOUT_SECONDS)`. If the LLM or retrieval exceeds the timeout:

1. Partial answer (accumulated tokens) is logged
2. A timeout message is sent to the client
3. The interaction is persisted with the partial answer for audit

### Interaction Logging

Every interaction is recorded in the `AIInteraction` table:

| Field | Description |
| --- | --- |
| `user_id` | User making the request |
| `question` | SHA-256 hash of the normalized question |
| `intent` | Classified intent (appointment, preparation, etc.) |
| `answer` | Full or partial response (redacted for user-scoped content) |
| `refused` | True if medical advice was detected |
| `cache_hit` | True if answer was cached |
| `model` | LLM model used (e.g., gpt-4o-mini) |
| `latency_ms` | End-to-end response time |
| `input_tokens` | Estimated input token count |
| `output_tokens` | Estimated output token count |
| `retrieved_ids` | Service IDs included in context |
| `prompt_version` | Version of the prompt template used |

This enables audit trails, performance analysis, and safety monitoring.

### Prompts and Safe Transcripts

Prompt templates are versioned in `app/services/assistant_prompts.py`. The safety
classifier runs before retrieval and uses `PROMPT_SAFETY_V1` as the provider
contract for model-backed classification where configured. All non-refused
assistant intents use `PROMPT_NAV_V1`; the prompt includes authenticated role,
profile identity, retrieved domain facts, and conversation history. The persisted
`prompt_version` identifies the template used for each generated result.

Raw questions and clinical conversation text are not retained as transcripts.
Questions are stored as SHA-256 hashes, appointment answers are redacted, and
interaction reconstruction uses the correlation ID, intent, and retrieved
service or appointment IDs. A safe transcript therefore contains only the
request hash, refusal status, source IDs, token counts, latency, and final
metadata. For example:

```text
request: sha256:<question-hash>
intent: medical_advice
refused: true
retrieved_ids: []
correlation_id: <request-correlation-id>
```

### Failure Modes

- **Safety refusal:** medical advice and acute symptom requests are refused
	before embeddings, retrieval, or LLM calls. The interaction is still logged.
- **No grounded match:** the assistant returns `we don't offer that` and emits
	empty citations rather than inventing a service.
- **Provider timeout or outage:** bounded timeout and retry handling returns a
	clear provider-unavailable response while preserving the interaction record.
- **Malformed report output:** structured report parsing is retried once with a
	repair instruction, then fails cleanly if the output remains invalid.
- **Redis unavailable:** cache and rate-limit operations degrade gracefully;
	core booking and assistant processing remain available where possible.
- **Client disconnect:** partial generated output and interaction metadata are
	persisted before the stream cancellation is re-raised.

### Testing Without External LLM

Local development and CI/CD use a **FakeLLM** that requires no network or API keys:

- Deterministic responses based on question content
- Routes to different response types (medical, appointment, preparation, availability, navigation)
- Simulates streaming and tokenization
- Enables full test suite to run offline

Tests cover:

- Medical advice refusal and safety
- PHI scoping (no sensitive data in responses)
- Malformed input rejection (empty, gibberish, oversized)
- Streaming response format (SSE, text events, citations, done)
- Report schema validation
- Caching behavior
- Error paths and timeouts

Run tests with:

```bash
pytest tests/unit/test_ai_layer_comprehensive.py -v
pytest tests/unit/test_assistant_safety.py -v
```

---

## Embeddings and Vector Similarity

An embedding is a numeric representation of text that captures useful aspects of its meaning. An embedding model reads a sentence or document chunk and maps it to a point in a fixed-dimensional vector space. Texts with related meaning tend to be placed near one another, even when they do not share the same words. For example, a query about "heart specialist availability" may be close to a service description that says "cardiology appointments," while a keyword-only search might miss that relationship.

SmartHealth can use this representation for semantic retrieval over approved service descriptions, clinical guidance, or other indexed content. During ingestion, long documents should be split into reasonably sized, slightly overlapping chunks. Each chunk is sent to the configured embedding model and stored with its vector plus metadata such as source, department, document version, and access scope. At query time, the user's question is embedded with the same model. The system compares that query vector with stored vectors and returns the highest-scoring matches. The current configuration targets `sentence-transformers/all-MiniLM-L6-v2`, which produces 384-dimensional embeddings, with a target chunk size of 600 tokens and 80-token overlap.

A common comparison is cosine similarity. It measures the angle between two vectors rather than their raw length: vectors pointing in nearly the same direction receive a high score, while unrelated directions receive a lower score. A vector index can use this score to perform nearest-neighbor search. For a small collection, exact comparison is practical; for a large collection, an approximate nearest-neighbor index such as HNSW can return results quickly while accepting that an occasional perfect match may be missed. `RETRIEVAL_TOP_K=5` limits the initial result set, and `RETRIEVAL_MIN_SIMILARITY=0.65` provides a minimum relevance gate. That threshold is a starting point, not a universal measure of correctness, so it should be calibrated against representative queries and reviewed whenever the model or corpus changes.

Embeddings support finding relevant context; they do not prove that two statements are equivalent, current, or clinically safe. Retrieved content should therefore be treated as evidence for a later answer, not as an answer by itself. Results must pass normal authentication and authorization filters before retrieval, and source/version metadata should be retained so responses can be traced back to approved content. Exact identifiers, dates, dosage values, and negations may require keyword or structured filters alongside vector search. In healthcare workflows, a similarity score must never be interpreted as a diagnosis, risk probability, or substitute for professional review.

## Publication and Chunk Contract

Publishing a service runs validation, structuring, chunking, embedding, and persistence stages. The publish-status response keeps the current `status` value and also exposes `stage`, `chunks_total`, and `embeddings_generated`, so clients can show progress while the workflow is running.

Every content chunk deliberately starts with the same labeled context block:

```text
Service: <service name>
Department: <department name>
Specialty: <specialty or Not specified>
Preparation instructions: <instructions or Not specified>
```

The description is then split into 120-character segments and appended to that context. Repeating the context in each chunk prevents retrieval results from losing the service identity or preparation guidance when only one chunk is returned. Chunks are embedded with the configured 384-dimensional model and stored in the `content_chunks.embedding` pgvector column.

Each indexed chunk also stores `service_id`, `department`, `specialty`, `published`, and a SHA-256 `content_hash`. The hash is calculated from the final chunk text, including the labeled context. During publication, the embedding Activity looks up existing rows by service, chunk index, and hash. Unchanged chunks reuse their stored vector; only new or changed chunks are sent to the provider. Provider calls are bounded by `EMBEDDING_BATCH_SIZE` and retried by Temporal using the configured Activity retry policy.

Persistence is handled by `ContentChunkRepository`, not by the embedding provider. Re-indexing replaces all chunks for the service before the service is marked published. The unique `(service_id, chunk_index)` constraint and replacement behavior remove stale chunks and prevent duplicate indexes. Existing rows created before hash support are embedded once on their next publication and then receive a hash.

## Service Search

`POST /search` accepts `{"query": "...", "limit": 5}` and requires authentication. The endpoint caps the requested limit at `RETRIEVAL_TOP_K`, ranks candidates by cosine similarity, and omits results below `RETRIEVAL_MIN_SIMILARITY`. Only the highest-scoring chunk per service is returned.
This endpoint also streams results as Server-Sent Events (SSE), providing real-time updates to clients.

Search requires both the stored chunk flag `published=true` and the live service flag `is_published=true`. This prevents stale or unpublished vectors from being returned after catalog changes. Results contain only approved service-catalog fields: `service_id`, `service_name`, `score`, `department`, `specialty`, and chunk content. Patient, appointment, billing, authentication, and other PHI-bearing tables are not joined or returned. Authentication is enforced by the API dependency, while service filtering is enforced in the repository layer.

When `EMBEDDING_API_KEY` is unset, local development uses a deterministic token-based embedding so workflows and tests remain runnable. Production deployments should configure the selected embedding provider and key.

## Retrieval Evaluation

The checked-in evaluation set at `evaluation/retrieval_eval.json` contains 10 query and expected-service pairs. Run it with:

```bash
python scripts/eval_retrieval.py --limit 5
```

The script reports `top_k`, the active similarity `threshold`, total cases, hits, hit rate, and per-case results. A hit means the expected service name or ID appears in the returned top-k results. The result is corpus-dependent: run it after services are published and vectors are indexed, and record the output when changing the model, threshold, chunking, or service corpus.

The evaluation harness is committed and ready, but no reproducible hit-rate number is recorded in this development checkout because its local SQLite database has no indexed `content_chunks` corpus. The honest baseline/current status is:

| Run | Corpus state | Result |
| --- | --- | --- |
| Before retrieval safeguards | Not captured | Not comparable |
| Current implementation | Dataset committed; local corpus unavailable | Run the command above after migration and publication |

Do not treat a missing corpus or an empty result as a zero-quality model. Run the evaluation against the same published corpus before and after any model, threshold, or chunking change.

Further reading:

- [Sentence Transformers: Semantic Search](https://www.sbert.net/examples/sentence_transformer/applications/semantic-search/README.html)
- [Hugging Face Inference Client: Feature Extraction](https://huggingface.co/docs/huggingface_hub/en/package_reference/inference_client#huggingface_hub.InferenceClient.feature_extraction)
