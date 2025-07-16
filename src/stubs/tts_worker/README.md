# 🔊 TTS Worker (Stub)

**Service Type:** Independent microservice (FastAPI)

## 🎯 Purpose
The Text-to-Speech (TTS) Worker will eventually **generate audio renditions of book descriptions, recommendations, or entire books** to improve accessibility and engagement for early readers or students with visual impairments.  For now it is a *stub* exposing health and process endpoints so service discovery, deployment, and observability patterns can be validated end-to-end.

## 🌐 API Contract (Frozen)
| Method | Path | Description | Request Body | Response |
|--------|------|-------------|--------------|----------|
| `GET`  | `/health`  | Liveness/readiness check | *None* | `{ "status": "ok", "stub": true }` |
| `POST` | `/process` | Request TTS generation for a text snippet or `book_id`. Future versions will stream or store the resulting audio (e.g., MP3). | `{ "book_id": "B123", "text": "Once upon a time..." }` | `{ "message": "accepted", "stub": true }` (future: job id) |

> **Compatibility**: This contract must stay stable until a versioned API is introduced.  Downstream services should not depend on synchronous audio generation; they will receive an event when audio is ready.

## 🏗️ Implementation Roadmap
1. **Input Validation**: Enforce maximum input length and safe characters.
2. **TTS Engine Integration**: Evaluate Amazon Polly, Google Cloud TTS, ElevenLabs, or local models.
3. **Streaming & Storage**: Store audio in object storage (S3/MinIO) and return secure URL; optionally stream via WebSockets.
4. **Event Emission**: Publish `tts_generated` event once audio is ready.
5. **Caching**: Cache generated audio by `(book_id, language, voice)` key.
6. **Metrics**: Track generation latency, success/failure counts, audio length.

## ⚙️ Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `TTS_PROVIDER` | `polly` | polly \| gcloud \| elevenlabs \| local |
| `AUDIO_STORAGE_BUCKET` | `book-audio` | S3/MinIO bucket for audio files |
| `KAFKA_BROKERS` | `kafka:9092` | Bootstrap servers |
| `MAX_TEXT_LENGTH` | `1000` | Safeguard against excessive input |
| `REQUEST_TIMEOUT_SEC` | `30` | Upstream provider timeout |

## 🚀 Local Development
```bash
# Build & run the stub
cd src/stubs/tts_worker
docker build -t tts_worker_stub .
docker run -p 8011:8000 tts_worker_stub

# Health check
curl http://localhost:8011/health
```

## 📝 Testing Strategy
* **Unit tests** (future): Mock provider SDKs to verify request formatting.
* **Integration tests**: Use local TTS engine container for deterministic output.

## 🔒 Security Considerations (Future)
* Sanitize text input to avoid injection.
* Rate-limit requests per user to manage provider costs.
* Store audio with private ACL and serve via signed URLs or CDN.

## 🔄 Deployment & Scaling
* **Stateless service** suitable for horizontal scaling.
* **Concurrency limits**: Max simultaneous TTS jobs per pod based on CPU/Memory.
* **Auto-scaling**: Scale by queue depth or Kafka consumer lag.

---
*Status: stub only.  API contract & roadmap defined; real TTS implementation to follow once provider is chosen.* 