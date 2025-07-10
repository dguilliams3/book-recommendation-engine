# 📷 Image Worker (Stub)

**Service Type:** Independent microservice (FastAPI)

## 🎯 Purpose
The Image Worker’s long-term goal is to **fetch, generate, and manage cover images or thumbnails** for books in the catalog.  In the current version of the system it is a *stub*—a placeholder that exposes health and process endpoints so the rest of the distributed system can be wired end-to-end without blocking on image-processing implementation.

## 🌐 API Contract (Frozen)
Even though the internal logic is not yet implemented, the **external contract is fixed** to avoid breaking changes once real logic is added.

| Method | Path | Description | Request Body | Response |
|--------|------|-------------|--------------|----------|
| `GET`  | `/health`  | Liveness/readiness check | *None* | `{ "status": "ok", "stub": true }` |
| `POST` | `/process` | Accepts a request to process an image for a specific `book_id`. Future versions will download the image, resize/optimise it, and store it in object storage (e.g., S3). | `{ "book_id": "B123", "image_url": "https://..." }` | `{ "message": "accepted", "stub": true }` (future: job id) |

> **Breaking Changes Policy:** Any change to the request/response shape requires a new versioned endpoint (`/v2/process`) or an event-driven contract update.  Never change the behaviour of the existing endpoint in a backwards-incompatible way.

## 🏗️ Implementation Roadmap
1. **Kafka Consumer**: Listen to `book_enriched` events that contain `cover_image_url`.
2. **Image Processing**: Download, verify, resize (multiple resolutions), optimise (WebP/AVIF), generate placeholder images.
3. **Storage & CDN**: Upload processed images to S3 (or MinIO in dev) and cache via CDN.  Persist storage URL back to `catalog` table.
4. **Observability**: Emit metrics—processing latency, failure counts, image size stats.
5. **Retry Logic**: Exponential backoff & dead-letter queue for failed downloads.

## ⚙️ Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `IMAGE_STORAGE_BUCKET` | `book-covers` | S3/MinIO bucket name |
| `KAFKA_BROKERS` | `kafka:9092` | Bootstrap servers |
| `MAX_IMAGE_SIZE_MB` | `5` | Hard limit for input images |
| `PROCESS_TIMEOUT_SEC` | `20` | Guard against slow downloads |

## 🚀 Local Development
```bash
# Build & run the stub
cd src/stubs/image_worker
docker build -t image_worker_stub .
docker run -p 8010:8000 image_worker_stub

# Health check
curl http://localhost:8010/health
```

## 📝 Testing Strategy
* **Unit tests** (to be added): Validate image validation/resizing functions.
* **Integration tests**: Spin up MinIO + Kafka via docker-compose override and verify end-to-end flow.

## 🔒 Security Considerations (Future)
* Validate input URLs to prevent SSRF.
* Enforce MIME type checks and file size limits.
* Store images with private ACL, serve via signed URLs or CDN.

## 🔄 Deployment & Scaling
* **Stateless**: Multiple replicas can run behind a load balancer.
* **Horizontal scaling** driven by Kafka consumer lag.
* **Resource limits**: CPU-bound during image transforms, consider GPU acceleration if needed.

---
*Current status: stub only.  No image processing yet; contract and roadmap are defined to unblock integration.* 