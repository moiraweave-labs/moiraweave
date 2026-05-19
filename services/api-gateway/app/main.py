from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.config import get_settings
from app.middleware.rate_limit import limiter
from app.middleware.telemetry import setup_tracing, shutdown_tracing
from app.routes import auth, health, pipelines, search

_settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    # startup
    app.state.redis = Redis.from_url(str(_settings.redis_url), decode_responses=True)
    qdrant = AsyncQdrantClient(url=str(_settings.qdrant_url))
    qdrant.set_model(_settings.embedding_model)
    app.state.qdrant = qdrant
    yield
    # shutdown
    await app.state.redis.aclose()
    await app.state.qdrant.close()
    shutdown_tracing()


app = FastAPI(
    title=_settings.app_name,
    version=_settings.app_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# OTel must be set up after app creation so FastAPIInstrumentor can patch routes
setup_tracing(app, _settings)

app.include_router(health.router)
app.include_router(auth.router, prefix="/auth")
app.include_router(pipelines.router)
app.include_router(search.router, prefix="/v1")

# Expose Prometheus metrics at /metrics (scraped by Prometheus ServiceMonitor)
Instrumentator().instrument(app).expose(app)
