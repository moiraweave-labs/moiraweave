from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from moiraweave_shared.control_plane import connect_postgres_control_plane
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.config import get_settings
from app.middleware.metrics import setup_metrics
from app.middleware.rate_limit import limiter
from app.middleware.telemetry import setup_tracing, shutdown_tracing
from app.routes import auth, health, search, workloads

_settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    # startup
    app.state.redis = Redis.from_url(str(_settings.redis_url), decode_responses=True)
    app.state.control_plane = await connect_postgres_control_plane(
        _settings.postgres_dsn
    )
    qdrant = AsyncQdrantClient(url=str(_settings.qdrant_url))
    if _settings.embedding_model:
        qdrant.set_model(_settings.embedding_model)
    app.state.search_enabled = bool(_settings.embedding_model)
    app.state.qdrant = qdrant
    yield
    # shutdown
    await app.state.control_plane.close()
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
setup_metrics(app)

app.include_router(health.router)
app.include_router(auth.router, prefix="/auth")
app.include_router(workloads.router)
app.include_router(search.router, prefix="/v1")
