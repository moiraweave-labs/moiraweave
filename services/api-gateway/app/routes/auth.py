from datetime import UTC, datetime, timedelta
from hashlib import sha256
from hmac import compare_digest
from secrets import token_urlsafe

import jwt
from fastapi import APIRouter, HTTPException, Request, status
from moiraweave_shared.control_plane import StoredApiKey, utc_now_iso

from app.config import Settings, get_settings
from app.dependencies.auth import AdminUser, CurrentUser  # noqa: TC001
from app.dependencies.control_plane import ControlPlane  # noqa: TC001
from app.middleware.rate_limit import limiter
from app.models.auth import (
    ApiKeyCreateRequest,
    ApiKeyCreateResponse,
    ApiKeyResponse,
    AuthProfile,
    LoginRequest,
    Token,
)

router = APIRouter(tags=["auth"])

# Rate-limit string for the login endpoint. Mirrors Settings.rate_limit_auth default.
# Using a literal here avoids calling get_settings() at module import time,
# which would bypass the DI system and break test overrides.
_RATE_LIMIT_AUTH = "10/minute"


def _verify_password(plain: str, expected: str) -> bool:
    """Constant-time comparison to prevent timing attacks."""
    return compare_digest(plain.encode(), expected.encode())


def _create_access_token(subject: str, role: str, settings: Settings) -> str:
    expire = datetime.now(UTC) + timedelta(
        minutes=settings.jwt_access_token_expire_minutes
    )
    return jwt.encode(
        {"sub": subject, "role": role, "exp": expire},
        settings.jwt_secret_key.get_secret_value(),
        algorithm=settings.jwt_algorithm,
    )


def _api_key_secret() -> str:
    return f"mwk_{token_urlsafe(32)}"


def _api_key_response(api_key: StoredApiKey) -> ApiKeyResponse:
    return ApiKeyResponse(
        key_id=api_key.key_id,
        name=api_key.name,
        subject=api_key.subject,
        role=api_key.role,
        secret_prefix=api_key.secret_prefix,
        created_by=api_key.created_by,
        created_at=api_key.created_at,
        last_used_at=api_key.last_used_at,
        revoked_at=api_key.revoked_at,
    )


async def _audit_auth(
    control_plane: ControlPlane,
    actor: str,
    action: str,
    resource_id: str,
    *,
    metadata: dict[str, object] | None = None,
) -> None:
    await control_plane.record_audit_event(
        actor,
        action,
        "api_key",
        resource_id,
        metadata=metadata,
        timestamp=utc_now_iso(),
    )


@router.post("/token", response_model=Token, summary="Issue JWT access token")
@limiter.limit(_RATE_LIMIT_AUTH)
async def login(
    request: Request,
    body: LoginRequest,
) -> Token:
    """Authenticate and return a signed JWT.

    Rate-limited to 10 requests/minute per IP to mitigate brute-force attacks.
    Override ``DEMO_USERNAME`` and ``DEMO_PASSWORD`` via environment variables.
    Replace with a database-backed user store for production.
    """
    del request
    settings = get_settings()
    if body.username != settings.demo_username or not _verify_password(
        body.password, settings.demo_password.get_secret_value()
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return Token(
        access_token=_create_access_token(body.username, settings.demo_role, settings),
        subject=body.username,
        role=settings.demo_role,
    )


@router.get("/me", response_model=AuthProfile, summary="Return authenticated profile")
async def current_profile(current_user: CurrentUser) -> AuthProfile:
    """Resolve the current bearer credential to the subject and role used by RBAC."""
    return AuthProfile(
        subject=current_user.subject,
        role=current_user.role,
        credential_type="api_key" if current_user.api_key_id else "jwt",
        api_key_id=current_user.api_key_id,
    )


@router.get(
    "/api-keys",
    response_model=list[ApiKeyResponse],
    summary="List API keys",
)
async def list_api_keys(
    control_plane: ControlPlane,
    current_user: AdminUser,
) -> list[ApiKeyResponse]:
    """Return API key metadata without exposing secret values."""

    del current_user
    return [_api_key_response(item) for item in await control_plane.list_api_keys()]


@router.post(
    "/api-keys",
    response_model=ApiKeyCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create API key",
)
async def create_api_key(
    body: ApiKeyCreateRequest,
    control_plane: ControlPlane,
    current_user: AdminUser,
) -> ApiKeyCreateResponse:
    """Create a hashed API key and return the secret once."""

    secret = _api_key_secret()
    secret_hash = sha256(secret.encode()).hexdigest()
    api_key = await control_plane.create_api_key(
        secret_hash[:12],
        body.name,
        secret_hash,
        f"{secret[:10]}...",
        body.subject,
        body.role,
        current_user.subject,
        now=utc_now_iso(),
    )
    await _audit_auth(
        control_plane,
        current_user.subject,
        "api_key.create",
        api_key.key_id,
        metadata={
            "subject": api_key.subject,
            "role": api_key.role,
            "name": api_key.name,
        },
    )
    return ApiKeyCreateResponse(
        **_api_key_response(api_key).model_dump(),
        secret=secret,
    )


@router.delete(
    "/api-keys/{key_id}",
    response_model=ApiKeyResponse,
    summary="Revoke API key",
)
async def revoke_api_key(
    key_id: str,
    control_plane: ControlPlane,
    current_user: AdminUser,
) -> ApiKeyResponse:
    """Revoke an API key without deleting its audit-relevant metadata."""

    revoked = await control_plane.revoke_api_key(key_id, revoked_at=utc_now_iso())
    if revoked is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="API key not found"
        )
    await _audit_auth(
        control_plane,
        current_user.subject,
        "api_key.revoke",
        revoked.key_id,
        metadata={
            "subject": revoked.subject,
            "role": revoked.role,
            "name": revoked.name,
        },
    )
    return _api_key_response(revoked)
