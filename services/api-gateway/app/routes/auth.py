from datetime import UTC, datetime, timedelta
from hashlib import pbkdf2_hmac, sha256
from hmac import compare_digest
from secrets import token_urlsafe

import jwt
from fastapi import APIRouter, HTTPException, Request, status
from moiraweave_shared.control_plane import (
    StoredApiKey,
    StoredTeam,
    StoredTeamMember,
    StoredUser,
    utc_now_iso,
)

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
    TeamCreateRequest,
    TeamMemberRequest,
    TeamMemberResponse,
    TeamResponse,
    Token,
    UserCreateRequest,
    UserResponse,
)

router = APIRouter(tags=["auth"])

# Rate-limit string for the login endpoint. Mirrors Settings.rate_limit_auth default.
# Using a literal here avoids calling get_settings() at module import time,
# which would bypass the DI system and break test overrides.
_RATE_LIMIT_AUTH = "10/minute"
_PASSWORD_HASH_ITERATIONS = 210_000


def _verify_password(plain: str, expected: str) -> bool:
    """Constant-time comparison to prevent timing attacks."""
    return compare_digest(plain.encode(), expected.encode())


def _hash_password(plain: str) -> str:
    salt = token_urlsafe(18)
    digest = pbkdf2_hmac(
        "sha256",
        plain.encode(),
        salt.encode(),
        _PASSWORD_HASH_ITERATIONS,
    ).hex()
    return f"pbkdf2_sha256${_PASSWORD_HASH_ITERATIONS}${salt}${digest}"


def _verify_password_hash(plain: str, encoded: str) -> bool:
    try:
        algorithm, raw_iterations, salt, expected = encoded.split("$", 3)
        iterations = int(raw_iterations)
    except ValueError:
        return False
    if algorithm != "pbkdf2_sha256":
        return False
    digest = pbkdf2_hmac(
        "sha256",
        plain.encode(),
        salt.encode(),
        iterations,
    ).hex()
    return compare_digest(digest, expected)


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
        team_id=api_key.team_id,
        last_used_at=api_key.last_used_at,
        revoked_at=api_key.revoked_at,
    )


def _user_response(user: StoredUser) -> UserResponse:
    return UserResponse(
        subject=user.subject,
        display_name=user.display_name,
        role=user.role,
        created_by=user.created_by,
        created_at=user.created_at,
        updated_at=user.updated_at,
        disabled_at=user.disabled_at,
    )


def _team_response(team: StoredTeam) -> TeamResponse:
    return TeamResponse(
        team_id=team.team_id,
        name=team.name,
        description=team.description,
        created_by=team.created_by,
        created_at=team.created_at,
        updated_at=team.updated_at,
    )


def _team_member_response(member: StoredTeamMember) -> TeamMemberResponse:
    return TeamMemberResponse(
        team_id=member.team_id,
        subject=member.subject,
        role=member.role,
        created_by=member.created_by,
        created_at=member.created_at,
    )


async def _find_api_key(
    control_plane: ControlPlane, key_id: str
) -> StoredApiKey | None:
    return next(
        (item for item in await control_plane.list_api_keys() if item.key_id == key_id),
        None,
    )


async def _find_team(control_plane: ControlPlane, team_id: str) -> StoredTeam | None:
    return next(
        (item for item in await control_plane.list_teams() if item.team_id == team_id),
        None,
    )


async def _store_api_key(
    control_plane: ControlPlane,
    *,
    name: str,
    subject: str,
    role: str,
    created_by: str,
    team_id: str | None,
    now: str,
) -> tuple[StoredApiKey, str]:
    secret = _api_key_secret()
    secret_hash = sha256(secret.encode()).hexdigest()
    return (
        await control_plane.create_api_key(
            secret_hash[:12],
            name,
            secret_hash,
            f"{secret[:10]}...",
            subject,
            role,
            created_by,
            team_id=team_id,
            now=now,
        ),
        secret,
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
    control_plane: ControlPlane,
) -> Token:
    """Authenticate and return a signed JWT.

    Rate-limited to 10 requests/minute per IP to mitigate brute-force attacks.
    Override ``DEMO_USERNAME`` and ``DEMO_PASSWORD`` via environment variables.
    Replace with a database-backed user store for production.
    """
    del request
    settings = get_settings()
    user = await control_plane.get_user(body.username)
    if (
        user is not None
        and user.disabled_at is None
        and _verify_password_hash(body.password, user.password_hash)
    ):
        return Token(
            access_token=_create_access_token(body.username, user.role, settings),
            subject=body.username,
            role=user.role,
        )

    if body.username == settings.demo_username and _verify_password(
        body.password, settings.demo_password.get_secret_value()
    ):
        return Token(
            access_token=_create_access_token(
                body.username, settings.demo_role, settings
            ),
            subject=body.username,
            role=settings.demo_role,
        )

    if user is not None and user.disabled_at is not None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect username or password",
        headers={"WWW-Authenticate": "Bearer"},
    )


@router.get("/me", response_model=AuthProfile, summary="Return authenticated profile")
async def current_profile(
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> AuthProfile:
    """Resolve the current bearer credential to the subject and role used by RBAC."""
    memberships = await control_plane.list_team_members(subject=current_user.subject)
    return AuthProfile(
        subject=current_user.subject,
        role=current_user.role,
        credential_type="api_key" if current_user.api_key_id else "jwt",
        api_key_id=current_user.api_key_id,
        team_id=current_user.team_id,
        teams=[membership.team_id for membership in memberships],
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

    if (
        body.team_id is not None
        and await _find_team(control_plane, body.team_id) is None
    ):
        raise HTTPException(status_code=404, detail="Team not found")
    api_key, secret = await _store_api_key(
        control_plane,
        name=body.name,
        subject=body.subject,
        role=body.role,
        created_by=current_user.subject,
        team_id=body.team_id,
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
            "team_id": api_key.team_id,
        },
    )
    return ApiKeyCreateResponse(
        **_api_key_response(api_key).model_dump(),
        secret=secret,
    )


@router.post(
    "/api-keys/{key_id}/rotate",
    response_model=ApiKeyCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Rotate API key",
)
async def rotate_api_key(
    key_id: str,
    control_plane: ControlPlane,
    current_user: AdminUser,
) -> ApiKeyCreateResponse:
    """Create a replacement API key and revoke the previous active key."""

    existing = await _find_api_key(control_plane, key_id)
    if existing is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="API key not found"
        )
    if existing.revoked_at is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="API key is already revoked"
        )

    now = utc_now_iso()
    replacement, secret = await _store_api_key(
        control_plane,
        name=existing.name,
        subject=existing.subject,
        role=existing.role,
        created_by=current_user.subject,
        team_id=existing.team_id,
        now=now,
    )
    await control_plane.revoke_api_key(existing.key_id, revoked_at=now)
    await _audit_auth(
        control_plane,
        current_user.subject,
        "api_key.rotate",
        replacement.key_id,
        metadata={
            "previous_key_id": existing.key_id,
            "subject": replacement.subject,
            "role": replacement.role,
            "name": replacement.name,
            "team_id": replacement.team_id,
        },
    )
    return ApiKeyCreateResponse(
        **_api_key_response(replacement).model_dump(),
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
            "team_id": revoked.team_id,
        },
    )
    return _api_key_response(revoked)


@router.get("/users", response_model=list[UserResponse], summary="List users")
async def list_users(
    control_plane: ControlPlane,
    current_user: AdminUser,
) -> list[UserResponse]:
    del current_user
    return [_user_response(item) for item in await control_plane.list_users()]


@router.post(
    "/users",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create or update user",
)
async def upsert_user(
    body: UserCreateRequest,
    control_plane: ControlPlane,
    current_user: AdminUser,
) -> UserResponse:
    user = await control_plane.upsert_user(
        body.subject,
        _hash_password(body.password),
        body.role,
        current_user.subject,
        display_name=body.display_name,
        now=utc_now_iso(),
    )
    await control_plane.record_audit_event(
        current_user.subject,
        "user.upsert",
        "user",
        user.subject,
        metadata={"role": user.role},
        timestamp=utc_now_iso(),
    )
    return _user_response(user)


@router.delete(
    "/users/{subject}",
    response_model=UserResponse,
    summary="Disable user",
)
async def disable_user(
    subject: str,
    control_plane: ControlPlane,
    current_user: AdminUser,
) -> UserResponse:
    disabled = await control_plane.disable_user(subject, disabled_at=utc_now_iso())
    if disabled is None:
        raise HTTPException(status_code=404, detail="User not found")
    await control_plane.record_audit_event(
        current_user.subject,
        "user.disable",
        "user",
        disabled.subject,
        metadata={"role": disabled.role},
        timestamp=utc_now_iso(),
    )
    return _user_response(disabled)


@router.get("/teams", response_model=list[TeamResponse], summary="List teams")
async def list_teams(
    control_plane: ControlPlane,
    current_user: AdminUser,
) -> list[TeamResponse]:
    del current_user
    return [_team_response(item) for item in await control_plane.list_teams()]


@router.post(
    "/teams",
    response_model=TeamResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create or update team",
)
async def upsert_team(
    body: TeamCreateRequest,
    control_plane: ControlPlane,
    current_user: AdminUser,
) -> TeamResponse:
    team = await control_plane.upsert_team(
        body.team_id,
        body.name,
        current_user.subject,
        description=body.description,
        now=utc_now_iso(),
    )
    await control_plane.record_audit_event(
        current_user.subject,
        "team.upsert",
        "team",
        team.team_id,
        metadata={"name": team.name},
        timestamp=utc_now_iso(),
    )
    return _team_response(team)


@router.get(
    "/teams/{team_id}/members",
    response_model=list[TeamMemberResponse],
    summary="List team members",
)
async def list_team_members(
    team_id: str,
    control_plane: ControlPlane,
    current_user: AdminUser,
) -> list[TeamMemberResponse]:
    del current_user
    return [
        _team_member_response(item)
        for item in await control_plane.list_team_members(team_id=team_id)
    ]


@router.post(
    "/teams/{team_id}/members",
    response_model=TeamMemberResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add or update team member",
)
async def add_team_member(
    team_id: str,
    body: TeamMemberRequest,
    control_plane: ControlPlane,
    current_user: AdminUser,
) -> TeamMemberResponse:
    if await _find_team(control_plane, team_id) is None:
        raise HTTPException(status_code=404, detail="Team not found")
    if await control_plane.get_user(body.subject) is None:
        raise HTTPException(status_code=404, detail="User not found")
    member = await control_plane.add_team_member(
        team_id,
        body.subject,
        body.role,
        current_user.subject,
        now=utc_now_iso(),
    )
    await control_plane.record_audit_event(
        current_user.subject,
        "team.member.upsert",
        "team",
        team_id,
        metadata={"subject": member.subject, "role": member.role},
        timestamp=utc_now_iso(),
    )
    return _team_member_response(member)
