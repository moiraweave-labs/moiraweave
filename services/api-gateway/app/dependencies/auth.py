from collections.abc import Awaitable, Callable
from hashlib import sha256
from hmac import compare_digest
from typing import Annotated

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import InvalidTokenError

from app.config import get_settings
from app.dependencies.control_plane import ControlPlane  # noqa: TC001
from app.models.auth import TokenData

_bearer_scheme = HTTPBearer()
_ROLES = {"viewer": 0, "operator": 1, "admin": 2}


def _auth_error() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )


def _normalize_role(value: object) -> str:
    role = str(value or "viewer").strip().lower()
    return role if role in _ROLES else "viewer"


def _api_key_id(secret: str) -> str:
    return sha256(secret.encode()).hexdigest()[:12]


async def _api_key_user(token: str, control_plane: ControlPlane) -> TokenData | None:
    settings = get_settings()
    for raw_entry in settings.moira_api_keys.split(","):
        entry = raw_entry.strip()
        if not entry:
            continue
        try:
            secret, subject, role = [part.strip() for part in entry.split(":", 2)]
        except ValueError:
            continue
        if secret and subject and compare_digest(token, secret):
            return TokenData(
                subject=subject,
                role=_normalize_role(role),
                api_key_id=_api_key_id(secret),
                team_id=None,
            )
    secret_hash = sha256(token.encode()).hexdigest()
    stored = await control_plane.get_api_key_by_secret_hash(secret_hash)
    if stored is None:
        return None
    stored_user = await control_plane.get_user(stored.subject)
    if stored_user is not None and stored_user.disabled_at is not None:
        return None
    await control_plane.touch_api_key(stored.key_id)
    return TokenData(
        subject=stored.subject,
        role=_normalize_role(stored.role),
        api_key_id=stored.key_id,
        team_id=stored.team_id,
    )


async def _jwt_user_from_payload(
    payload: dict[str, object], control_plane: ControlPlane
) -> TokenData:
    subject = payload.get("sub")
    if not isinstance(subject, str):
        raise _auth_error()

    stored_user = await control_plane.get_user(subject)
    if stored_user is not None:
        if stored_user.disabled_at is not None:
            raise _auth_error()
        return TokenData(
            subject=stored_user.subject,
            role=_normalize_role(stored_user.role),
        )

    return TokenData(subject=subject, role=_normalize_role(payload.get("role")))


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(_bearer_scheme)],
    control_plane: ControlPlane,
) -> TokenData:
    """Validate Bearer JWT or configured API key and return the token payload.

    :raises HTTPException: 401 if the token is missing, expired or invalid.
    """
    exc = _auth_error()
    settings = get_settings()
    token = credentials.credentials
    try:
        payload: dict[str, object] = jwt.decode(
            token,
            settings.jwt_secret_key.get_secret_value(),
            algorithms=[settings.jwt_algorithm],
        )
        return await _jwt_user_from_payload(payload, control_plane)
    except InvalidTokenError as err:
        api_key_user = await _api_key_user(token, control_plane)
        if api_key_user is not None:
            return api_key_user
        raise exc from err


def require_role(minimum_role: str) -> Callable[[TokenData], Awaitable[TokenData]]:
    async def _dependency(
        current_user: Annotated[TokenData, Depends(get_current_user)],
    ) -> TokenData:
        if _ROLES.get(current_user.role, -1) < _ROLES[minimum_role]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires {minimum_role} role",
            )
        return current_user

    return _dependency


# Convenience alias for use in route signatures
CurrentUser = Annotated[TokenData, Depends(get_current_user)]
OperatorUser = Annotated[TokenData, Depends(require_role("operator"))]
AdminUser = Annotated[TokenData, Depends(require_role("admin"))]
