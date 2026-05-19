from datetime import UTC, datetime, timedelta
from hmac import compare_digest
from typing import Annotated

import jwt
from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.config import Settings, get_settings
from app.middleware.rate_limit import limiter
from app.models.auth import LoginRequest, Token

router = APIRouter(tags=["auth"])


def _verify_password(plain: str, expected: str) -> bool:
    """Constant-time comparison to prevent timing attacks."""
    return compare_digest(plain.encode(), expected.encode())


def _create_access_token(subject: str, settings: Settings) -> str:
    expire = datetime.now(UTC) + timedelta(
        minutes=settings.jwt_access_token_expire_minutes
    )
    return jwt.encode(
        {"sub": subject, "exp": expire},
        settings.jwt_secret_key.get_secret_value(),
        algorithm=settings.jwt_algorithm,
    )


@router.post("/token", response_model=Token, summary="Issue JWT access token")
@limiter.limit(get_settings().rate_limit_auth)
async def login(
    request: Request,
    body: LoginRequest,
    settings: Annotated[Settings, Depends(get_settings)],
) -> Token:
    """Authenticate and return a signed JWT.

    Rate-limited to 10 requests/minute per IP to mitigate brute-force attacks.
    Override ``DEMO_USERNAME`` and ``DEMO_PASSWORD`` via environment variables.
    Replace with a database-backed user store for production.
    """
    if body.username != settings.demo_username or not _verify_password(
        body.password, settings.demo_password.get_secret_value()
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return Token(access_token=_create_access_token(body.username, settings))
