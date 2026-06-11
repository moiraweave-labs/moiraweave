from typing import Literal

from pydantic import BaseModel, Field, field_validator


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
    subject: str
    role: str


class TokenData(BaseModel):
    subject: str
    role: str = "admin"
    api_key_id: str | None = None


class AuthProfile(BaseModel):
    subject: str
    role: str
    credential_type: str
    api_key_id: str | None = None


class LoginRequest(BaseModel):
    username: str
    password: str


class ApiKeyCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    subject: str = Field(min_length=1, max_length=120)
    role: Literal["admin", "operator", "viewer"] = "operator"

    @field_validator("name", "subject")
    @classmethod
    def strip_non_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value


class ApiKeyResponse(BaseModel):
    key_id: str
    name: str
    subject: str
    role: str
    secret_prefix: str
    created_by: str
    created_at: str
    last_used_at: str | None = None
    revoked_at: str | None = None


class ApiKeyCreateResponse(ApiKeyResponse):
    secret: str
