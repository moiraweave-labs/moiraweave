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
    team_id: str | None = None


class AuthProfile(BaseModel):
    subject: str
    role: str
    credential_type: str
    api_key_id: str | None = None
    team_id: str | None = None
    teams: list[str] = Field(default_factory=list)


class LoginRequest(BaseModel):
    username: str
    password: str


class BootstrapAdminRequest(BaseModel):
    subject: str = Field(min_length=1, max_length=120)
    password: str = Field(min_length=12, max_length=256)
    display_name: str | None = Field(default=None, max_length=160)

    @field_validator("subject", "display_name")
    @classmethod
    def strip_optional(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value


class ApiKeyCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    subject: str = Field(min_length=1, max_length=120)
    role: Literal["admin", "operator", "viewer"] = "operator"
    team_id: str | None = Field(default=None, min_length=1, max_length=80)

    @field_validator("name", "subject", "team_id")
    @classmethod
    def strip_non_empty(cls, value: str | None) -> str | None:
        if value is None:
            return None
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
    team_id: str | None = None
    last_used_at: str | None = None
    revoked_at: str | None = None


class ApiKeyCreateResponse(ApiKeyResponse):
    secret: str


class UserCreateRequest(BaseModel):
    subject: str = Field(min_length=1, max_length=120)
    password: str = Field(min_length=8, max_length=256)
    role: Literal["admin", "operator", "viewer"] = "operator"
    display_name: str | None = Field(default=None, max_length=160)

    @field_validator("subject", "display_name")
    @classmethod
    def strip_optional(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value


class UserUpdateRequest(BaseModel):
    role: Literal["admin", "operator", "viewer"] | None = None
    display_name: str | None = Field(default=None, max_length=160)

    @field_validator("display_name")
    @classmethod
    def strip_display_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value


class UserPasswordChangeRequest(BaseModel):
    current_password: str = Field(min_length=1, max_length=256)
    new_password: str = Field(min_length=12, max_length=256)


class UserPasswordResetRequest(BaseModel):
    new_password: str = Field(min_length=12, max_length=256)


class UserResponse(BaseModel):
    subject: str
    display_name: str | None = None
    role: str
    created_by: str
    created_at: str
    updated_at: str
    disabled_at: str | None = None


class TeamCreateRequest(BaseModel):
    team_id: str = Field(min_length=1, max_length=80)
    name: str = Field(min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=500)

    @field_validator("team_id", "name", "description")
    @classmethod
    def strip_non_empty_optional(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value


class TeamUpdateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=500)

    @field_validator("name", "description")
    @classmethod
    def strip_non_empty_optional(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value


class TeamResponse(BaseModel):
    team_id: str
    name: str
    description: str | None = None
    created_by: str
    created_at: str
    updated_at: str


class TeamMemberRequest(BaseModel):
    subject: str = Field(min_length=1, max_length=120)
    role: Literal["admin", "operator", "viewer"] = "operator"

    @field_validator("subject")
    @classmethod
    def strip_subject(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value


class TeamMemberResponse(BaseModel):
    team_id: str
    subject: str
    role: str
    created_by: str
    created_at: str
