from pydantic import BaseModel


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
