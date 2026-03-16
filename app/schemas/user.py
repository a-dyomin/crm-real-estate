from datetime import datetime

from pydantic import BaseModel, EmailStr, Field

from app.models.enums import UserRole


class UserCreate(BaseModel):
    email: EmailStr
    full_name: str = Field(min_length=3, max_length=255)
    phone: str | None = None
    role: UserRole = UserRole.agent
    password: str = Field(min_length=8, max_length=128)
    agency_id: int | None = None


class UserRead(BaseModel):
    id: int
    agency_id: int
    email: str
    full_name: str
    phone: str | None
    role: UserRole
    is_active: bool
    last_login_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class UserToggleActive(BaseModel):
    is_active: bool

