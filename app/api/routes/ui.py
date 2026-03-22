from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.api.deps import require_roles
from app.core.config import get_settings
from app.models.enums import UserRole

router = APIRouter(tags=["ui"])
templates = Jinja2Templates(directory="app/templates")
settings = get_settings()


@router.get("/", response_class=HTMLResponse)
def app_index(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"api_prefix": settings.api_prefix},
    )


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={"api_prefix": settings.api_prefix},
    )


@router.get(
    "/parser-settings",
    response_class=HTMLResponse,
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def parser_settings_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="parser_settings.html",
        context={"api_prefix": settings.api_prefix},
    )
