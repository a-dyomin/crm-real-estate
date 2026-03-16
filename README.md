# CRE CRM - Commercial Real Estate

Vertical CRM for commercial real estate agencies with parser workflow, role access model, and manager dashboard.

## Implemented Scope

### Phase 1.1
- Core entities: properties, leads, deals, parser results.
- Parser Hub with batch ingest and dedup status.
- Convert parser result to lead/deal.

### Phase 1.2
- Authentication page (login only, registration disabled).
- Token-based API auth.
- Role-based permissions (admin, call-center, sales, agent, manager).
- Admin-only user creation and activation control.
- Audit logs for critical actions.

### Phase 1.3
- Manager dashboard summary (pipeline, conversion, parser stats).
- Tabbed UI with isolated work areas:
  - `Главная`
  - `Лиды` (Kanban)
  - `Сделки` (Kanban)
  - `Parser Hub`
  - `Пользователи` (admin only)

## Tech Stack
- Python 3.12+
- FastAPI
- SQLAlchemy
- SQLite (default, can be switched)
- Vanilla JS + Jinja templates

## Quick Start

```bash
python -m venv .venv
.venv\Scripts\python -m pip install -e .
copy .env.example .env
.venv\Scripts\python -m uvicorn app.main:app --reload
```

- App: `http://127.0.0.1:8000/`
- Login: `http://127.0.0.1:8000/login`
- API docs: `http://127.0.0.1:8000/docs`

## Default Admin
- Email: `admin@crecrm.app`
- Password: `admin123`

Set your own secure values via `.env`:
- `DEFAULT_ADMIN_EMAIL`
- `DEFAULT_ADMIN_PASSWORD`
- `SECRET_KEY`

## Main API
- `POST /api/v1/auth/login`
- `POST /api/v1/auth/logout`
- `GET /api/v1/auth/me`
- `GET/POST /api/v1/users` (admin create)
- `PATCH /api/v1/users/{id}/active` (admin)
- `GET /api/v1/dashboard/summary`
- `GET/POST /api/v1/leads`
- `PATCH /api/v1/leads/{id}/status`
- `GET/POST /api/v1/deals`
- `PATCH /api/v1/deals/{id}/status`
- `POST /api/v1/parser/ingest`
- `GET /api/v1/parser/results`
- `POST /api/v1/parser/results/{id}/to-lead`
- `POST /api/v1/parser/results/{id}/to-deal`

