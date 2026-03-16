# CRE CRM for Commercial Real Estate

Production-oriented CRM baseline for a regional commercial real estate agency.

## Implemented Phases
- Phase 1.1: CRM core (`leads`, `deals`, `properties`, `parser results`).
- Phase 1.2: login-only authentication, RBAC, admin user management, audit log.
- Phase 1.3: dashboard and tabbed UI with kanban boards.
- Phase 2/3: call-center module (IP-telephony webhook, recording playback, transcription, call-to-lead).
- Parser automation update: fully automatic parser scheduler every 24 hours by default.

## Main Tabs
- `Home` - KPI and conversion dashboard.
- `Leads` - kanban board.
- `Deals` - kanban board.
- `Parser Hub` - auto sources, run history, parser results, manual ingest.
- `Call Center` - call history, recordings, transcription, conversion to lead.
- `Users` - admin-only user management.

## Runtime Stack
- Python 3.12+
- FastAPI
- SQLAlchemy
- SQLite (default for pilot)
- Vanilla JS + Jinja templates

## Quick Start

```bash
python -m venv .venv
.venv\Scripts\python -m pip install -e .[dev]
copy .env.example .env
.venv\Scripts\python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

- App: `http://127.0.0.1:8000/`
- Login page: `http://127.0.0.1:8000/login`
- API docs: `http://127.0.0.1:8000/docs`

## Default Admin
- Email: `admin@crecrm.app`
- Password: `admin123`

## Required Environment Variables

Core:
- `SECRET_KEY`
- `DEFAULT_ADMIN_EMAIL`
- `DEFAULT_ADMIN_PASSWORD`

Parser automation:
- `PARSER_SCHEDULER_ENABLED=true`
- `PARSER_POLL_INTERVAL_MINUTES=1440`
- `PARSER_REQUEST_TIMEOUT_SEC=25`
- `PARSER_MAX_ITEMS_PER_SOURCE=20`
- `PARSER_DETAIL_FETCH_LIMIT=10`

Telephony and transcription:
- `MEDIA_DIR=./media`
- `TELEPHONY_WEBHOOK_TOKEN=<strong-random-token>`
- `OPENAI_API_KEY=<your-openai-key>`
- `OPENAI_BASE_URL=https://api.openai.com/v1`
- `TRANSCRIPTION_MODEL=whisper-1`

## Parser Hub: Automatic Collection Every 10 Minutes

1. Configure sources in UI (`Parser Hub -> Auto Sources`) or API.
2. Keep sources active (`is_active=true`).
3. The background scheduler runs every `PARSER_POLL_INTERVAL_MINUTES` and:
   - fetches sources from `avito`, `cian`, `domclick`, and `telegram`,
   - extracts listings and contact signals,
   - deduplicates them via existing dedup pipeline,
   - writes run history with counts and errors.
4. You can force an immediate run with `Run parser now` button or API.

## API Endpoints

Auth and users:
- `POST /api/v1/auth/login`
- `POST /api/v1/auth/logout`
- `GET /api/v1/auth/me`
- `GET /api/v1/users`
- `POST /api/v1/users` (admin only)
- `PATCH /api/v1/users/{id}/active` (admin only)

Sales pipeline:
- `GET/POST /api/v1/leads`
- `PATCH /api/v1/leads/{id}/status`
- `GET/POST /api/v1/deals`
- `PATCH /api/v1/deals/{id}/status`

Parser hub:
- `GET /api/v1/parser/sources`
- `POST /api/v1/parser/sources`
- `PATCH /api/v1/parser/sources/{id}`
- `GET /api/v1/parser/runs`
- `POST /api/v1/parser/run-now`
- `POST /api/v1/parser/ingest`
- `GET /api/v1/parser/results`
- `POST /api/v1/parser/results/{id}/to-lead`
- `POST /api/v1/parser/results/{id}/to-deal`
- `POST /api/v1/parser/results/{id}/reject`

Call center:
- `GET /api/v1/calls`
- `POST /api/v1/calls/manual`
- `POST /api/v1/calls/{id}/upload-recording`
- `POST /api/v1/calls/{id}/transcribe`
- `POST /api/v1/calls/{id}/to-lead`
- `POST /api/v1/calls/webhook`

Dashboard:
- `GET /api/v1/dashboard/summary`

## IP-Telephony Webhook

Webhook endpoint:
- `POST /api/v1/calls/webhook`

Header:
- `X-Telephony-Token: <TELEPHONY_WEBHOOK_TOKEN>`

Payload example:

```json
{
  "agency_id": 1,
  "provider": "asterisk",
  "external_call_id": "ast-1001",
  "event": "call_ended",
  "direction": "inbound",
  "status": "completed",
  "from_number": "+79990001122",
  "to_number": "+73412223344",
  "started_at": "2026-03-16T10:30:00Z",
  "ended_at": "2026-03-16T10:32:10Z",
  "duration_sec": 130,
  "recording_url": "https://pbx.example/records/ast-1001.mp3"
}
```

## Go-Live Notes
- Put the app behind HTTPS reverse proxy (Nginx/Caddy).
- Rotate `SECRET_KEY` and `TELEPHONY_WEBHOOK_TOKEN`.
- Move from SQLite to PostgreSQL before scaling.
- Create call-center operator users with `call_center` role.
- Verify provider audio format (`mp3`, `wav`, `m4a`) for transcription quality.
