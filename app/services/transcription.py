import tempfile
from pathlib import Path

import requests
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.call_record import CallRecord
from app.models.enums import TranscriptStatus

settings = get_settings()


def _extract_keywords(text: str) -> list[str]:
    words = [word.strip(".,:;!?()[]{}\"'").lower() for word in text.split()]
    tokens = [word for word in words if len(word) >= 5]
    counts: dict[str, int] = {}
    for token in tokens:
        counts[token] = counts.get(token, 0) + 1
    ordered = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    return [token for token, _ in ordered[:10]]


def _make_summary(text: str) -> str:
    cleaned = " ".join(text.split())
    if len(cleaned) <= 260:
        return cleaned
    return f"{cleaned[:257]}..."


def _resolve_recording_path(call: CallRecord) -> tuple[Path, bool]:
    if call.recording_local_path:
        file_path = Path(call.recording_local_path)
        if file_path.exists():
            return file_path, False
    if not call.recording_url:
        raise ValueError("Call has no recording URL or local path.")

    if call.recording_url.startswith("/media/"):
        local_path = Path(settings.media_dir) / call.recording_url.replace("/media/", "", 1)
        if local_path.exists():
            return local_path, False
        raise ValueError(f"Recording file not found: {local_path}")

    if call.recording_url.startswith("http://") or call.recording_url.startswith("https://"):
        response = requests.get(call.recording_url, timeout=60)
        response.raise_for_status()
        suffix = Path(call.recording_url).suffix or ".mp3"
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        temp_file.write(response.content)
        temp_file.flush()
        temp_file.close()
        return Path(temp_file.name), True

    raise ValueError(f"Unsupported recording URL format: {call.recording_url}")


def transcribe_call(db: Session, call: CallRecord) -> CallRecord:
    if not settings.openai_api_key:
        call.transcript_status = TranscriptStatus.failed
        call.transcript_error = "OPENAI_API_KEY is not configured."
        db.flush()
        return call

    call.transcript_status = TranscriptStatus.processing
    call.transcript_error = None
    db.flush()

    path, should_delete = _resolve_recording_path(call)
    try:
        with path.open("rb") as file_obj:
            response = requests.post(
                f"{settings.openai_base_url.rstrip('/')}/audio/transcriptions",
                headers={"Authorization": f"Bearer {settings.openai_api_key}"},
                data={"model": settings.transcription_model, "response_format": "json"},
                files={"file": (path.name, file_obj, "application/octet-stream")},
                timeout=180,
            )
        if response.status_code >= 400:
            raise ValueError(f"Transcription API failed: {response.status_code} {response.text}")
        payload = response.json()
        text = (payload.get("text") or "").strip()
        if not text:
            raise ValueError("Transcription API returned empty text.")

        call.transcript_text = text
        call.summary_text = _make_summary(text)
        call.extracted_entities = {"keywords": _extract_keywords(text)}
        call.transcript_status = TranscriptStatus.completed
        call.transcript_error = None
    except Exception as exc:
        call.transcript_status = TranscriptStatus.failed
        call.transcript_error = str(exc)
    finally:
        if should_delete and path.exists():
            path.unlink(missing_ok=True)
    db.flush()
    return call

