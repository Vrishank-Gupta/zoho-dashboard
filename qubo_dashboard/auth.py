from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import hmac
import json
import secrets
import smtplib
from email.message import EmailMessage

from fastapi import HTTPException, Request, Response

from .config import settings


SESSION_COOKIE = "qubo_dashboard_session"
_OTP_STORE: dict[str, "OtpRecord"] = {}


@dataclass(slots=True)
class OtpRecord:
    otp_hash: str
    expires_at: datetime
    attempts: int = 0


def normalize_email(email: str) -> str:
    return str(email or "").strip().lower()


def validate_allowed_email(email: str) -> str:
    normalized = normalize_email(email)
    if not normalized or "@" not in normalized:
        raise HTTPException(status_code=400, detail="Enter a valid Hero Electronix email address.")
    if not normalized.endswith(settings.auth_allowed_domain):
        raise HTTPException(status_code=403, detail=f"Email must end with {settings.auth_allowed_domain}.")
    return normalized


def request_otp(email: str) -> dict:
    normalized = validate_allowed_email(email)
    otp = f"{secrets.randbelow(1_000_000):06d}"
    _OTP_STORE[normalized] = OtpRecord(
        otp_hash=_hash_otp(normalized, otp),
        expires_at=_now() + timedelta(minutes=settings.auth_otp_minutes),
    )
    _send_otp_email(normalized, otp)
    return {"status": "ok", "expires_in_minutes": settings.auth_otp_minutes}


def verify_otp(email: str, otp: str, request: Request, response: Response) -> dict:
    normalized = validate_allowed_email(email)
    code = str(otp or "").strip()
    record = _OTP_STORE.get(normalized)
    if not record or record.expires_at <= _now():
        _OTP_STORE.pop(normalized, None)
        raise HTTPException(status_code=400, detail="OTP expired. Please request a new code.")
    if record.attempts >= 5:
        _OTP_STORE.pop(normalized, None)
        raise HTTPException(status_code=429, detail="Too many attempts. Please request a new code.")
    record.attempts += 1
    if not hmac.compare_digest(record.otp_hash, _hash_otp(normalized, code)):
        raise HTTPException(status_code=400, detail="Invalid OTP.")

    _OTP_STORE.pop(normalized, None)
    expires_at = _now() + timedelta(days=settings.auth_session_days)
    response.set_cookie(
        SESSION_COOKIE,
        _sign_session({"email": normalized, "exp": int(expires_at.timestamp())}),
        max_age=settings.auth_session_days * 24 * 60 * 60,
        httponly=True,
        secure=_is_secure_request(request),
        samesite="lax",
        path="/",
    )
    return {"status": "ok", "email": normalized, "expires_at": expires_at.isoformat()}


def logout(response: Response) -> dict:
    response.delete_cookie(SESSION_COOKIE, path="/", samesite="lax")
    return {"status": "ok"}


def current_user(request: Request) -> dict | None:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    payload = _unsign_session(token)
    if not payload:
        return None
    if int(payload.get("exp") or 0) <= int(_now().timestamp()):
        return None
    email = normalize_email(str(payload.get("email") or ""))
    if not email.endswith(settings.auth_allowed_domain):
        return None
    return {"email": email}


def require_user(request: Request) -> dict:
    user = current_user(request)
    if user:
        return user
    raise HTTPException(status_code=401, detail="Login required.")


def auth_configured() -> bool:
    return bool(
        settings.auth_session_secret
        and settings.smtp_host
        and settings.smtp_user
        and settings.smtp_password
        and settings.smtp_sender
    )


def _hash_otp(email: str, otp: str) -> str:
    return hmac.new(_secret_bytes(), f"{email}:{otp}".encode("utf-8"), hashlib.sha256).hexdigest()


def _sign_session(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    body = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    signature = hmac.new(_secret_bytes(), body.encode("ascii"), hashlib.sha256).hexdigest()
    return f"{body}.{signature}"


def _unsign_session(token: str) -> dict | None:
    try:
        body, signature = token.rsplit(".", 1)
    except ValueError:
        return None
    expected = hmac.new(_secret_bytes(), body.encode("ascii"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        padded = body + "=" * (-len(body) % 4)
        return json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))
    except (ValueError, json.JSONDecodeError):
        return None


def _send_otp_email(email: str, otp: str) -> None:
    if not auth_configured():
        raise HTTPException(status_code=500, detail="Email login is not configured.")
    message = EmailMessage()
    message["Subject"] = "Your Qubo Support dashboard OTP"
    message["From"] = settings.smtp_sender or ""
    message["To"] = email
    message.set_content(
        f"Your Qubo Support dashboard OTP is {otp}.\n\n"
        f"This code is valid for {settings.auth_otp_minutes} minutes."
    )
    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=20) as smtp:
            smtp.starttls()
            smtp.login(settings.smtp_user, settings.smtp_password)
            smtp.send_message(message)
    except smtplib.SMTPException as exc:
        raise HTTPException(status_code=502, detail="Could not send OTP email.") from exc


def _secret_bytes() -> bytes:
    if not settings.auth_session_secret:
        raise HTTPException(status_code=500, detail="Login session secret is not configured.")
    return settings.auth_session_secret.encode("utf-8")


def _is_secure_request(request: Request) -> bool:
    return (
        request.url.scheme == "https"
        or request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip().lower() == "https"
    )


def _now() -> datetime:
    return datetime.now(UTC)
