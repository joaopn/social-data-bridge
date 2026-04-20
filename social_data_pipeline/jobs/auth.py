"""Optional password gate for the web UI.

Design choices:
- The admin password lives only in the container's process env, populated
  by `sdp db start`. It is never written to disk by the scheduler.
- Sessions are represented by an HMAC-signed cookie. The signing key is
  generated at module load (`secrets.token_urlsafe(32)`) and lives only in
  process memory, so restarting the container invalidates every session.
- Only the web UI routes are gated. The MCP endpoint at ``/mcp`` stays
  open, matching the other SDP MCP servers.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
import time
from typing import Optional

from fastapi import HTTPException, Request
from fastapi.responses import RedirectResponse


log = logging.getLogger(__name__)


COOKIE_NAME = "sdp_jobs_auth"
SESSION_TTL_SECONDS = 30 * 86400  # 30 days
_PASSWORD_ENV_ORDER = (
    "POSTGRES_PASSWORD",
    "STARROCKS_ROOT_PASSWORD",
    "MONGO_ADMIN_PASSWORD",
)


# Ephemeral signing key — regenerated on every process start.
_SIGNING_KEY = secrets.token_urlsafe(32).encode()


def admin_password() -> Optional[str]:
    """First non-empty admin password from the container environment."""
    for var in _PASSWORD_ENV_ORDER:
        v = os.environ.get(var, "")
        if v:
            return v
    return None


def validate_startup(auth_required: bool) -> None:
    """Raise at process start if auth is configured but no password is
    available, so misconfiguration fails loudly instead of silently
    leaving the UI open or always-rejecting logins."""
    if auth_required and admin_password() is None:
        raise RuntimeError(
            "jobs config has `auth: true` but no DB admin password is set "
            "in the container environment (POSTGRES_PASSWORD / "
            "STARROCKS_ROOT_PASSWORD / MONGO_ADMIN_PASSWORD). Either enable "
            "DB auth via `sdp db setup`, or disable web UI auth via "
            "`sdp db setup-jobs`."
        )


def check_password(submitted: str) -> bool:
    pw = admin_password()
    if not pw or not submitted:
        return False
    return hmac.compare_digest(submitted.encode(), pw.encode())


def _sign(payload: str) -> str:
    mac = hmac.new(_SIGNING_KEY, payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{mac}"


def issue_token(now: Optional[float] = None) -> str:
    ts = int(now if now is not None else time.time())
    return _sign(str(ts))


def verify_token(token: str, now: Optional[float] = None) -> bool:
    try:
        payload, mac = token.rsplit(".", 1)
    except ValueError:
        return False
    expected = hmac.new(_SIGNING_KEY, payload.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(mac, expected):
        return False
    try:
        ts = int(payload)
    except ValueError:
        return False
    age = int(now if now is not None else time.time()) - ts
    if age < 0 or age > SESSION_TTL_SECONDS:
        return False
    return True


def is_authenticated(request: Request) -> bool:
    tok = request.cookies.get(COOKIE_NAME)
    return bool(tok and verify_token(tok))


def require_auth_dep(auth_required: bool):
    """Factory producing a FastAPI dependency for UI routes.

    When ``auth_required`` is False, the dependency is a no-op (keeps
    today's open-UI behaviour). When True, unauthenticated requests get
    a 302 redirect to ``/login``.
    """

    if not auth_required:
        async def noop() -> None:
            return None
        return noop

    async def enforce(request: Request) -> None:
        if is_authenticated(request):
            return None
        raise HTTPException(
            status_code=302,
            headers={"Location": "/login"},
            detail="authentication required",
        )

    return enforce


def set_auth_cookie(response: RedirectResponse) -> None:
    response.set_cookie(
        key=COOKIE_NAME,
        value=issue_token(),
        max_age=SESSION_TTL_SECONDS,
        httponly=True,
        samesite="strict",
        secure=False,  # HTTP-over-Tailscale friendly; set True behind HTTPS
        path="/",
    )


def clear_auth_cookie(response: RedirectResponse) -> None:
    response.delete_cookie(key=COOKIE_NAME, path="/")
