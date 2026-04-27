"""Tests for the optional admin-password gate on the jobs UI.

Driver/fastapi stubs are installed by tests/jobs/conftest.py.
"""

from __future__ import annotations

import pytest

from social_data_pipeline.jobs import auth as auth_mod


# ── admin_password (composite of three env vars) ─────────────────────────────


class TestAdminPassword:
    def test_picks_first_nonempty(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_PASSWORD", "pg_pw")
        monkeypatch.setenv("STARROCKS_ROOT_PASSWORD", "sr_pw")
        # PG comes first in _PASSWORD_ENV_ORDER, so it wins.
        assert auth_mod.admin_password() == "pg_pw"

    def test_falls_through_empty_to_next(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_PASSWORD", "")
        monkeypatch.setenv("STARROCKS_ROOT_PASSWORD", "sr_pw")
        monkeypatch.delenv("MONGO_ADMIN_PASSWORD", raising=False)
        assert auth_mod.admin_password() == "sr_pw"

    def test_all_unset_returns_none(self, monkeypatch):
        for var in auth_mod._PASSWORD_ENV_ORDER:
            monkeypatch.delenv(var, raising=False)
        assert auth_mod.admin_password() is None


# ── validate_startup ─────────────────────────────────────────────────────────


class TestValidateStartup:
    def test_passes_when_auth_disabled(self, monkeypatch):
        for var in auth_mod._PASSWORD_ENV_ORDER:
            monkeypatch.delenv(var, raising=False)
        # auth_required=False with no password — no error.
        auth_mod.validate_startup(auth_required=False)

    def test_passes_when_auth_required_and_password_set(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_PASSWORD", "pw")
        auth_mod.validate_startup(auth_required=True)

    def test_raises_when_auth_required_no_password(self, monkeypatch):
        for var in auth_mod._PASSWORD_ENV_ORDER:
            monkeypatch.delenv(var, raising=False)
        with pytest.raises(RuntimeError, match="auth: true"):
            auth_mod.validate_startup(auth_required=True)


# ── check_password ───────────────────────────────────────────────────────────


class TestCheckPassword:
    def test_correct_password(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_PASSWORD", "s3cr3t")
        assert auth_mod.check_password("s3cr3t") is True

    def test_wrong_password(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_PASSWORD", "s3cr3t")
        assert auth_mod.check_password("guess") is False

    def test_no_password_set(self, monkeypatch):
        for var in auth_mod._PASSWORD_ENV_ORDER:
            monkeypatch.delenv(var, raising=False)
        assert auth_mod.check_password("anything") is False

    def test_empty_submitted(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_PASSWORD", "s3cr3t")
        assert auth_mod.check_password("") is False


# ── token issue / verify (HMAC + TTL) ────────────────────────────────────────


class TestTokens:
    def test_issue_then_verify_round_trip(self):
        tok = auth_mod.issue_token(now=1_000_000)
        assert auth_mod.verify_token(tok, now=1_000_010) is True

    def test_verify_rejects_garbage(self):
        assert auth_mod.verify_token("not.a.real.token") is False
        assert auth_mod.verify_token("nodot") is False

    def test_verify_rejects_tampered_payload(self):
        tok = auth_mod.issue_token(now=1_000_000)
        payload, mac = tok.rsplit(".", 1)
        # Modify the payload but keep the same MAC — the HMAC check must fail.
        tampered = f"{int(payload) + 1}.{mac}"
        assert auth_mod.verify_token(tampered) is False

    def test_verify_rejects_tampered_mac(self):
        tok = auth_mod.issue_token(now=1_000_000)
        payload, mac = tok.rsplit(".", 1)
        bad_mac = "0" * len(mac)
        assert auth_mod.verify_token(f"{payload}.{bad_mac}") is False

    def test_verify_rejects_expired(self):
        tok = auth_mod.issue_token(now=1_000_000)
        future = 1_000_000 + auth_mod.SESSION_TTL_SECONDS + 1
        assert auth_mod.verify_token(tok, now=future) is False

    def test_verify_rejects_negative_age(self):
        # Token claims a future timestamp — verifier rejects.
        tok = auth_mod.issue_token(now=2_000_000)
        assert auth_mod.verify_token(tok, now=1_000_000) is False

    def test_verify_rejects_non_integer_payload(self):
        # Hand-craft a token with a non-integer payload but a valid MAC.
        import hashlib
        import hmac
        payload = "not-a-timestamp"
        mac = hmac.new(auth_mod._SIGNING_KEY, payload.encode(), hashlib.sha256).hexdigest()
        assert auth_mod.verify_token(f"{payload}.{mac}") is False


# ── require_auth_dep factory ─────────────────────────────────────────────────


class TestRequireAuthDep:
    def test_disabled_dep_is_noop(self):
        dep = auth_mod.require_auth_dep(auth_required=False)
        # Awaiting the noop dependency must not raise.
        import asyncio
        result = asyncio.run(dep())
        assert result is None
