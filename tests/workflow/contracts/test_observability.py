from __future__ import annotations

from piko.workflow.observability import safe_error_message, safe_log_fields, short_lock_token


def test_observability_redacts_credentials_and_full_tokens():
    token = "a" * 64
    assert short_lock_token(token) != token
    assert len(short_lock_token(token)) < len(token)
    fields = safe_log_fields({"password": "secret", "lock_token": token, "stage": "stage-a"})
    assert fields["password"] == "[REDACTED]"
    assert fields["lock_token"] != token
    assert "secret" not in safe_error_message("password=secret dsn=mysql://user:secret@host/db")


def test_error_logging_contract_keeps_locator_without_business_payload():
    message = safe_error_message("stage=alpha task_id=t-1 error_code=E42 payload=very-large")
    assert "stage=alpha" in message
    assert "error_code=E42" in message
