"""Pruebas unitarias de EmailService con SMTP simulado (sin red)."""
import pytest

from app.services.email_service import EmailService

pytestmark = pytest.mark.unit


class _FakeSMTP:
    last = None

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sent_to = None
        _FakeSMTP.last = self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def ehlo(self, name=None):
        pass

    def starttls(self):
        pass

    def login(self, user, password):
        pass

    def send_message(self, msg, to_addrs=None):
        self.sent_to = to_addrs


def test_normalize_addresses():
    assert EmailService._normalize_addresses(None) == []
    assert EmailService._normalize_addresses("a@x.co, b@x.co") == ["a@x.co", "b@x.co"]
    assert EmailService._normalize_addresses([" a@x.co ", "", "b@x.co"]) == ["a@x.co", "b@x.co"]


def test_send_sin_destinatarios():
    svc = EmailService()
    assert svc.send_assignment_report(recipient=[], subject="s", body="b") is False


def test_send_ok(monkeypatch):
    monkeypatch.setattr("app.services.email_service.smtplib.SMTP", _FakeSMTP)
    svc = EmailService()
    ok = svc.send_assignment_report(
        recipient="a@x.co", subject="Asunto", body="<p>hola</p>", cc="c@x.co"
    )
    assert ok is True
    assert "a@x.co" in _FakeSMTP.last.sent_to


def test_send_con_adjunto(monkeypatch, tmp_path):
    monkeypatch.setattr("app.services.email_service.smtplib.SMTP", _FakeSMTP)
    f = tmp_path / "base.xlsx"
    f.write_text("data")
    missing = str(tmp_path / "no.xlsx")
    svc = EmailService()
    ok = svc.send_assignment_report(
        recipient=["a@x.co"], subject="s", body="<p>b</p>",
        attachments=[str(f), missing],  # uno existe, otro no
    )
    assert ok is True


def test_send_multiple_reports(monkeypatch, tmp_path):
    monkeypatch.setattr("app.services.email_service.smtplib.SMTP", _FakeSMTP)
    svc = EmailService()
    ok = svc.send_multiple_reports(
        recipient="a@x.co", serlefin_file=None, cobyser_file=None,
        metrics_html="<p>m</p>",
    )
    assert ok is True
