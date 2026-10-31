"""The Studio web app's server: security checks, the JSON API and a real (offline) job."""

import http.client
import json
import logging
import threading
import time
from http.server import ThreadingHTTPServer

import pytest

from purffle_shorts import studio as ST


@pytest.fixture
def server(settings):
    app = ST.StudioServer(settings.with_overrides(upload=False))
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), ST._handler(app, "127.0.0.1"))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield app, httpd.server_address[1]
    httpd.shutdown()
    httpd.server_close()
    root = logging.getLogger()
    for h in [h for h in root.handlers if isinstance(h, ST._JobLog)]:
        root.removeHandler(h)


def req(port, method, path, body=None, token=None, host=None):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Studio-Token"] = token
    if host:
        headers["Host"] = host
    c.request(method, path, body=json.dumps(body) if body is not None else None, headers=headers)
    r = c.getresponse()
    data = r.read()
    c.close()
    return r.status, dict(r.getheaders()), data


def test_page_has_token_and_strict_csp(server):
    app, port = server
    status, headers, body = req(port, "GET", "/")
    assert status == 200 and app.token.encode() in body and b"__TOKEN__" not in body
    assert "default-src 'self'" in headers["Content-Security-Policy"]
    assert "unsafe-inline" not in headers["Content-Security-Policy"]
    assert req(port, "GET", "/static/app.js")[0] == 200
    assert req(port, "GET", "/static/../studio.py")[0] == 404


def test_other_host_names_are_refused(server):
    app, port = server
    assert req(port, "GET", "/api/info", host="evil.example:8765")[0] == 421
    assert req(port, "GET", "/api/info", host=f"localhost:{port}")[0] == 200


def test_writes_need_the_token(server):
    app, port = server
    assert req(port, "POST", "/api/ideas", {"subject": "x"})[0] == 403
    status, _, body = req(port, "POST", "/api/ideas", {"subject": "Why cats purr", "style": "facts"}, app.token)
    assert status == 200
    iid = json.loads(body)["id"]
    ideas = json.loads(req(port, "GET", "/api/ideas")[2])["pending"]
    assert [(i["id"], i["style"]) for i in ideas] == [(iid, "facts")]
    req(port, "POST", f"/api/ideas/{iid}/delete", {}, app.token)
    assert json.loads(req(port, "GET", "/api/ideas")[2])["pending"] == []


def test_options_and_doctor(server):
    app, port = server
    opts = json.loads(req(port, "GET", "/api/options")[2])
    assert {"chat", "dialogue"} <= set(opts["styles"]) and "16:9" in opts["aspects"]
    checks = json.loads(req(port, "GET", "/api/doctor")[2])
    assert any(c["label"] == "Python" for c in checks)


def test_form_values_are_validated(server):
    app, _ = server
    s = app.overrides({"caption_style": "<script>", "aspect": "16:9", "duration": "999", "review": "rewrite",
                       "grade": "cinematic", "visuals": ["pollinations"], "music": False, "tts": "rm -rf"})
    assert s.caption_style == app.settings.caption_style and s.resolution == (1920, 1080)
    assert s.target_seconds == 170 and s.color_grade == "cinematic" and s.visual_sources == ["pollinations"]
    assert s.music_volume == 0.0 and s.tts_engine == app.settings.tts_engine


def test_offline_draft_job_runs_to_completion(server):
    app, port = server
    status, _, body = req(port, "POST", "/api/draft", {"offline": True, "style": "chat"}, app.token)
    jid = json.loads(body)["job"]
    for _ in range(100):
        job = json.loads(req(port, "GET", f"/api/jobs/{jid}")[2])
        if job["status"] in ("done", "failed"):
            break
        time.sleep(0.05)
    assert job["status"] == "done", job
    assert job["result"]["script"]["style"] == "chat" and job["pct"] == 100
    assert job["label"] == "Demo video"


def _job(port, jid, until=("done", "failed", "cancelled"), tries=600):
    for _ in range(tries):
        job = json.loads(req(port, "GET", f"/api/jobs/{jid}")[2])
        if job["status"] in until:
            return job
        time.sleep(0.05)
    return job


def test_event_stream_pushes_job_changes(server):
    app, port = server
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    c.request("GET", "/api/events")
    r = c.getresponse()
    assert r.status == 200 and r.getheader("Content-Type").startswith("text/event-stream")
    jid = json.loads(req(port, "POST", "/api/draft", {"offline": True}, app.token)[2])["job"]
    seen = set()
    deadline = time.time() + 15
    while time.time() < deadline and "done" not in seen:
        line = r.fp.readline().decode()
        if line.startswith("data: "):
            jobs = json.loads(line[6:])
            seen |= {j["status"] for j in jobs if j["id"] == jid}
    c.close()
    assert "done" in seen  # the finished job arrived on the stream, without polling


def test_a_running_job_can_be_cancelled(server, monkeypatch):
    from purffle_shorts import pipeline as P
    from purffle_shorts.utils import CANCEL
    app, port = server
    started = threading.Event()
    real_step = P.Studio._step

    def slow_step(self, stage, message=""):
        if stage == "voice":
            started.set()
            for _ in range(200):  # stand-in for a long stage that checks for cancellation
                if CANCEL.is_set():
                    break
                time.sleep(0.02)
        return real_step(self, stage, message)
    monkeypatch.setattr(P.Studio, "_step", slow_step)
    jid = json.loads(req(port, "POST", "/api/make", {"offline": True}, app.token)[2])["job"]
    assert started.wait(10)
    assert json.loads(req(port, "POST", f"/api/jobs/{jid}/cancel", {}, app.token)[2])["ok"]
    job = _job(port, jid)
    assert job["status"] == "cancelled", job
    assert app.studio.history.recent(5) == []  # nothing half-made was left in the library
    assert not CANCEL.is_set()                 # the next job starts clean


def test_password_protects_everything(settings):
    app = ST.StudioServer(settings.with_overrides(upload=False))
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), ST._handler(app, "127.0.0.1", password="s3cret"))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    port = httpd.server_address[1]
    try:
        status, headers, _ = req(port, "GET", "/api/info")
        assert status == 401 and "Basic" in headers["WWW-Authenticate"]
        import base64
        good = "Basic " + base64.b64encode(b"me:s3cret").decode()
        bad = "Basic " + base64.b64encode(b"me:nope").decode()
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        c.request("GET", "/api/info", headers={"Authorization": bad})
        assert c.getresponse().status == 401
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        c.request("GET", "/api/info", headers={"Authorization": good})
        assert c.getresponse().status == 200
    finally:
        httpd.shutdown()
        httpd.server_close()
        root = logging.getLogger()
        for h in [h for h in root.handlers if isinstance(h, ST._JobLog)]:
            root.removeHandler(h)


def test_running_jobs_report_an_eta(server):
    app, port = server
    app.studio.clock.avg.update({"voice": 2.0, "render": 4.0})  # as if the machine had made videos before
    st = app.studio
    st._begin(["voice", "footage", "captions", "render", "check", "done"])
    st._step("voice")
    eta = st.eta()
    assert eta is not None and eta > 4.0
    st._t_start -= 10  # ten seconds into the job
    st._step("render")
    st._sub(0.5)
    assert st.eta() == 3.0  # the second half of render (2 s) + the default 1 s for the check
    assert 0 < st._pct < 100


def test_bundled_fonts_and_brand_assets_are_served(server):
    app, port = server
    status, headers, body = req(port, "GET", "/static/fonts/SchibstedGrotesk.woff2")
    assert status == 200 and headers["Content-Type"] == "font/woff2" and body[:4] == b"wOF2"
    assert "max-age" in headers["Cache-Control"]
    assert req(port, "GET", "/static/fonts/JetBrainsMono.woff2")[0] == 200
    assert req(port, "GET", "/static/fonts/missing.woff2")[0] == 404
    assert req(port, "GET", "/static/fonts/../app.js")[0] == 404
    status, headers, body = req(port, "GET", "/static/icon.svg")
    assert status == 200 and b"<svg" in body


def test_page_has_no_inline_styles_or_scripts(server):
    """The Content-Security-Policy forbids inline styles and scripts, so the markup must not use them."""
    import re
    app, port = server
    html = req(port, "GET", "/")[2].decode()
    js = req(port, "GET", "/static/app.js")[2].decode()
    assert not re.search(r"\sstyle=", html) and not re.search(r"<script>(?!\s*</script>)", html)
    assert not re.search(r"""\sstyle=["']""", js)


def test_upload_api_respects_upload_false(server):
    app, port = server
    status, _, body = req(port, "POST", "/api/upload/1", {}, app.token)
    assert status == 403
    assert b"UPLOAD=false" in body


def test_automation_settings_are_persistent_and_validated(server):
    app, port = server
    initial = json.loads(req(port, "GET", "/api/automation")[2])
    assert initial["enabled"] is False
    assert initial["mode"] == "review_required"
    assert initial["auto_approve_ideas"] is False
    assert initial["auto_produce"] is False
    assert initial["auto_publish"] is False
    status, _, body = req(port, "POST", "/api/automation",
                          {"enabled": True, "plan_time": "23:59", "timezone": "Asia/Kolkata",
                           "ideas_per_run": 12}, app.token)
    assert status == 200, body
    saved = json.loads(req(port, "GET", "/api/automation")[2])
    assert saved["enabled"] is True and saved["plan_time"] == "23:59"
    assert saved["timezone"] == "Asia/Kolkata" and saved["ideas_per_run"] == 12
    status, _, body = req(port, "POST", "/api/automation", {"enabled": "false"}, app.token)
    assert status == 200 and json.loads(req(port, "GET", "/api/automation")[2])["enabled"] is False
    status, _, body = req(port, "POST", "/api/automation", {"plan_time": "25:99"}, app.token)
    assert status == 400 and b"HH:MM" in body
    status, _, body = req(port, "POST", "/api/automation", {"auto_publish": True}, app.token)
    assert status == 400 and b"UPLOAD=false" in body


def test_rolling_upload_limit_counts_public_and_unknown_visibility(server):
    app, _ = server
    h = app.studio.history
    from datetime import datetime, timezone, timedelta
    recent = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(timespec="seconds")
    h.add_video(title="Public", status="uploaded", youtube_id="pub1", uploaded_at=recent,
                youtube_privacy="public")
    h.add_video(title="Private", status="uploaded", youtube_id="priv1", uploaded_at=recent,
                youtube_privacy="private")
    h.add_video(title="Legacy", status="uploaded", youtube_id="legacy1", uploaded_at=recent)
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(timespec="seconds")
    assert h.youtube_uploads_last_24h(since) == 2


def test_profile_api_persists_and_applies_settings(server):
    app, port = server
    assert req(port, "GET", "/api/profiles")[0] == 200
    payload = {"name": "Space Lab", "niche": "space science", "style": "explainer",
               "language": "en", "duration": 55, "aspect": "9:16",
               "visual_style": "cinematic documentary", "provider": "gemini",
               "model": "gemini-2.5-flash", "channel_name": "Space Lab TV",
               "audience": "curious science fans"}
    status, _, body = req(port, "POST", "/api/profiles/save", payload, app.token)
    assert status == 200, body
    assert app.settings.niches == ["space science"]
    assert app.settings.target_seconds == 55
    data = json.loads(req(port, "GET", "/api/profiles")[2])
    assert data["active"] == "Space Lab"
    assert data["profiles"]["Space Lab"]["visual_style"] == "cinematic documentary"
    assert data["profiles"]["Space Lab"]["provider"] == "gemini"
    assert app.settings.llm_model == "gemini-2.5-flash"
    assert app.settings.channel_name == "Space Lab TV"
    assert app.studio.s.llm_provider == "gemini"
    assert app.studio.s.llm_model == "gemini-2.5-flash"
    assert "profiles" in data["profiles"]["Space Lab"]["token_file"]
    assert app.settings.token_file == data["profiles"]["Space Lab"]["token_file"]
    status, _, _ = req(port, "POST", "/api/profiles/apply", {"name": "missing"}, app.token)
    assert status == 400
    status, _, _ = req(port, "POST", "/api/profiles/delete", {"name": "Space Lab"}, app.token)
    assert status == 200
    assert "Space Lab" not in json.loads(req(port, "GET", "/api/profiles")[2])["profiles"]


def test_setup_status_never_exposes_secret_values(server):
    app, port = server
    status, _, body = req(port, "GET", "/api/setup")
    assert status == 200
    data = json.loads(body)
    assert "llm_api_key" not in data
    assert "openai_api_key" not in data
    assert "telegram_bot_token" not in data and "telegram_chat_id" not in data
    assert "youtube_connected" in data


def test_setup_saves_credentials_without_returning_secret(server, tmp_path, monkeypatch):
    app, port = server
    monkeypatch.chdir(tmp_path)
    for key in ("LLM_API_KEY", "LLM_PROVIDER", "LLM_MODEL", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
        monkeypatch.setenv(key, "")
    secret = "test-secret-never-return-this"
    telegram_token = "telegram-token-never-return-this"
    status, _, body = req(port, "POST", "/api/setup",
                          {"llm_provider": "gemini", "llm_model": "gemini-2.5-flash",
                           "llm_api_key": secret, "telegram_bot_token": telegram_token,
                           "telegram_chat_id": "123456789"}, app.token)
    assert status == 200, body
    assert secret.encode() not in body and telegram_token.encode() not in body
    assert app.settings.llm_api_key == secret
    env_text = (tmp_path / ".env").read_text(encoding="utf-8")
    assert "LLM_API_KEY=" in env_text and "TELEGRAM_BOT_TOKEN=" in env_text
    state = json.loads(req(port, "GET", "/api/setup")[2])
    assert state["llm_key_set"] is True and state["telegram_configured"] is True

def test_ideas_require_approval_and_selected_make_uses_the_requested_id(server, monkeypatch):
    app, port = server
    status, _, body = req(port, "POST", "/api/ideas",
                          {"subject": "Why the sky turns red at sunset", "style": "facts"}, app.token)
    assert status == 200
    iid = json.loads(body)["id"]
    data = json.loads(req(port, "GET", "/api/ideas")[2])
    assert [i["id"] for i in data["review"]] == [iid]
    assert data["approved"] == []

    status, _, body = req(port, "POST", f"/api/ideas/{iid}/make", {}, app.token)
    assert status == 409 and b"approve" in body
    status, _, body = req(port, "POST", f"/api/ideas/{iid}/approve", {}, app.token)
    assert status == 200 and json.loads(body)["ok"] is True

    captured = []
    class Stub:
        id = 987
    monkeypatch.setattr(app, "submit", lambda kind, params, label="": (
        captured.append((kind, params, label)) or Stub()))
    status, _, body = req(port, "POST", f"/api/ideas/{iid}/make", {}, app.token)
    assert status == 200 and json.loads(body)["job"] == 987
    assert captured[0][0] == "make_idea"
    assert captured[0][1]["idea_id"] == iid
    assert captured[0][1]["upload"] is False


def test_history_claims_only_approved_ideas_and_can_target_an_id(server):
    h = server[0].studio.history
    first = h.add_idea("First approved idea", approved=True)
    second = h.add_idea("Second approved idea", approved=True)
    review = h.add_idea("Needs a human review", approved=False)
    assert h.claim_idea(second)["id"] == second
    assert h.claim_idea(second) is None
    assert h.claim_idea()["id"] == first
    assert h.claim_idea(review) is None


def test_setup_can_toggle_upload_without_echoing_secrets(server, tmp_path, monkeypatch):
    app, port = server
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("UPLOAD", "false")
    status, _, body = req(port, "POST", "/api/setup", {"upload_enabled": True}, app.token)
    assert status == 200, body
    assert app.settings.upload is True
    assert json.loads(req(port, "GET", "/api/setup")[2])["upload_enabled"] is True
    assert 'UPLOAD="true"' in (tmp_path / ".env").read_text(encoding="utf-8")
    status, _, body = req(port, "POST", "/api/setup", {"upload_enabled": False}, app.token)
    assert status == 200, body
    assert app.settings.upload is False
    assert json.loads(req(port, "GET", "/api/automation")[2])["auto_publish"] is False

def test_upload_requests_require_a_connected_youtube_account(server):
    app, port = server
    app.settings = app.settings.with_overrides(upload=True)
    app.studio.s = app.settings
    status, _, body = req(port, "POST", "/api/make",
                          {"topic": "test upload gate", "upload": True}, app.token)
    assert status == 403 and b"Connect a YouTube account" in body
    status, _, body = req(port, "POST", "/api/upload/1", {}, app.token)
    assert status == 403 and b"Connect a YouTube account" in body


def test_automation_publish_requires_upload_and_oauth(server):
    app, port = server
    status, _, body = req(port, "POST", "/api/automation", {"auto_publish": True}, app.token)
    assert status == 400 and b"UPLOAD=false" in body

    token = app.settings.data_path / "profiles" / "default" / "token.json"
    token.parent.mkdir(parents=True, exist_ok=True)
    token.write_text("test token", encoding="utf-8")
    app.settings = app.settings.with_overrides(upload=True, token_file=str(token))
    app.studio.s = app.settings
    status, _, body = req(port, "POST", "/api/automation",
                          {"auto_publish": True, "auto_produce": True, "auto_approve_ideas": True}, app.token)
    assert status == 200, body
    state = json.loads(req(port, "GET", "/api/automation")[2])
    assert state["auto_publish"] is True and state["auto_produce"] is True
    assert state["auto_approve_ideas"] is True

def test_flush_uploads_worker_job_reports_completed_count(server, monkeypatch):
    from purffle_shorts.pipeline import Studio as PipelineStudio
    app, port = server
    monkeypatch.setattr(PipelineStudio, "flush_queue", lambda self: 3)
    jid = app.submit("flush_uploads", {}, "Retry queued uploads").id
    job = _job(port, jid)
    assert job["status"] == "done", job
    assert job["result"]["uploaded"] == 3

def test_new_settings_default_to_local_render_only():
    from purffle_shorts.config import Settings
    assert Settings().upload is False
    assert Settings().max_uploads_24h == 2

