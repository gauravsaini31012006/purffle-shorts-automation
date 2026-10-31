from datetime import datetime, timedelta, timezone

from purffle_shorts.config import Settings
from purffle_shorts.history import History


def test_youtube_uploads_last_24h_counts_only_recent_youtube_uploads(tmp_path):
    history = History(tmp_path / "history.db")
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(hours=2)).isoformat(timespec="seconds")
    old = (now - timedelta(hours=26)).isoformat(timespec="seconds")

    history.add_video(title="Recent YouTube", status="uploaded", youtube_id="yt1", uploaded_at=recent)
    history.add_video(title="Old YouTube", status="uploaded", youtube_id="yt2", uploaded_at=old)
    history.add_video(title="Crosspost only", status="uploaded", uploaded_at=recent)

    cutoff = (now - timedelta(hours=24)).isoformat(timespec="seconds")
    assert history.youtube_uploads_last_24h(cutoff) == 1


def test_rolling_upload_cap_defaults_to_two(monkeypatch):
    monkeypatch.delenv("MAX_PUBLIC_UPLOADS_24H", raising=False)
    assert Settings.from_env().max_uploads_24h == 2


def test_rolling_upload_cap_can_be_configured(monkeypatch):
    monkeypatch.setenv("MAX_PUBLIC_UPLOADS_24H", "1")
    assert Settings.from_env().max_uploads_24h == 1

def test_rolling_upload_cap_cannot_be_raised_above_two(monkeypatch):
    monkeypatch.setenv("MAX_PUBLIC_UPLOADS_24H", "99")
    assert Settings.from_env().max_uploads_24h == 2


def test_runtime_upload_gate_hard_caps_public_uploads_at_two(tmp_path, monkeypatch):
    import json
    from purffle_shorts.pipeline import Studio

    settings = Settings(upload=True, max_uploads_24h=99, data_dir=str(tmp_path / "data"),
                        output_dir=str(tmp_path / "output"))
    studio = Studio(settings)
    folder = tmp_path / "output" / "ready"
    folder.mkdir(parents=True)
    video = folder / "short.mp4"
    video.write_bytes(b"not a real video; upload gate exits before reading it")
    (folder / "metadata.json").write_text(json.dumps({}), encoding="utf-8")
    video_id = studio.history.add_video(
        title="Safety gate test", status="rendered", folder=str(folder),
        video_path=str(video), tags="[]", youtube_privacy="public",
    )
    monkeypatch.setattr(studio.history, "youtube_uploads_last_24h", lambda _since: 2)

    assert studio.upload_record(video_id) == "queued"
    assert studio.history.get(video_id)["status"] == "queued"
