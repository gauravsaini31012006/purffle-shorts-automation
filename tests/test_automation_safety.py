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
