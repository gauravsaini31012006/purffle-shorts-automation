"""PurffleShorts Studio — a local web app (standard library only, no build step).

    purffle-shorts studio        -> http://127.0.0.1:8765

Create videos (or draft and edit the script first), watch every stage live, browse and play the library,
plan ideas, clip long videos, read channel stats and check the setup. It binds to localhost, refuses
requests addressed to any other host name (DNS rebinding), and every write needs a per-session token,
so other websites can't drive it from your browser.
"""

from __future__ import annotations

import base64
import hmac
import json
import logging
import mimetypes
import os
import queue
import re
import secrets
import threading
import time
import webbrowser
from dataclasses import dataclass, field, replace
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from . import __version__
from .config import ASPECTS, Settings, aspect_resolution

log = logging.getLogger("purffle")

WEB = Path(__file__).parent / "web"
FILES = ("short.mp4", "video.mp4", "cover.jpg", "captions.srt", "metadata.json", "script.json")
STATIC = {"app.js": "text/javascript; charset=utf-8", "app.css": "text/css; charset=utf-8",
          "icon.svg": "image/svg+xml"}
LOCAL_HOSTS = {"127.0.0.1", "localhost", "[::1]", "::1"}


@dataclass
class Job:
    id: int
    kind: str                       # make | draft | clip | plan | upload | stats
    params: dict
    label: str = ""
    status: str = "queued"          # queued | running | done | failed | cancelled
    stage: str = ""
    pct: int = 0
    message: str = ""
    log: list[str] = field(default_factory=list)
    result: dict = field(default_factory=dict)
    created: float = field(default_factory=time.time)
    started: float | None = None
    finished: float | None = None
    eta_fn: object = field(default=None, repr=False)   # the running Studio's eta(), while it runs

    def public(self, full_log: bool = False) -> dict:
        eta = None
        if self.status == "running" and callable(self.eta_fn):
            try:
                eta = self.eta_fn()
            except Exception:
                eta = None
        return {"id": self.id, "kind": self.kind, "label": self.label, "status": self.status, "stage": self.stage,
                "pct": self.pct, "message": self.message, "result": self.result, "created": self.created,
                "started": self.started, "finished": self.finished, "eta": eta,
                "log": self.log if full_log else self.log[-60:]}


class _JobLog(logging.Handler):
    """Copies log lines produced by the worker thread into the running job."""

    def __init__(self, studio: StudioServer):
        super().__init__(logging.INFO)
        self.studio = studio
        self.setFormatter(logging.Formatter("%(asctime)s %(message)s", "%H:%M:%S"))

    def emit(self, record: logging.LogRecord) -> None:
        job = self.studio.current
        if job is None:
            return
        # One job runs at a time, so the worker's own thread pools (voices, footage, segments) belong to it.
        if record.threadName == "studio-worker" or record.threadName.startswith("ThreadPoolExecutor"):
            job.log.append(self.format(record))
            del job.log[:-400]
            self.studio.changed()


def _choice(value, allowed, default=None):
    return value if value in allowed else default


class StudioServer:
    def __init__(self, settings: Settings):
        from .pipeline import Studio
        self.settings = settings
        self.studio = Studio(settings)
        # Restore the last selected creator profile after a restart.
        try:
            active_profile = self.studio.history.get_meta("studio_active_profile")
            if active_profile and active_profile != "Default":
                self.apply_profile(active_profile)
        except (KeyError, TypeError, ValueError):
            log.warning("Could not restore the active creator profile")
        # STUDIO_TOKEN fixes the write token, so scripts and tools like n8n can call the API (see docs/api.md)
        self.token = os.getenv("STUDIO_TOKEN", "").strip() or secrets.token_urlsafe(24)
        self.jobs: dict[int, Job] = {}
        self.queue: queue.Queue[Job] = queue.Queue()
        self.current: Job | None = None
        self._next = 1
        self._lock = threading.Lock()
        self._voices: dict[str, list] = {}
        self.version = 0                       # bumped on every job change; the event stream waits on it
        self._changed = threading.Condition()
        logging.getLogger().addHandler(_JobLog(self))
        threading.Thread(target=self._worker, name="studio-worker", daemon=True).start()
        self._scheduler_stop = threading.Event()
        self._youtube_auth_thread = None
        threading.Thread(target=self._scheduler_loop, name="studio-scheduler", daemon=True).start()

    def automation_state(self) -> dict:
        try:
            state = json.loads(self.studio.history.get_meta("studio_automation") or "{}")
        except (TypeError, ValueError):
            state = {}
        if not isinstance(state, dict):
            state = {}
        return {"enabled": bool(state.get("enabled", False)),
                "plan_time": str(state.get("plan_time") or "09:00"),
                "timezone": str(state.get("timezone") or self.settings.timezone or "Asia/Kolkata"),
                "ideas_per_run": max(1, min(30, int(state.get("ideas_per_run") or 8))),
                "auto_approve_ideas": bool(state.get("auto_approve_ideas", False)),
                "auto_produce": bool(state.get("auto_produce", False)),
                "auto_publish": bool(state.get("auto_publish", False)),
                "auto_sync_stats": bool(state.get("auto_sync_stats", False)),
                "last_run": state.get("last_run"),
                "last_status": state.get("last_status") or "idle",
                "last_message": state.get("last_message") or "",
                "last_production_run": state.get("last_production_run"),
                "last_production_status": state.get("last_production_status") or "idle",
                "last_production_message": state.get("last_production_message") or "",
                "mode": "review_required" if not state.get("auto_approve_ideas") else "auto_approval_enabled"}

    def update_automation(self, payload: dict) -> dict:
        state = self.automation_state()
        if "enabled" in payload:
            state["enabled"] = bool(payload["enabled"])
        if "plan_time" in payload:
            value = str(payload["plan_time"])
            if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value):
                raise ValueError("Plan time must be HH:MM (24-hour time)")
            state["plan_time"] = value
        if "timezone" in payload:
            from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
            value = str(payload["timezone"]).strip()[:64]
            try:
                ZoneInfo(value)
            except (ZoneInfoNotFoundError, ValueError):
                raise ValueError("Unknown IANA timezone") from None
            state["timezone"] = value
        if "ideas_per_run" in payload:
            try:
                state["ideas_per_run"] = max(1, min(30, int(payload["ideas_per_run"])))
            except (TypeError, ValueError):
                raise ValueError("Ideas per run must be a number") from None
        for key in ("auto_approve_ideas", "auto_produce", "auto_publish", "auto_sync_stats"):
            if key in payload:
                state[key] = bool(payload[key])
        if state["auto_publish"]:
            if not self.settings.upload:
                raise ValueError("Enable YouTube publishing in Setup first. UPLOAD=false is a hard safety stop.")
            if not Path(self.settings.token_file).expanduser().exists():
                raise ValueError("Connect a YouTube account before enabling automatic publishing.")
        self.studio.history.set_meta("studio_automation", json.dumps(state))
        self.changed()
        return state

    def _scheduler_loop(self) -> None:
        while not self._scheduler_stop.wait(15):
            try:
                state = self.automation_state()
                if not state["enabled"]:
                    continue
                from datetime import datetime
                from zoneinfo import ZoneInfo
                now = datetime.now(ZoneInfo(state["timezone"]))
                if now.strftime("%H:%M") < state["plan_time"]:
                    continue
                if state.get("last_run") == now.date().isoformat():
                    continue
                # Persist the date before queueing: a restart cannot enqueue duplicate daily research.
                state["last_run"] = now.date().isoformat()
                state["last_status"] = "queued"
                state["last_message"] = "Daily idea research queued"
                self.studio.history.set_meta("studio_automation", json.dumps(state))
                profile = self.profiles()["profiles"].get(self.profiles()["active"], {})
                niche = profile.get("niche") or (self.settings.niches[0] if self.settings.niches else "")
                self.submit("plan", {"count": state["ideas_per_run"], "niche": niche, "scheduled": True,
                                     "auto_approve": state["auto_approve_ideas"]},
                            "Scheduled daily idea research")
            except Exception:
                log.exception("Daily research scheduler failed")

    def changed(self) -> None:
        with self._changed:
            self.version += 1
            self._changed.notify_all()

    def wait_change(self, seen: int, timeout: float) -> int:
        with self._changed:
            self._changed.wait_for(lambda: self.version != seen, timeout=timeout)
            return self.version

    def jobs_public(self) -> list[dict]:
        return [j.public() for j in sorted(self.jobs.values(), key=lambda j: j.id, reverse=True)[:40]]

    # ------------------------------------------------------------------ jobs
    def submit(self, kind: str, params: dict, label: str = "") -> Job:
        with self._lock:
            job = Job(self._next, kind, params, label=label or kind)
            self._next += 1
            self.jobs[job.id] = job
            for old in sorted(self.jobs)[:-100]:  # keep the list bounded
                if self.jobs[old].status not in ("queued", "running"):
                    del self.jobs[old]
        self.queue.put(job)
        self.changed()
        return job

    def cancel(self, jid: int) -> bool:
        """Cancel a queued job, or stop the running one (its ffmpeg is stopped at once, the rest at the next
        stage)."""
        from . import ffmpeg
        from .utils import CANCEL
        job = self.jobs.get(jid)
        if job and job.status == "queued":
            job.status, job.finished = "cancelled", time.time()
            self.changed()
            return True
        if job and job.status == "running" and job is self.current:
            CANCEL.set()
            ffmpeg.kill_all()
            job.message = "cancelling…"
            self.changed()
            return True
        return False

    def _progress(self, job: Job):
        def cb(stage: str, pct: int, message: str) -> None:
            job.stage, job.pct = stage, max(job.pct, pct) if stage != "done" else 100
            job.message = message or job.message
            self.changed()
        return cb

    def overrides(self, p: dict) -> Settings:
        """Turn Create-form fields into Settings. Unknown values are ignored, never trusted."""
        from .overlays import CAPTION_STYLES
        from .render import GRADES, TRANSITIONS
        from .script import STYLES
        s = self.settings
        ov: dict = {
            "style": _choice(p.get("style"), STYLES),
            "language": (str(p.get("language") or "").strip().lower()[:8] or None),
            "tts_engine": _choice(p.get("tts"), ("edge", "openai", "elevenlabs", "kokoro", "coqui", "system")),
            "tts_voice": str(p.get("voice") or "").strip()[:200] or None,
            "tts_voice_b": str(p.get("voice_b") or "").strip()[:200] or None,
            "caption_style": _choice(p.get("caption_style"), CAPTION_STYLES),
            "caption_position": _choice(p.get("caption_position"), ("upper", "center", "lower")),
            "color_grade": _choice(p.get("grade"), GRADES),
            "transition": _choice(p.get("transition"), ["random", "none", *TRANSITIONS]),
            "llm_provider": str(p.get("provider") or "").strip().lower() or None,
            "llm_model": str(p.get("model") or "").strip()[:120] or None,
            "script_review": _choice(p.get("review"), ("off", "score", "rewrite")),
        }
        if p.get("duration"):
            try:
                ov["target_seconds"] = max(10, min(170, int(p["duration"])))
            except (TypeError, ValueError):
                pass
        if p.get("aspect") in ASPECTS:
            ov["resolution"] = aspect_resolution(p["aspect"])
        if isinstance(p.get("visuals"), list) and p["visuals"]:
            ov["visual_sources"] = [str(v).lower() for v in p["visuals"]][:6]
        if p.get("music") is False:
            ov["music_volume"] = 0.0
        if "background" in p:
            bg = str(p.get("background") or "").strip()[:1000]
            ov["background_video"] = bg if not bg or Path(bg).expanduser().exists() else None
        ov["background_layout"] = _choice(p.get("layout"), ("full", "split"))
        for key, field_name in (("sfx", "sound_effects"), ("emphasis", "caption_emphasis"), ("emoji", "caption_emoji"),
                                ("jump_cuts", "clip_jump_cuts"), ("zoom", "clip_zoom")):
            if isinstance(p.get(key), bool):
                ov[field_name] = p[key]
        s = s.with_overrides(**ov)
        if p.get("offline"):
            s = replace(s, offline=True)
        return s

    def _worker(self) -> None:
        from .pipeline import Studio
        from .utils import CANCEL, Cancelled
        while True:
            job = self.queue.get()
            if job.status == "cancelled":
                continue
            CANCEL.clear()
            self.current, job.status, job.started = job, "running", time.time()
            self.changed()
            p = job.params
            try:
                if job.kind == "upload":
                    status = self.studio.upload_record(int(p["id"]))
                    job.result = {"status": status, "id": int(p["id"])}
                    ok = status in ("uploaded", "scheduled", "queued")
                elif job.kind == "stats":
                    from .analytics import sync_stats
                    job.result = {"updated": sync_stats(self.settings, self.studio.history)}
                    ok = True
                else:
                    s = self.overrides(p)
                    studio = Studio(s, progress=self._progress(job))
                    job.eta_fn = studio.eta
                    ok = self._run(job, studio, s, p)
                job.status = "done" if ok else "failed"
            except Cancelled:
                job.status, job.message = "cancelled", "cancelled while running"
            except Exception as e:
                job.status, job.result = "failed", {"error": str(e)}
                log.exception("Studio job %d failed", job.id)
            finally:
                job.finished, job.eta_fn = time.time(), None
                if job.status == "done":
                    job.pct = 100
                self.current = None
                CANCEL.clear()
                if job.params.get("scheduled"):
                    state = self.automation_state()
                    scheduled_followup = None
                    schedule_stats = False
                    detail = (job.result.get("error") if isinstance(job.result, dict) else "") or job.message or job.status
                    if job.kind == "plan":
                        state["last_status"] = job.status
                        state["last_message"] = (f"Research finished: {job.result.get('ideas', 0)} ideas"
                                                  if job.status == "done" else detail)
                        # A successful research run may start one approved video. Newly researched ideas
                        # remain in review unless the owner explicitly enabled auto-approval.
                        if job.status == "done" and state["auto_produce"]:
                            candidate = next((idea for idea in self.studio.history.ideas("pending")
                                              if idea.get("approved")), None)
                            if candidate:
                                publish = bool(state["auto_publish"] and self.settings.upload
                                               and Path(self.settings.token_file).expanduser().exists())
                                scheduled_followup = ("make_idea", {"idea_id": candidate["id"], "upload": publish,
                                                                             "scheduled": True}, "Scheduled daily Short")
                                state["last_production_status"] = "queued"
                                state["last_production_message"] = "Approved idea queued for production"
                            else:
                                state["last_production_status"] = "waiting_approval"
                                state["last_production_message"] = "No approved ideas are available"
                    elif job.kind == "make_idea":
                        state["last_production_run"] = time.strftime("%Y-%m-%d")
                        state["last_production_status"] = job.status
                        state["last_production_message"] = detail or (
                            f"Production finished: {job.result.get('title', 'video')}" if job.status == "done"
                            else "Production failed")
                        if state["auto_sync_stats"] and Path(self.settings.token_file).expanduser().exists():
                            schedule_stats = True
                    elif job.kind == "stats":
                        state["last_production_message"] = "Channel analytics refreshed" if job.status == "done" else detail
                    self.studio.history.set_meta("studio_automation", json.dumps(state))
                    # Store state before queueing follow-ups so a very fast worker cannot be overwritten
                    # by the parent job's stale state snapshot.
                    if scheduled_followup:
                        self.submit(*scheduled_followup)
                    if schedule_stats:
                        self.submit("stats", {"scheduled": True}, "Scheduled channel analytics sync")
                    from . import notify
                    notify.send(self.settings, "failed" if job.status != "done" else "rendered",
                                title=job.label or "Scheduled automation", detail=state["last_message"] if job.kind == "plan"
                                else state["last_production_message"])
                self.changed()

    def _run(self, job: Job, studio, s: Settings, p: dict) -> bool:
        from .script import script_from_data
        if job.kind == "draft":
            script, pick, writer = studio.draft(p.get("topic") or None, source=p.get("source") or None,
                                                style=s.style if s.style != "auto" else None,
                                                url=p.get("url") or None)
            job.result = {"script": script.to_dict(), "source": pick.source, "llm": writer}
            job.stage, job.pct = "done", 100
            return True
        if job.kind == "plan":
            from .planner import plan_ideas
            niches = [p["niche"].strip()] if str(p.get("niche") or "").strip() else None
            ideas = plan_ideas(studio.llm, s, studio.history, int(p.get("count") or 10), niches,
                               approved=bool(p.get("auto_approve")))
            job.result = {"ideas": len(ideas)}
            return bool(ideas)
        if job.kind == "make_idea":
            idea_id = int(p.get("idea_id") or 0)
            idea = studio.history.claim_idea(idea_id)
            if not idea:
                raise ValueError("That idea is no longer available or has not been approved.")
            angle = (idea.get("notes") or "").strip()
            topic = f'{idea["subject"]} (angle: {angle})' if angle else idea["subject"]
            try:
                r = studio.make(topic, upload=bool(p.get("upload")) and s.upload,
                                style=idea.get("style") or (s.style if s.style != "auto" else None))
            except Exception:
                studio.history.release_idea(idea_id)
                raise
            if r.ok and r.record_id:
                studio.history.link_idea(idea_id, r.record_id)
            else:
                studio.history.release_idea(idea_id)
            job.result = _result(r)
            return r.ok
        if job.kind == "clip":
            from .clipper import make_clips
            results = make_clips(studio, str(p.get("source") or ""), count=int(p.get("count") or 3),
                                 crop=p.get("crop") or "center", upload=bool(p.get("upload")) and s.upload,
                                 min_seconds=int(p.get("min_seconds") or 20), max_seconds=int(p.get("max_seconds") or 58),
                                 brief=str(p.get("brief") or "")[:500])
            job.result = {"clips": [_result(r) for r in results]}
            return any(r.ok for r in results)
        # make
        script = script_from_data(p["script"], s) if isinstance(p.get("script"), dict) else None
        langs = p.get("also_langs")
        if isinstance(langs, str):
            langs = [x.strip().lower() for x in langs.split(",") if x.strip()]
        # UPLOAD=false in .env is a hard stop, whatever a request asks for
        r = studio.make(p.get("topic") or None, source=p.get("source") or None,
                        upload=bool(p.get("upload")) and s.upload,
                        style=s.style if s.style != "auto" else None, script=script, url=p.get("url") or None,
                        languages=[str(x)[:8] for x in (langs or [])][:6])
        job.result = _result(r)
        return r.ok

    # ------------------------------------------------------------------ data
    def videos(self, status: str = "", q: str = "") -> list[dict]:
        out = []
        for r in self.studio.history.recent(200):
            if status and r["status"] != status:
                continue
            if q and q.lower() not in f"{r['title']} {r['topic']} {r['subject']}".lower():
                continue
            folder = Path(r["folder"]) if r["folder"] else None
            video = Path(r["video_path"]) if r["video_path"] else None
            out.append({
                "id": r["id"], "title": r["title"], "status": r["status"], "created": r["created_at"],
                "youtube_id": r["youtube_id"], "publish_at": r["publish_at"], "duration": r["duration"],
                "style": r["style"], "language": r["language"], "score": r.get("score"),
                "views": r.get("views"), "likes": r.get("likes"), "parent_id": r.get("parent_id"),
                "video": video.name if video and video.exists() else None,
                "has_cover": bool(folder and (folder / "cover.jpg").exists()),
                "error": r["error"],
            })
        return out

    def video(self, vid: int) -> dict | None:
        rec = self.studio.history.get(vid)
        if not rec:
            return None
        folder = Path(rec["folder"]) if rec["folder"] else None
        data = dict(rec)
        data["tags"] = json.loads(rec["tags"] or "[]")
        for name in ("metadata", "script"):
            f = folder / f"{name}.json" if folder else None
            try:
                data[name] = json.loads(f.read_text(encoding="utf-8")) if f and f.exists() else None
            except (OSError, ValueError):
                data[name] = None
        video = Path(rec["video_path"]) if rec["video_path"] else None
        data["video"] = video.name if video and video.exists() else None
        data["has_cover"] = bool(folder and (folder / "cover.jpg").exists())
        data["has_srt"] = bool(folder and (folder / "captions.srt").exists())
        return data

    def file_for(self, vid: int, name: str) -> Path | None:
        if name not in FILES:
            return None
        rec = self.studio.history.get(vid)
        if not rec or not rec["folder"]:
            return None
        p = Path(rec["folder"]) / name
        return p if p.exists() else None

    def voices(self, lang: str) -> list[dict]:
        lang = (lang or self.settings.language).lower()[:8]
        if lang not in self._voices:
            try:
                from .tts import list_edge_voices
                self._voices[lang] = [{"name": v["ShortName"], "gender": v["Gender"], "locale": v["Locale"]}
                                      for v in list_edge_voices(lang)]
            except Exception as e:
                log.warning("Could not list voices: %s", e)
                return []
        return self._voices[lang]

    def info(self) -> dict:
        s = self.settings
        from .llm import LLMError, resolve_provider_name
        try:
            llm = f"{resolve_provider_name(s)}{(':' + s.llm_model) if s.llm_model else ''}"
            llm_ok = True
        except LLMError:
            llm, llm_ok = "not configured", False
        h = self.studio.history
        stats = h.stats()
        views = sum(r.get("views") or 0 for r in h.recent(1000))
        return {"version": __version__, "llm": llm, "llm_ok": llm_ok, "tts": s.tts_engine,
                "voice": s.tts_voice or "auto", "visuals": s.visual_sources, "upload": s.upload,
                "privacy": s.privacy, "quota_left": self.studio.quota_left(), "daily_limit": s.daily_upload_limit,
                "language": s.language, "caption_style": s.caption_style, "caption_position": s.caption_position,
                "duration": s.target_seconds, "style": s.style, "grade": s.color_grade, "transition": s.transition,
                "review": s.script_review, "aspect": next((k for k, v in ASPECTS.items() if v == s.resolution),
                                                          f"{s.width}x{s.height}"),
                "also_languages": s.also_languages, "learn_from_stats": s.learn_from_stats,
                "publish_to": s.publish_to,
                "counts": stats, "total": sum(stats.values()), "views": views,
                "ideas": len(h.ideas("pending")), "channel": s.channel_name}

    def options(self) -> dict:
        from .llm import PRESETS, available_providers
        from .overlays import CAPTION_STYLES
        from .render import GRADES, TRANSITIONS
        from .script import LANGUAGES, STYLES
        ready = set(available_providers(self.settings))
        return {"styles": STYLES, "languages": LANGUAGES, "aspects": list(ASPECTS),
                "caption_styles": list(CAPTION_STYLES), "grades": list(GRADES),
                "transitions": ["random", "none", *TRANSITIONS],
                "providers": [{"name": n, "label": p.label, "ready": n in ready or p.local}
                              for n, p in PRESETS.items()],
                "tts": ["edge", "openai", "elevenlabs", "kokoro", "coqui", "system"],
                "visuals": ["pexels", "pixabay", "local", "pollinations", "openai-images"],
                "backgrounds": self.backgrounds()}

    # -- reusable content profiles ---------------------------------------------------------
    def profiles(self) -> dict:
        try:
            saved = json.loads(self.studio.history.get_meta("studio_profiles") or "{}")
        except (TypeError, ValueError):
            saved = {}
        if not isinstance(saved, dict):
            saved = {}
        active = str(self.studio.history.get_meta("studio_active_profile") or "Default")
        return {"profiles": saved, "active": active}

    def save_profile(self, payload: dict) -> dict:
        from .script import STYLES
        name = str(payload.get("name") or "").strip()[:48]
        if not name:
            raise ValueError("Profile name is required")
        niche = str(payload.get("niche") or "").strip()[:240]
        if not niche:
            raise ValueError("Niche is required")
        style = _choice(payload.get("style"), STYLES, "auto")
        language = str(payload.get("language") or self.settings.language).strip().lower()[:8]
        if not re.fullmatch(r"[a-z]{2,3}(?:-[A-Za-z0-9]{2,4})?", language):
            raise ValueError("Invalid language code")
        try:
            duration = max(10, min(170, int(payload.get("duration") or self.settings.target_seconds)))
        except (TypeError, ValueError):
            raise ValueError("Duration must be a number") from None
        aspect = str(payload.get("aspect") or "9:16")
        if aspect not in ASPECTS:
            raise ValueError("Unsupported aspect ratio")
        visual_style = str(payload.get("visual_style") or self.settings.visual_style).strip()[:500]
        from .llm import PRESETS
        provider = str(payload.get("provider") or self.settings.llm_provider).strip().lower()[:40]
        if provider not in PRESETS and provider != "auto":
            raise ValueError("Choose a supported LLM provider")
        model = str(payload.get("model") or self.settings.llm_model).strip()[:120]
        channel_name = str(payload.get("channel_name") or self.settings.channel_name).strip()[:100]
        audience = str(payload.get("audience") or self.settings.audience).strip()[:240]
        youtube_channel = str(payload.get("youtube_channel") or "").strip()[:100]
        slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:40] or "creator"
        token_file = str(Path(self.settings.data_dir) / "profiles" / slug / "token.json")
        profile = {"name": name, "niche": niche, "style": style, "language": language,
                   "duration": duration, "aspect": aspect, "visual_style": visual_style,
                   "provider": provider, "model": model, "channel_name": channel_name, "audience": audience,
                   "youtube_channel": youtube_channel, "token_file": token_file}
        state = self.profiles()
        state["profiles"][name] = profile
        self.studio.history.set_meta("studio_profiles", json.dumps(state["profiles"], ensure_ascii=False))
        # Saving from the profile editor also selects the profile immediately, so the
        # next Create job uses the newly saved niche and production settings.
        return self.apply_profile(name)

    def apply_profile(self, name: str) -> dict:
        state = self.profiles()
        profile = state["profiles"].get(str(name))
        if not isinstance(profile, dict):
            raise ValueError("Profile not found")
        self.settings = self.settings.with_overrides(
            niches=[profile["niche"]], style=profile["style"], language=profile["language"],
            target_seconds=profile["duration"], resolution=aspect_resolution(profile["aspect"]),
            visual_style=profile["visual_style"], llm_provider=profile.get("provider", self.settings.llm_provider),
            llm_model=profile.get("model", self.settings.llm_model),
            channel_name=profile.get("channel_name", self.settings.channel_name),
            audience=profile.get("audience", self.settings.audience),
            token_file=profile.get("token_file", self.settings.token_file))
        # Pipeline Studio consumes Settings through .s; clear its cached LLM so provider
        # switches take effect immediately instead of continuing to use the previous model.
        self.studio.s = self.settings
        self.studio._llm = None
        self.studio.history.set_meta("studio_active_profile", str(name))
        return {"profile": profile, "active": str(name)}

    def delete_profile(self, name: str) -> dict:
        state = self.profiles()
        if name == "Default":
            raise ValueError("The Default profile cannot be deleted")
        state["profiles"].pop(name, None)
        self.studio.history.set_meta("studio_profiles", json.dumps(state["profiles"], ensure_ascii=False))
        if state["active"] == name:
            self.studio.history.set_meta("studio_active_profile", "Default")
        return self.profiles()

    def setup_state(self) -> dict:
        """Return connection readiness only; never return API keys, OAuth tokens, or secret values."""
        s = self.settings
        token = Path(s.token_file)
        return {
            "llm_provider": s.llm_provider, "llm_model": s.llm_model,
            "llm_key_set": bool(s.llm_api_key or s.openai_api_key),
            "pexels_key_set": bool(s.pexels_api_key), "pixabay_key_set": bool(s.pixabay_api_key),
            "elevenlabs_key_set": bool(s.elevenlabs_api_key),
            "youtube_client_file": Path(s.client_secrets).exists(),
            "youtube_connected": token.exists(),
            "youtube_auth_status": self.studio.history.get_meta("youtube_auth_status") or "not_started",
            "upload_enabled": bool(s.upload),
            "privacy": s.privacy,
            "profile_count": len(self.profiles()["profiles"]),
        }

    def connect_youtube(self) -> dict:
        """Start the official local OAuth consent flow without blocking the Studio request."""
        if self._youtube_auth_thread and self._youtube_auth_thread.is_alive():
            return {"status": "authorizing"}
        self.studio.history.set_meta("youtube_auth_status", "authorizing")
        def run():
            try:
                from . import youtube
                youtube.get_credentials(self.settings, interactive=True, extra_scopes=youtube.auth_scopes(self.settings))
                self.studio.history.set_meta("youtube_auth_status", "connected")
            except Exception as exc:
                self.studio.history.set_meta("youtube_auth_status", "failed: " + str(exc)[:300])
                log.warning("YouTube authorization failed: %s", exc)
            self.changed()
        self._youtube_auth_thread = threading.Thread(target=run, name="youtube-oauth", daemon=True)
        self._youtube_auth_thread.start()
        return {"status": "authorizing"}

    def save_setup(self, payload: dict) -> dict:
        """Save user-supplied provider credentials locally, reload settings, and never echo secrets."""
        allowed = {
            "LLM_PROVIDER": "llm_provider", "LLM_MODEL": "llm_model", "LLM_BASE_URL": "llm_base_url",
            "LLM_API_KEY": "llm_api_key", "OPENAI_API_KEY": "openai_api_key",
            "PEXELS_API_KEY": "pexels_api_key", "PIXABAY_API_KEY": "pixabay_api_key",
            "ELEVENLABS_API_KEY": "elevenlabs_api_key",
        }
        updates = {}
        for env_name, field_name in allowed.items():
            value = payload.get(field_name)
            if value is not None and str(value).strip():
                updates[env_name] = str(value).strip()
        if "upload_enabled" in payload:
            value = payload.get("upload_enabled")
            enabled = value if isinstance(value, bool) else str(value).strip().lower() in {"1", "true", "yes", "on"}
            updates["UPLOAD"] = "true" if enabled else "false"
        client_json = str(payload.get("youtube_client_json") or "").strip()
        if client_json:
            try:
                parsed = json.loads(client_json)
                client = parsed.get("installed") or parsed.get("web") or {}
                if not client.get("client_id") or not client.get("client_secret"):
                    raise ValueError
            except (ValueError, TypeError, AttributeError):
                raise ValueError("Google OAuth JSON is invalid. Use a Desktop app OAuth client JSON file.") from None
            target = Path(self.settings.client_secrets).expanduser()
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(parsed, indent=2), encoding="utf-8")
            try:
                os.chmod(target, 0o600)
            except OSError:
                pass
        if updates:
            env_path = Path(".env")
            existing = env_path.read_text(encoding="utf-8") if env_path.exists() else ""
            lines = existing.splitlines()
            seen = set()
            for index, line in enumerate(lines):
                match = re.match(r"^\s*([A-Z][A-Z0-9_]*)\s*=", line)
                if match and match.group(1) in updates:
                    key = match.group(1)
                    lines[index] = f"{key}={json.dumps(updates[key])}"
                    seen.add(key)
            for key, value in updates.items():
                if key not in seen:
                    lines.append(f"{key}={json.dumps(value)}")
            temp = env_path.with_suffix(".env.tmp")
            temp.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
            try:
                os.chmod(temp, 0o600)
            except OSError:
                pass
            temp.replace(env_path)
            os.environ.update(updates)
        field_updates = {field_name: updates[env_name] for env_name, field_name in allowed.items()
                         if env_name in updates}
        if "UPLOAD" in updates:
            field_updates["upload"] = updates["UPLOAD"].lower() == "true"
        # Keep all current runtime choices; only replace the fields explicitly edited in Setup.
        fresh = self.settings.with_overrides(**field_updates)
        self.settings = fresh
        self.studio.s = fresh
        self.studio._llm = None
        # If the active profile exists, keep its provider/model in sync with Setup edits.
        active = self.profiles()["active"]
        profile_state = self.profiles()
        if active in profile_state["profiles"] and ({"LLM_PROVIDER", "LLM_MODEL"} & set(updates)):
            profile_state["profiles"][active]["provider"] = fresh.llm_provider
            profile_state["profiles"][active]["model"] = fresh.llm_model
            self.studio.history.set_meta("studio_profiles",
                json.dumps(profile_state["profiles"], ensure_ascii=False))
        self.changed()
        return self.setup_state()

    def backgrounds(self) -> list[dict]:
        """Background videos to offer: BACKGROUND_VIDEO (a file, or every video in that folder) and
        media/backgrounds/."""
        from .media import VIDEO_EXT
        s = self.settings
        out: list[dict] = []
        if s.background_video:
            out.append({"path": s.background_video, "name": Path(s.background_video).name + (
                " (random)" if Path(s.background_video).expanduser().is_dir() else "")})
        for root in [Path(s.background_video).expanduser(), Path(s.media_dir) / "backgrounds"]:
            if not root.is_dir():
                continue
            for f in sorted(root.rglob("*")):
                if f.suffix.lower() in VIDEO_EXT and not f.name.startswith(".") and len(out) < 60:
                    item = {"path": str(f), "name": f.name}
                    if item not in out:
                        out.append(item)
        return out

    def channel(self) -> dict:
        from .analytics import calibration
        rows = [r for r in self.studio.history.recent(1000) if r.get("views") is not None]
        by_style: dict[str, list[int]] = {}
        for r in rows:
            by_style.setdefault(r["style"] or "other", []).append(r["views"])
        total_views = sum(r.get("views") or 0 for r in rows)
        total_likes = sum(r.get("likes") or 0 for r in rows)
        total_comments = sum(r.get("comments") or 0 for r in rows)
        return {"videos": [{"id": r["id"], "title": r["title"], "views": r["views"], "likes": r["likes"],
                            "comments": r["comments"], "score": r.get("score"), "style": r["style"],
                            "engagement_rate": round(100 * ((r.get("likes") or 0) + (r.get("comments") or 0)) /
                                                     max(1, r["views"]), 2),
                            "youtube_id": r["youtube_id"]} for r in sorted(rows, key=lambda r: -r["views"])[:100]],
                "by_style": sorted(({"style": k, "videos": len(v), "avg_views": round(sum(v) / len(v)),
                                     "avg_engagement_rate": round(100 * sum(
                                         ((r.get("likes") or 0) + (r.get("comments") or 0)) / max(1, r["views"])
                                         for r in rows if (r.get("style") or "other") == k) / len(v), 2)}
                                    for k, v in by_style.items()), key=lambda x: -x["avg_views"]),
                "summary": {"videos_with_stats": len(rows), "views": total_views, "likes": total_likes,
                            "comments": total_comments,
                            "engagement_rate": round(100 * (total_likes + total_comments) / max(1, total_views), 2),
                            "avg_views": round(total_views / len(rows)) if rows else 0},
                "synced": max((r.get("stats_at") or "" for r in rows), default=""),
                "calibration": calibration(self.studio.history)}


def _result(r) -> dict:
    return {"ok": r.ok, "title": r.title, "status": r.status, "youtube_id": r.youtube_id, "error": r.error,
            "id": r.record_id, "score": r.score, "language": r.language, "seconds": round(r.seconds, 1),
            "reason": getattr(r, "reason", ""), "posts": getattr(r, "posts", {}),
            "variants": [_result(v) for v in getattr(r, "variants", [])]}


def _handler(app: StudioServer, bound_host: str, password: str | None = None):
    loopback = bound_host in LOCAL_HOSTS
    password = os.getenv("STUDIO_PASSWORD", "") if password is None else password

    class H(BaseHTTPRequestHandler):
        server_version = f"PurffleStudio/{__version__}"

        def log_message(self, fmt, *args):  # keep the console for pipeline logs
            pass

        def _auth_ok(self) -> bool:
            """With STUDIO_PASSWORD set (for a Studio on a server), every request needs it (HTTP Basic)."""
            if not password:
                return True
            scheme, _, value = (self.headers.get("Authorization") or "").partition(" ")
            if scheme.lower() != "basic":
                return False
            try:
                _, _, given = base64.b64decode(value).decode("utf-8").partition(":")
            except (ValueError, UnicodeDecodeError):
                return False
            return hmac.compare_digest(given.encode(), password.encode())

        def _gate(self) -> bool:
            """True when the request may go on; otherwise the refusal has been sent."""
            if not self._host_ok():
                self._send(421, b"wrong host", "text/plain")
                return False
            if not self._auth_ok():
                self._send(401, b"password required", "text/plain",
                           {"WWW-Authenticate": 'Basic realm="PurffleShorts Studio", charset="UTF-8"'})
                return False
            return True

        def _events(self):
            """Server-sent events: the job list whenever anything changes (at most 4 times a second), and a
            comment every 15 s so proxies keep the connection open."""
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            seen = -1
            try:
                self.wfile.write(b"retry: 2000\n\n")
                while True:
                    version = app.wait_change(seen, 15.0)
                    if version != seen:
                        seen = version
                        data = json.dumps(app.jobs_public(), default=str)
                        self.wfile.write(f"event: jobs\ndata: {data}\n\n".encode())
                    else:
                        self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    time.sleep(0.25)
            except (BrokenPipeError, ConnectionResetError, OSError):
                return

        def _host_ok(self) -> bool:
            if not loopback:
                return True
            host = (self.headers.get("Host") or "").strip().lower()
            name = host.rsplit(":", 1)[0] if not host.startswith("[") else host.split("]")[0] + "]"
            return name in LOCAL_HOSTS

        def _send(self, code: int, body: bytes, ctype: str, extra: dict | None = None, cache: str = "no-store"):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", cache)
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

        def _json(self, data, code: int = 200):
            self._send(code, json.dumps(data, default=str).encode(), "application/json")

        def do_GET(self):
            if not self._gate():
                return
            path, _, qs = self.path.partition("?")
            params = dict(x.partition("=")[::2] for x in qs.split("&") if x)
            from urllib.parse import unquote_plus
            params = {k: unquote_plus(v) for k, v in params.items()}
            if path == "/":
                html = (WEB / "index.html").read_text(encoding="utf-8")
                html = html.replace("__TOKEN__", app.token).replace("__VERSION__", __version__)
                return self._send(200, html.encode(), "text/html; charset=utf-8", {
                    "Content-Security-Policy": "default-src 'self'; img-src 'self' data: https://i.ytimg.com; "
                                               "media-src 'self'; connect-src 'self'; frame-ancestors 'none'",
                    "X-Frame-Options": "DENY"})
            m = re.fullmatch(r"/static/([\w.-]+)", path)
            if m and m.group(1) in STATIC:
                return self._send(200, (WEB / m.group(1)).read_bytes(), STATIC[m.group(1)])
            m = re.fullmatch(r"/static/fonts/([\w-]+\.woff2)", path)
            if m and (WEB / "fonts" / m.group(1)).is_file():  # fonts never change within a version: cache them
                return self._send(200, (WEB / "fonts" / m.group(1)).read_bytes(), "font/woff2",
                                  cache="public, max-age=86400")
            if path == "/api/info":
                return self._json(app.info())
            if path == "/api/options":
                return self._json(app.options())
            if path == "/api/profiles":
                return self._json(app.profiles())
            if path == "/api/setup":
                return self._json(app.setup_state())
            if path == "/api/automation":
                return self._json(app.automation_state())
            if path == "/api/videos":
                return self._json(app.videos(params.get("status", ""), params.get("q", "")))
            m = re.fullmatch(r"/api/videos/(\d+)", path)
            if m:
                v = app.video(int(m.group(1)))
                return self._json(v) if v else self._json({"error": "not found"}, 404)
            if path == "/api/jobs":
                return self._json(app.jobs_public())
            if path == "/api/events":
                return self._events()
            m = re.fullmatch(r"/api/jobs/(\d+)", path)
            if m and int(m.group(1)) in app.jobs:
                return self._json(app.jobs[int(m.group(1))].public(full_log=True))
            if path == "/api/ideas":
                pending = app.studio.history.ideas("pending")
                return self._json({"pending": pending,
                                   "review": [i for i in pending if not i.get("approved")],
                                   "approved": [i for i in pending if i.get("approved")],
                                   "used": app.studio.history.ideas("used", 30)})
            if path == "/api/doctor":
                from .doctor import checks
                return self._json(checks(app.settings))
            if path == "/api/channel":
                return self._json(app.channel())
            if path == "/api/voices":
                return self._json(app.voices(params.get("lang", "")))
            m = re.fullmatch(r"/files/(\d+)/([\w.]+)", path)
            if m:
                f = app.file_for(int(m.group(1)), m.group(2))
                if not f:
                    return self._send(404, b"not found", "text/plain")
                return self._file(f, download="download" in params)
            self._send(404, b"not found", "text/plain")

        def _file(self, f: Path, download: bool = False):
            size = f.stat().st_size
            ctype = mimetypes.guess_type(f.name)[0] or "application/octet-stream"
            if f.suffix == ".srt":
                ctype = "text/plain; charset=utf-8"
            rng = self.headers.get("Range")
            start, end = 0, size - 1
            m = re.match(r"bytes=(\d*)-(\d*)", rng or "")
            if m and (m.group(1) or m.group(2)):
                if m.group(1):
                    start = int(m.group(1))
                    end = int(m.group(2)) if m.group(2) else size - 1
                else:
                    start = max(0, size - int(m.group(2)))
                end = min(end, size - 1)
                if start > end:
                    self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                    self.send_header("Content-Range", f"bytes */{size}")
                    self.end_headers()
                    return
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            else:
                self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("X-Content-Type-Options", "nosniff")
            if download:
                folder = re.sub(r"[^\w.-]+", "-", f.parent.name)[:80]
                self.send_header("Content-Disposition", f'attachment; filename="{folder}{f.suffix}"')
            self.end_headers()
            with open(f, "rb") as fh:
                fh.seek(start)
                left = end - start + 1
                while left > 0:
                    chunk = fh.read(min(1 << 16, left))
                    if not chunk:
                        break
                    try:
                        self.wfile.write(chunk)
                    except (BrokenPipeError, ConnectionResetError):
                        return
                    left -= len(chunk)

        def do_POST(self):
            if not self._gate():
                return
            if not hmac.compare_digest(self.headers.get("X-Studio-Token") or "", app.token):
                return self._json({"error": "bad token"}, 403)
            length = min(int(self.headers.get("Content-Length") or 0), 512 * 1024)
            try:
                body = json.loads(self.rfile.read(length) or b"{}")
            except json.JSONDecodeError:
                return self._json({"error": "bad json"}, 400)
            if not isinstance(body, dict):
                return self._json({"error": "expected an object"}, 400)
            path = self.path
            h = app.studio.history
            if path == "/api/automation":
                try:
                    return self._json(app.update_automation(body))
                except ValueError as e:
                    return self._json({"error": str(e)}, 400)
            if path == "/api/setup":
                try:
                    return self._json(app.save_setup(body))
                except ValueError as e:
                    return self._json({"error": str(e)}, 400)
            if path == "/api/youtube/connect":
                return self._json(app.connect_youtube())
            if path == "/api/profiles/save":
                try:
                    return self._json(app.save_profile(body))
                except ValueError as e:
                    return self._json({"error": str(e)}, 400)
            if path == "/api/profiles/apply":
                try:
                    return self._json(app.apply_profile(str(body.get("name") or "")))
                except ValueError as e:
                    return self._json({"error": str(e)}, 400)
            if path == "/api/profiles/delete":
                try:
                    return self._json(app.delete_profile(str(body.get("name") or "")))
                except ValueError as e:
                    return self._json({"error": str(e)}, 400)

            if path in ("/api/make", "/api/draft"):
                kind = "make" if path == "/api/make" else "draft"
                if kind == "make" and body.get("upload"):
                    if not app.settings.upload:
                        return self._json({"error": "Uploads are disabled in Setup. Enable publishing explicitly first."}, 403)
                    if not Path(app.settings.token_file).expanduser().exists():
                        return self._json({"error": "Connect a YouTube account in Setup before uploading."}, 403)
                label = (body.get("script") or {}).get("title") if isinstance(body.get("script"), dict) else None
                label = (label or body.get("topic") or body.get("url")
                         or ("Demo video" if body.get("offline") else
                             "Next idea from the queue" if body.get("source") == "queue" else "AI-picked topic"))
                return self._json({"job": app.submit(kind, body, str(label)[:120]).id})
            if path == "/api/clip":
                if body.get("upload") and not app.settings.upload:
                    return self._json({"error": "Uploads are disabled in Setup. Enable publishing explicitly first."}, 403)
                if body.get("upload") and not Path(app.settings.token_file).expanduser().exists():
                    return self._json({"error": "Connect a YouTube account in Setup before uploading."}, 403)
                if not str(body.get("source") or "").strip():
                    return self._json({"error": "give a video file path or URL"}, 400)
                src = str(body["source"]).strip()
                name = src if re.match(r"^https?://", src) else (Path(src).name or src)
                return self._json({"job": app.submit("clip", body, f"Clip: {name[:100]}").id})
            if path == "/api/plan":
                label = f"Plan {body.get('count') or 10} ideas" + (f" · {body['niche']}" if body.get("niche") else "")
                return self._json({"job": app.submit("plan", body, label).id})
            if path == "/api/stats":
                return self._json({"job": app.submit("stats", {}, "Refresh channel stats").id})
            if path == "/api/ideas":
                subject = str(body.get("subject") or "").strip()
                if not subject:
                    return self._json({"error": "empty idea"}, 400)
                from .script import STYLES
                idea_id = h.add_idea(subject[:300], _choice(body.get("style"), STYLES, ""),
                                       str(body.get("notes") or "")[:500], approved=False)
                return self._json({"id": idea_id, "status": "review"})
            m = re.fullmatch(r"/api/ideas/(\d+)/approve", path)
            if m:
                ok = h.approve_idea(int(m.group(1)))
                return self._json({"ok": ok}, 200 if ok else 404)
            m = re.fullmatch(r"/api/ideas/(\d+)/make", path)
            if m:
                idea_id = int(m.group(1))
                idea = next((i for i in h.ideas("pending") if i["id"] == idea_id), None)
                if not idea:
                    return self._json({"error": "idea not found"}, 404)
                if not idea.get("approved"):
                    return self._json({"error": "approve this idea before production"}, 409)
                return self._json({"job": app.submit("make_idea", {"idea_id": idea_id, "upload": False},
                                                     f"Make idea: {idea['subject'][:90]}").id})
            m = re.fullmatch(r"/api/ideas/(\d+)/delete", path)
            if m:
                h.delete_idea(int(m.group(1)))
                return self._json({"ok": True})
            m = re.fullmatch(r"/api/upload/(\d+)", path)
            if m:
                if not app.settings.upload:
                    return self._json({"error": "Uploads are disabled in Setup (UPLOAD=false). Enable explicitly before uploading."}, 403)
                if not Path(app.settings.token_file).expanduser().exists():
                    return self._json({"error": "Connect a YouTube account in Setup before uploading."}, 403)
                rec = h.get(int(m.group(1)))
                label = f"Upload: {rec['title']}" if rec else f"Upload #{m.group(1)}"
                return self._json({"job": app.submit("upload", {"id": int(m.group(1))}, label).id})
            m = re.fullmatch(r"/api/videos/(\d+)/delete", path)
            if m:
                return self._json({"ok": app.studio.delete(int(m.group(1)))})
            m = re.fullmatch(r"/api/jobs/(\d+)/cancel", path)
            if m:
                return self._json({"ok": app.cancel(int(m.group(1)))})
            self._json({"error": "not found"}, 404)

    return H


def serve(settings: Settings, host: str = "127.0.0.1", port: int = 8765, open_browser: bool = True) -> None:
    app = StudioServer(settings)
    httpd = ThreadingHTTPServer((host, port), _handler(app, host))
    url = f"http://{'127.0.0.1' if host in ('0.0.0.0', '::') else host}:{port}/"
    log.info("PurffleShorts Studio running at %s  (Ctrl+C to stop)", url)
    if host not in LOCAL_HOSTS and not os.getenv("STUDIO_PASSWORD"):
        log.warning("Studio is reachable from your network on %s — anyone who can load the page can use it. "
                    "Set STUDIO_PASSWORD to require a password.", host)
    elif os.getenv("STUDIO_PASSWORD"):
        log.info("Studio password is on (any user name, the password from STUDIO_PASSWORD)")
    if open_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
