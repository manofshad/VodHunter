import asyncio
from concurrent.futures import Future
from contextlib import contextmanager
from io import BytesIO
import json
from pathlib import Path
import subprocess
import sys
from threading import Event
import wave
from types import SimpleNamespace

from fastapi.testclient import TestClient
import pytest
from starlette.datastructures import UploadFile

from backend.apps.public import create_public_app
from backend.services.media_duration import MediaDurationError, probe_uploaded_video
from backend.services.search_jobs import SearchJobService
from backend.services.search_manager import SearchManager
from backend.services.uploaded_clip_store import MAX_ACTIVE_UPLOADS, MAX_MULTIPART_BYTES, MAX_UPLOAD_BYTES, StagedUpload, UploadError, UploadedClipStore
from backend.upload_body_limit import MAX_INGRESS_REQUESTS, UploadBodyLimitMiddleware
from search.access import create_search_token
from search.models import SearchExecutionMetadata, SearchExecutionResult, SearchJobRecord, SearchResult
from search.query_preprocessor import QueryPreprocessor
from storage.search_job_repository import SearchJobRepository


class Jobs:
    def __init__(self):
        self.records = {}
        self.tokens = {}
        self.fail_insert = False
        self.fail_status = False
        self.fail_persist = False

    def create_uploaded_search_job(self, *, search_token, clip_filename, streamer, **kwargs):
        if self.fail_insert:
            raise RuntimeError("database unavailable")
        identity = len(self.records) + 1
        self.tokens[search_token] = identity
        self.records[identity] = SearchJobRecord(
            identity, "queued", "validating", "2026-10-13T00:00:00Z", None, None,
            None, None, None, streamer=streamer, input_type="upload", clip_filename=clip_filename,
        )
        return identity

    def update_search_job_status(self, search_id, *, status=None, stage=None, started=False):
        if self.fail_status:
            raise RuntimeError("private status failure")
        record = self.records[search_id]
        if status:
            record.status = status
        record.stage = stage

    def complete_search_job(self, search_id, outcome):
        if self.fail_persist:
            raise RuntimeError("private persistence failure")
        record = self.records[search_id]
        record.status = "completed"
        record.stage = None
        record.result = outcome.result

    def fail_search_job(self, search_id, *, error_code, error_message, **kwargs):
        record = self.records[search_id]
        record.status = "failed"
        record.stage = None
        record.error_code = error_code
        record.error_message = error_message

    def get_public_search_job(self, token):
        return self.records.get(self.tokens.get(token))


class InlineExecutor:
    def submit(self, callback, *args):
        future = Future()
        try:
            future.set_result(callback(*args))
        except BaseException as exc:
            future.set_exception(exc)
        return future


class QueuedExecutor:
    def __init__(self):
        self.futures = []
        self.callbacks = []

    def submit(self, callback, *args):
        future = Future()
        self.futures.append(future)
        self.callbacks.append(lambda: callback(*args))
        return future


class Matcher:
    def __init__(self):
        self.calls = []

    def search_file(self, path, streamer, **kwargs):
        assert Path(path).is_file()
        self.calls.append((path, streamer, kwargs))
        return SearchExecutionResult(SearchResult(found=False, reason="test no match"), SearchExecutionMetadata())


class Videos:
    def list_searchable_streamers(self):
        return [SimpleNamespace(name="xqc", profile_image_url=None)]

    def get_creator_id_by_name(self, name):
        return 1


@pytest.fixture(scope="module")
def media(tmp_path_factory):
    root = tmp_path_factory.mktemp("upload-media")
    fixtures = {}
    for suffix in ["mp4", "mov", "m4v", "webm", "mkv"]:
        path = root / f"clip.{suffix}"
        codecs = ["-c:v", "libvpx-vp9", "-c:a", "libopus"] if suffix == "webm" else ["-c:v", "libx264", "-c:a", "aac"]
        subprocess.run([
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "color=size=32x32:rate=5",
            "-f", "lavfi", "-i", "sine=frequency=440", "-t", "1", *codecs, "-y", str(path),
        ], check=True, capture_output=True, timeout=30)
        fixtures[suffix] = path
    for name, options in [("audio-only", ["-vn"]), ("no-audio", ["-an"]), ("overlong", ["-t", "181"])]:
        path = root / f"{name}.mp4"
        subprocess.run([
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "color=size=16x16:rate=1",
            "-f", "lavfi", "-i", "sine=frequency=440", "-t", "1", "-c:v", "libx264", "-c:a", "aac",
            *options, "-y", str(path),
        ], check=True, capture_output=True, timeout=30)
        fixtures[name] = path
    fixtures["corrupt"] = root / "corrupt.mp4"
    fixtures["corrupt"].write_bytes(b"not a video")
    return fixtures


@pytest.fixture
def stack(tmp_path):
    store = UploadedClipStore(tmp_path / "uploads")
    jobs = Jobs()
    matcher = Matcher()
    executor = InlineExecutor()
    service = SearchJobService(jobs, SearchManager(matcher, None, max_duration_seconds=180), executor, store)
    app = create_public_app(enable_lifespan=False)
    app.state.videos = Videos()
    app.state.search_job_service = service
    with TestClient(app) as client:
        yield SimpleNamespace(store=store, jobs=jobs, matcher=matcher, service=service, client=client)
    store.close()


@pytest.mark.parametrize("suffix", ["mp4", "mov", "m4v", "webm", "mkv"])
def test_real_upload_containers_match_and_restore_metadata(stack, media, suffix):
    response = stack.client.post("/api/search/clip", data={"streamer": "XQC"}, files={"clip_file": (f"C:\\private\\clip.{suffix.upper()}", media[suffix].read_bytes(), "application/octet-stream")})
    assert response.status_code == 202
    token = response.json()["search_token"]
    restored = stack.client.get("/api/search/clip", headers={"Authorization": f"Bearer {token}"}).json()
    assert restored["status"] == "completed"
    assert restored["input_type"] == "upload"
    assert restored["clip_filename"] == f"clip.{suffix.upper()}"
    assert restored["tiktok_url"] is None
    assert len(stack.matcher.calls) == 1
    assert 0.9 <= stack.matcher.calls[0][2]["query_duration_seconds"] <= 1.1
    assert list(stack.store.directory.glob("*." + suffix)) == []
    assert str(stack.store.directory) not in json.dumps(restored)
    processor = QueryPreprocessor(str(media[suffix].parent / f"wav-{suffix}"))
    prepared = processor.prepare(str(media[suffix]))
    with wave.open(prepared) as audio:
        assert (audio.getframerate(), audio.getnchannels(), audio.getsampwidth()) == (8000, 1, 2)
        assert 0.9 <= audio.getnframes() / audio.getframerate() <= 1.1
    processor.cleanup(prepared)


@pytest.mark.parametrize("fixture,code", [("corrupt", "INVALID_SEARCH_INPUT"), ("audio-only", "INVALID_SEARCH_INPUT"), ("no-audio", "INVALID_SEARCH_INPUT"), ("overlong", "INPUT_DURATION_EXCEEDED")])
def test_invalid_real_media_fails_job_and_cleans_original(stack, media, fixture, code):
    response = stack.client.post("/api/search/clip", data={"streamer": "xqc"}, files={"clip_file": ("clip.mp4", media[fixture].read_bytes())})
    assert response.status_code == 202
    restored = stack.client.get("/api/search/clip", headers={"Authorization": f"Bearer {response.json()['search_token']}"}).json()
    assert restored["status"] == "failed"
    assert restored["error"]["code"] == code
    assert str(stack.store.directory) not in restored["error"]["message"]
    assert stack.matcher.calls == []
    assert list(stack.store.directory.glob("*.mp4")) == []


@pytest.mark.parametrize("data,files", [
    ({"streamer": "xqc"}, None),
    ({"streamer": "xqc", "tiktok_url": "https://www.tiktok.com/@u/video/1"}, {"clip_file": ("clip.mp4", b"media")}),
])
def test_exactly_one_source(stack, data, files):
    response = stack.client.post("/api/search/clip", data=data, files=files)
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_SEARCH_INPUT"
    assert stack.jobs.records == {}


def test_duplicate_sources_are_rejected(stack):
    response = stack.client.post("/api/search/clip", data={"streamer": "xqc"}, files=[("clip_file", ("a.mp4", b"a")), ("clip_file", ("b.mp4", b"b"))])
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_SEARCH_INPUT"
    response = stack.client.post("/api/search/clip", data={"streamer": "xqc", "tiktok_url": ["https://www.tiktok.com/@u/video/1", "https://www.tiktok.com/@u/video/2"]})
    assert response.status_code == 400
    assert stack.jobs.records == {}


@pytest.mark.parametrize("filename,content,code", [("bad.exe", b"media", "INVALID_UPLOAD"), ("empty.mp4", b"", "INVALID_UPLOAD")])
def test_request_upload_validation(stack, filename, content, code):
    response = stack.client.post("/api/search/clip", data={"streamer": "xqc"}, files={"clip_file": (filename, content)})
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == code
    assert stack.jobs.records == {}


def test_uploaded_original_limit_and_capacity_release(stack, monkeypatch):
    monkeypatch.setattr("backend.services.uploaded_clip_store.MAX_UPLOAD_BYTES", 4)
    response = stack.client.post("/api/search/clip", data={"streamer": "xqc"}, files={"clip_file": ("clip.mp4", b"12345")})
    assert response.status_code == 413
    assert response.json()["detail"]["code"] == "UPLOAD_TOO_LARGE"
    assert not stack.store._owned
    assert stack.jobs.records == {}


def test_admission_bounds_staged_queued_and_running_originals(stack):
    executor = QueuedExecutor()
    stack.service.executor = executor
    for index in range(MAX_ACTIVE_UPLOADS):
        response = stack.client.post("/api/search/clip", data={"streamer": "xqc"}, files={"clip_file": (f"clip{index}.mp4", b"media")})
        assert response.status_code == 202
    assert len(stack.store._owned) == 8
    response = stack.client.post("/api/search/clip", data={"streamer": "xqc"}, files={"clip_file": ("ninth.mp4", b"media")})
    assert response.status_code == 429
    assert response.json()["detail"]["code"] == "UPLOAD_CAPACITY_EXCEEDED"
    assert len(stack.jobs.records) == 8
    assert executor.futures[0].cancel()
    assert len(stack.store._owned) == 7
    assert stack.jobs.records[1].error_code == "SEARCH_CANCELLED"
    response = stack.client.post("/api/search/clip", data={"streamer": "xqc"}, files={"clip_file": ("replacement.mp4", b"media")})
    assert response.status_code == 202
    for future in executor.futures:
        future.cancel()
    assert not stack.store._owned


@pytest.mark.parametrize("failure", ["insert", "submit", "status", "persist", "manager"])
def test_all_job_failure_paths_release_original(stack, media, failure):
    if failure == "insert":
        stack.jobs.fail_insert = True
    elif failure == "submit":
        stack.service.executor = SimpleNamespace(submit=lambda *args: (_ for _ in ()).throw(RuntimeError("executor closed")))
    elif failure == "status":
        stack.jobs.fail_status = True
    elif failure == "persist":
        stack.jobs.fail_persist = True
    else:
        stack.service.search_manager.search_uploaded_file = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("/private/path ffmpeg stderr"))
    async def submit():
        return await stack.service.create_uploaded_search_job(clip_file=UploadFile(BytesIO(media["mp4"].read_bytes()), filename="clip.mp4"), streamer="xqc", creator_id=1)
    if failure in {"insert", "submit"}:
        with pytest.raises(RuntimeError):
            asyncio.run(submit())
    else:
        asyncio.run(submit())
    assert not stack.store._owned
    assert list(stack.store.directory.glob("*.mp4")) == []
    if failure != "insert":
        assert stack.jobs.records[1].status == "failed"
        assert "private" not in stack.jobs.records[1].error_message


def test_copy_and_upload_close_failures_release_reservation(tmp_path):
    store = UploadedClipStore(tmp_path / "uploads")
    class FailedUpload:
        filename = "clip.mp4"
        count = 0
        async def read(self, size):
            self.count += 1
            if self.count == 1:
                return b"partial"
            raise OSError("client disconnected")
        async def close(self):
            pass
    class FailedClose(UploadFile):
        async def close(self):
            raise OSError("close failed")
    for upload in [FailedUpload(), FailedClose(BytesIO(b"media"), filename="clip.mp4")]:
        with pytest.raises(OSError):
            asyncio.run(store.stage(upload))
        assert not store._owned
        assert list(store.directory.glob("*.mp4")) == []
    store.close()


def test_cleanup_cannot_delete_unowned_file_and_restart_preserves_live_instance(tmp_path):
    root = tmp_path / "uploads"
    code = "\n".join([
        "import asyncio, os, sys", "from io import BytesIO", "from pathlib import Path",
        "from starlette.datastructures import UploadFile",
        "from backend.services.uploaded_clip_store import UploadedClipStore",
        "store = UploadedClipStore(Path(sys.argv[1]))",
        "upload = asyncio.run(store.stage(UploadFile(BytesIO(b'media'), filename='clip.mp4')))",
        "print(upload.path, flush=True)", "sys.stdin.buffer.read(1)", "os._exit(0)",
    ])
    process = subprocess.Popen([sys.executable, "-c", code, str(root)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    path = Path(process.stdout.readline().strip())
    foreign = tmp_path / "foreign.mp4"
    foreign.write_bytes(b"keep")
    try:
        second = UploadedClipStore(root)
        second.cleanup(StagedUpload(foreign, "foreign.mp4", 4))
        assert path.is_file()
        assert foreign.read_bytes() == b"keep"
        process.communicate("x", timeout=10)
        third = UploadedClipStore(root)
        assert not path.exists()
        assert second.directory.is_dir()
        second.close()
        third.close()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)


def test_filename_normalization_and_idempotent_cleanup(tmp_path):
    store = UploadedClipStore(tmp_path / "uploads")
    upload = asyncio.run(store.stage(UploadFile(BytesIO(b"media"), filename="../../private/clip\x00\n.MP4")))
    assert upload.filename == "clip.MP4"
    assert upload.path.name != upload.filename
    store.cleanup(upload)
    store.cleanup(upload)
    store.close()
    store.close()


@pytest.mark.parametrize("headers", [[], [(b"content-length", b"1")]])
def test_chunked_and_dishonest_request_limits_precede_parser(headers):
    invoked = []
    messages = iter([{"type": "http.request", "body": b"123", "more_body": True}, {"type": "http.request", "body": b"456", "more_body": False}])
    sent = []
    async def app(*args):
        invoked.append(True)
    async def receive():
        return next(messages)
    async def send(message):
        sent.append(message)
    asyncio.run(UploadBodyLimitMiddleware(app, max_bytes=5)({"type": "http", "method": "POST", "path": "/api/search/clip", "headers": headers}, receive, send))
    assert invoked == []
    assert sent[0]["status"] == 413
    assert json.loads(sent[1]["body"])["detail"]["code"] == "UPLOAD_TOO_LARGE"


def test_raw_spool_replays_only_after_complete_body():
    received = []
    messages = iter([{"type": "http.request", "body": b"123", "more_body": True}, {"type": "http.request", "body": b"45", "more_body": False}])
    async def receive():
        message = next(messages)
        received.append(message)
        return message
    async def app(scope, replay, send):
        assert len(received) == 2
        assert await replay() == {"type": "http.request", "body": b"12345", "more_body": False}
    asyncio.run(UploadBodyLimitMiddleware(app, max_bytes=5)({"type": "http", "method": "POST", "path": "/api/search/clip", "headers": []}, receive, None))


def test_http_body_boundary_and_encoding(stack):
    response = stack.client.post("/api/search/clip", headers={"Content-Length": str(MAX_MULTIPART_BYTES + 1)}, content=b"tiny")
    assert response.status_code == 413
    assert response.json()["detail"]["code"] == "UPLOAD_TOO_LARGE"
    response = stack.client.post("/api/search/clip", headers={"Content-Encoding": "gzip"}, content=b"tiny")
    assert response.status_code == 415
    assert stack.jobs.records == {}


def test_upload_probe_uses_longest_presented_duration_and_excludes_cover_art(monkeypatch):
    payload = {"format": {"format_name": "mov,mp4,m4a,3gp,3g2,mj2", "duration": "2"}, "streams": [
        {"codec_type": "video", "duration": "1", "disposition": {"attached_pic": 0}},
        {"codec_type": "audio", "duration": "181"},
        {"codec_type": "video", "duration": "0", "disposition": {"attached_pic": 1}},
        {"codec_type": "subtitle", "duration": "9999"},
    ]}
    monkeypatch.setattr("backend.services.media_duration.subprocess.run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(payload)))
    assert probe_uploaded_video("clip.mp4") == 181
    payload["streams"] = payload["streams"][1:]
    with pytest.raises(MediaDurationError):
        probe_uploaded_video("clip.mp4")


@pytest.mark.parametrize("duration", ["NaN", "inf", "-1", "0", "bad"])
def test_invalid_reported_duration_is_rejected(monkeypatch, duration):
    payload = {"format": {"format_name": "matroska,webm", "duration": duration}, "streams": [{"codec_type": "video"}, {"codec_type": "audio"}]}
    monkeypatch.setattr("backend.services.media_duration.subprocess.run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(payload)))
    with pytest.raises(MediaDurationError):
        probe_uploaded_video("clip.webm")


def test_probe_timeout_and_container_mismatch_are_stable(media, monkeypatch, tmp_path):
    mislabeled = tmp_path / "mislabeled.mp4"
    mislabeled.write_bytes(media["webm"].read_bytes())
    with pytest.raises(MediaDurationError):
        probe_uploaded_video(str(mislabeled))
    def timeout(cmd, **kwargs):
        assert kwargs["timeout"] == 10
        assert cmd[cmd.index("-protocol_whitelist") + 1] == "file,pipe"
        raise subprocess.TimeoutExpired(cmd, 10, stderr="/private/server/path")
    monkeypatch.setattr("backend.services.media_duration.subprocess.run", timeout)
    with pytest.raises(MediaDurationError) as exc:
        probe_uploaded_video("clip.mp4")
    assert "private" not in str(exc.value)


@pytest.mark.parametrize("failure", ["returncode", "timeout", "spawn"])
def test_preprocessor_partial_wav_cleanup_and_safe_error(tmp_path, monkeypatch, failure):
    source = tmp_path / "input.mp4"
    source.write_bytes(b"media")
    processor = QueryPreprocessor(str(tmp_path / "wav"))
    def run(cmd, **kwargs):
        assert kwargs["timeout"] == 60
        assert cmd[cmd.index("-protocol_whitelist") + 1] == "file,pipe"
        Path(cmd[-3]).write_bytes(b"partial")
        if failure == "timeout":
            raise subprocess.TimeoutExpired(cmd, 60, stderr="/private/path")
        if failure == "spawn":
            raise OSError("spawn failed")
        return SimpleNamespace(returncode=1, stderr="/private/path")
    monkeypatch.setattr("search.query_preprocessor.subprocess.run", run)
    with pytest.raises((RuntimeError, OSError)) as exc:
        processor.prepare(str(source))
    assert "private" not in str(exc.value)
    assert list(processor.temp_dir.iterdir()) == []


def test_upload_policy_matches_nginx():
    config = (Path(__file__).parents[1] / "nginx.public.conf.template").read_text()
    assert MAX_INGRESS_REQUESTS == 2
    assert MAX_ACTIVE_UPLOADS == 8
    assert MAX_UPLOAD_BYTES == 100 * 1024 * 1024
    assert MAX_MULTIPART_BYTES == 101 * 1024 * 1024
    search_location = config.split("location = /api/search/clip {", 1)[1].split("}", 1)[0]
    assert "client_max_body_size 101m;" in search_location
    assert "proxy_request_buffering off;" in search_location
    assert "proxy_http_version 1.1;" in search_location
    assert '"code":"UPLOAD_TOO_LARGE"' in config
    assert "rate=6r/m" in config
    assert "limit_req zone=public_search burst=2 nodelay;" in config


def test_repository_insertion_and_restoration_keep_upload_metadata():
    calls = []
    class Database:
        model_version = "model"
        preprocessing_version = "preprocess"
        @contextmanager
        def connect(self):
            yield self
        @contextmanager
        def cursor(self):
            yield self
        def execute(self, query, values):
            calls.append((query, values))
        def fetchone(self):
            if "INSERT" in calls[-1][0]:
                return (3,)
            return (3, "queued", "validating", "2026-10-13T00:00:00Z", None, None, None, "xqc", None, None, None, None, None, None, None, None, None, None, "upload", "clip.mp4")
    repository = SearchJobRepository(Database(), None)
    token = create_search_token()
    assert repository.create_uploaded_search_job(search_token=token, clip_filename="clip.mp4", streamer="xqc", creator_id=1) == 3
    assert calls[0][1][3] == "upload"
    assert calls[0][1][9:11] == (None, "clip.mp4")
    record = repository.get_public_search_job(token)
    assert record.input_type == "upload"
    assert record.clip_filename == "clip.mp4"
    assert record.tiktok_url is None


@pytest.mark.parametrize("extra", [
    {"streamer": ["xqc", "xqc"]}, {"streamed_from": ["2026-01-01", "2026-01-02"]},
    {"unknown": "value"}, {"streamer": "x" * 101}, {"tiktok_url": "x" * 2049},
])
def test_unknown_duplicate_and_oversized_string_fields_are_rejected(stack, extra):
    data = {"streamer": "xqc", **extra}
    response = stack.client.post("/api/search/clip", data=data, files={"clip_file": ("clip.mp4", b"media")})
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "INVALID_SEARCH_INPUT"
    assert stack.jobs.records == {}


def test_unknown_file_field_and_parser_limits_are_rejected(stack):
    for files in [[("foreign_file", ("clip.mp4", b"media"))], [("clip_file", ("a.mp4", b"a")), ("foreign_file", ("b.mp4", b"b"))]]:
        response = stack.client.post("/api/search/clip", data={"streamer": "xqc"}, files=files)
        assert response.status_code == 400
        assert response.json()["detail"]["code"] == "INVALID_SEARCH_INPUT"
    assert stack.jobs.records == {}


def test_cancelled_partial_stage_removes_original(tmp_path):
    store = UploadedClipStore(tmp_path / "uploads")
    closed = []
    class Disconnected:
        filename = "clip.mp4"
        count = 0
        async def read(self, size):
            self.count += 1
            if self.count == 1:
                return b"partial"
            raise asyncio.CancelledError()
        async def close(self):
            closed.append(True)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(store.stage(Disconnected()))
    assert not store._owned
    assert closed == [True]
    assert list(store.directory.glob("*.mp4")) == []
    store.close()


def test_cancelled_http_submission_keeps_worker_ownership(stack):
    started = Event()
    release = Event()
    original_insert = stack.jobs.create_uploaded_search_job
    executor = QueuedExecutor()
    stack.service.executor = executor
    def blocked_insert(**kwargs):
        started.set()
        assert release.wait(5)
        return original_insert(**kwargs)
    stack.jobs.create_uploaded_search_job = blocked_insert
    async def disconnect():
        task = asyncio.create_task(stack.service.create_uploaded_search_job(
            clip_file=UploadFile(BytesIO(b"media"), filename="clip.mp4"), streamer="xqc", creator_id=1,
        ))
        try:
            await asyncio.to_thread(started.wait, 5)
            assert started.is_set()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert len(stack.store._owned) == 1
        finally:
            release.set()
        for _ in range(100):
            if executor.futures:
                break
            await asyncio.sleep(0.01)
        assert len(executor.futures) == 1
        assert len(stack.store._owned) == 1
        executor.futures[0].cancel()
    asyncio.run(disconnect())
    assert not stack.store._owned


def test_startup_recovery_failure_closes_upload_store_and_embedder(tmp_path, monkeypatch):
    import backend.apps.public as public
    store = UploadedClipStore(tmp_path / "uploads")
    closed = []
    repositories = SimpleNamespace(search_jobs=Jobs())
    search_stack = SimpleNamespace(search_manager=None, query_embedder=SimpleNamespace(close=lambda: closed.append(True)))
    monkeypatch.setattr(public.bootstrap_shared, "build_repositories", lambda **kwargs: repositories)
    monkeypatch.setattr(public.bootstrap_shared, "build_search_stack", lambda **kwargs: search_stack)
    monkeypatch.setattr(public, "UploadedClipStore", lambda: store)
    def recovery_failure(self):
        raise RuntimeError("recovery failed")
    monkeypatch.setattr(SearchJobService, "fail_incomplete_public_search_jobs", recovery_failure)
    with pytest.raises(RuntimeError, match="recovery failed"):
        with TestClient(public.create_public_app()):
            pass
    assert not store.directory.exists()
    assert closed == [True]


def test_full_original_size_boundary(tmp_path):
    store = UploadedClipStore(tmp_path / "uploads")
    class Generated:
        filename = "clip.mp4"
        def __init__(self, length):
            self.remaining = length
        async def read(self, size):
            chunk = b"x" * min(size, self.remaining)
            self.remaining -= len(chunk)
            return chunk
        async def close(self):
            pass
    upload = asyncio.run(store.stage(Generated(MAX_UPLOAD_BYTES)))
    assert upload.path.stat().st_size == MAX_UPLOAD_BYTES
    store.cleanup(upload)
    with pytest.raises(UploadError) as exc:
        asyncio.run(store.stage(Generated(MAX_UPLOAD_BYTES + 1)))
    assert exc.value.status_code == 413
    assert not store._owned
    assert list(store.directory.glob("*.mp4")) == []
    store.close()


def test_failed_deletion_keeps_reservation_and_remains_reapable(tmp_path, monkeypatch):
    root = tmp_path / "uploads"
    store = UploadedClipStore(root)
    upload = asyncio.run(store.stage(UploadFile(BytesIO(b"media"), filename="clip.mp4")))
    unlink = Path.unlink
    def blocked_unlink(path, *args, **kwargs):
        if path == upload.path:
            raise OSError("file temporarily busy")
        return unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, "unlink", blocked_unlink)
    store.cleanup(upload)
    assert upload.path in store._owned
    reservations = [store._slots.acquire(blocking=False) for _ in range(MAX_ACTIVE_UPLOADS)]
    assert reservations == [True] * 7 + [False]
    for acquired in reservations:
        if acquired:
            store._slots.release()
    store.close()
    assert upload.path.is_file()
    assert (store.directory / ".ready").exists()
    monkeypatch.setattr(Path, "unlink", unlink)
    replacement = UploadedClipStore(root)
    assert not upload.path.exists()
    replacement.close()
