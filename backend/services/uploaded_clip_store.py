from __future__ import annotations

from dataclasses import dataclass
import getpass
import logging
import os
from pathlib import Path
import shutil
import tempfile
from threading import BoundedSemaphore, Lock
import unicodedata
import uuid

from starlette.datastructures import UploadFile
from starlette.concurrency import run_in_threadpool

from backend.services.media_duration import UPLOAD_CONTAINER_NAMES


MAX_UPLOAD_BYTES = 100 * 1024 * 1024
MAX_MULTIPART_BYTES = 101 * 1024 * 1024
MAX_ACTIVE_UPLOADS = 8
ACCEPTED_SUFFIXES = frozenset(UPLOAD_CONTAINER_NAMES)
logger = logging.getLogger("uvicorn.error")


class UploadError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 400):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class StagedUpload:
    path: Path
    filename: str
    byte_count: int


def _try_lock(handle) -> bool:
    try:
        if os.name == "nt":
            import msvcrt
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError:
        return False


def _unlock(handle) -> None:
    if os.name == "nt":
        import msvcrt
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _display_filename(filename: str | None) -> str:
    name = unicodedata.normalize("NFC", (filename or "").replace("\\", "/").split("/")[-1])
    name = "".join(char for char in name if not unicodedata.category(char).startswith("C")).strip()
    suffix = Path(name).suffix.lower()
    if suffix not in ACCEPTED_SUFFIXES:
        raise UploadError("INVALID_UPLOAD", "Choose an MP4, MOV, M4V, WebM, or MKV video.")
    return name[: 200 - len(suffix)] + suffix if len(name) > 200 else name


class UploadedClipStore:
    def __init__(self, root: Path | None = None):
        owner = str(os.getuid()) if hasattr(os, "getuid") else getpass.getuser()
        self.root = root or Path(tempfile.gettempdir()) / f"vodhunter-uploads-{owner}"
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._reap_orphans()
        self.directory = Path(tempfile.mkdtemp(prefix="instance-", dir=self.root))
        self._owner = (self.directory / ".owner").open("w+b")
        self._owner.write(b"1")
        self._owner.flush()
        if not _try_lock(self._owner):
            self._owner.close()
            shutil.rmtree(self.directory)
            raise RuntimeError("Could not own upload storage")
        (self.directory / ".ready").touch()
        self._slots = BoundedSemaphore(MAX_ACTIVE_UPLOADS)
        self._lock = Lock()
        self._owned: set[Path] = set()
        self._closed = False

    def _reap_orphans(self) -> None:
        for directory in self.root.glob("instance-*"):
            if directory.is_symlink() or not (directory / ".ready").is_file():
                continue
            try:
                with (directory / ".owner").open("r+b") as handle:
                    if not _try_lock(handle):
                        continue
                    try:
                        for item in directory.iterdir():
                            if item.name != ".owner" and item.is_file() and not item.is_symlink():
                                item.unlink()
                    finally:
                        _unlock(handle)
                (directory / ".owner").unlink(missing_ok=True)
                directory.rmdir()
            except OSError:
                logger.exception("Could not clean abandoned upload storage")

    async def stage(self, upload: UploadFile) -> StagedUpload:
        path = None
        reserved = False
        try:
            filename = _display_filename(upload.filename)
            with self._lock:
                if self._closed or not self._slots.acquire(blocking=False):
                    raise UploadError("UPLOAD_CAPACITY_EXCEEDED", "Too many uploaded searches are active. Try again shortly.", 429)
                reserved = True
                path = self.directory / f"{uuid.uuid4().hex}{Path(filename).suffix.lower()}"
                self._owned.add(path)
            count = 0
            with path.open("xb") as output:
                os.chmod(path, 0o600)
                while chunk := await upload.read(1024 * 1024):
                    count += len(chunk)
                    if count > MAX_UPLOAD_BYTES:
                        raise UploadError("UPLOAD_TOO_LARGE", "Video must be 100 MiB or smaller.", 413)
                    await run_in_threadpool(output.write, chunk)
            if not count:
                raise UploadError("INVALID_UPLOAD", "Choose a nonempty video file.")
            return StagedUpload(path=path, filename=filename, byte_count=count)
        except BaseException:
            if reserved and path is not None:
                self.cleanup(StagedUpload(path, "", 0))
            raise
        finally:
            try:
                await upload.close()
            except BaseException:
                if reserved and path is not None:
                    self.cleanup(StagedUpload(path, "", 0))
                raise

    def cleanup(self, upload: StagedUpload) -> None:
        with self._lock:
            if upload.path not in self._owned:
                return
            try:
                upload.path.unlink(missing_ok=True)
            except OSError:
                logger.exception("Could not remove staged upload")
                return
            self._owned.remove(upload.path)
            self._slots.release()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            remaining = list(self._owned)
        for path in remaining:
            self.cleanup(StagedUpload(path, "", 0))
        if self._owned:
            _unlock(self._owner)
            self._owner.close()
            return
        (self.directory / ".ready").unlink(missing_ok=True)
        _unlock(self._owner)
        self._owner.close()
        (self.directory / ".owner").unlink(missing_ok=True)
        self.directory.rmdir()
