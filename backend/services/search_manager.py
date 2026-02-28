import os
import shutil
import uuid

from fastapi import UploadFile

from backend.services.remote_clip_downloader import RemoteClipDownloader
from search.search_service import SearchService


class SearchInputError(Exception):
    pass


class SearchManager:
    def __init__(
        self,
        search_service: SearchService,
        upload_temp_dir: str,
        remote_downloader: RemoteClipDownloader,
    ):
        self.search_service = search_service
        self.upload_temp_dir = upload_temp_dir
        self.remote_downloader = remote_downloader
        os.makedirs(self.upload_temp_dir, exist_ok=True)

    def search_upload(self, file: UploadFile):
        if not file.filename:
            raise SearchInputError("Uploaded file must have a filename")

        suffix = os.path.splitext(file.filename)[1] or ".bin"
        temp_path = os.path.join(self.upload_temp_dir, f"upload_{uuid.uuid4().hex}{suffix}")

        try:
            with open(temp_path, "wb") as out:
                shutil.copyfileobj(file.file, out)

            if os.path.getsize(temp_path) == 0:
                raise SearchInputError("Uploaded file is empty")

            return self._search_local_file(temp_path)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def search_tiktok_url(self, url: str):
        downloaded_path = ""
        try:
            result = self.remote_downloader.download_tiktok(url)
            downloaded_path = result.path
            return self._search_local_file(downloaded_path)
        finally:
            if downloaded_path:
                self.remote_downloader.cleanup(downloaded_path)

    def _search_local_file(self, path: str):
        return self.search_service.search_file(path)
