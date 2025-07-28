import hashlib
import shutil
from abc import ABC, abstractmethod
from pathlib import Path

import requests

from cstar.base.gitutils import _checkout, _clone
from cstar.io import SourceData


class Retriever(ABC):
    @abstractmethod
    def read(self, source: SourceData) -> bytes:
        """Retrieve data to memory, if supported"""
        pass

    def save(self, target_dir: Path, source: SourceData) -> Path:
        """
        Save this object to the given directory.

        This method performs common setup ( ensuring the target directory exists)
        and then calls the subclass-defined `_save()` method for the actual save.
        """
        if target_dir.exists():
            if not target_dir.is_dir():
                raise ValueError(
                    f"Cannot save to target_dir={target_dir} (not a directory)"
                )
        else:
            target_dir.mkdir(parents=True)

        savepath = self._save(target_dir=target_dir, source=source)
        return savepath

    @abstractmethod
    def _save(self, target_dir: Path, source: SourceData) -> Path:
        """Retrieve data to a local path"""
        pass


class RemoteFileRetriever(Retriever, ABC):
    def read(self, source: SourceData) -> bytes:
        response = requests.get(source.location, allow_redirects=True)
        response.raise_for_status()
        data = response.content

        return data

    @abstractmethod
    def _save(self, target_dir: Path, source: SourceData) -> Path:
        pass


class RemoteBinaryFileRetriever(RemoteFileRetriever):
    def _save(self, target_dir: Path, source: SourceData) -> Path:
        hash_obj = hashlib.sha256()

        target_path = target_dir / source.filename

        with requests.get(source.location, stream=True, allow_redirects=True) as r:
            r.raise_for_status()
            with open(target_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):  # Download in 8kB chunks
                    if chunk:
                        f.write(chunk)
                        hash_obj.update(chunk)

        # Hash verification if specified in SourceData:
        if source.file_hash:
            actual_hash = hash_obj.hexdigest()
            expected_hash = source.file_hash.lower()

            if actual_hash != expected_hash:
                target_path.unlink(missing_ok=True)  # Remove downloaded file
                raise ValueError(
                    f"Hash mismatch for {source.location} (saved file):\n"
                    f"Expected: {expected_hash}\nActual:   {actual_hash}.\n"
                    f"File deleted for safety."
                )
        return target_path


class RemoteTextFileRetriever(RemoteFileRetriever):
    def _save(self, target_dir: Path, source: SourceData) -> Path:
        data = self.read(source=source)
        target_path = target_dir / source.filename
        with open(target_path, "wb") as f:
            f.write(data)
        return target_path


class LocalFileRetriever(Retriever):
    def read(self, source: SourceData) -> bytes:
        with open(source.location, "rb") as f:
            return f.read()

    def _save(self, target_dir: Path, source: SourceData) -> Path:
        target_path = target_dir / source.filename
        shutil.copy2(src=Path(source.location).resolve(), dst=target_path)
        return target_path


class RemoteRepositoryRetriever(Retriever):
    def read(self, source: SourceData) -> bytes:
        raise NotImplementedError("Cannot 'read' a remote repository to memory")

    def _save(self, target_dir: Path, source: SourceData) -> Path:
        _clone(
            source_repo=source.location,
            local_path=target_dir,
        )
        if source.checkout_target:
            _checkout(
                source_repo=source.location,
                local_path=target_dir,
                checkout_target=source.checkout_target,
            )
        return target_dir
