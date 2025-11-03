from __future__ import annotations

import typing
from typing import Optional

if typing.TYPE_CHECKING:
    from tuspyserver.file import TusUploadFile

import aiofiles
import aiofiles.os
import json
import logging
import os

from tuspyserver.params import TusUploadParams

logger = logging.getLogger(__name__)


class TusUploadInfo:
    _params: Optional[TusUploadParams]
    _loaded: bool
    file: TusUploadFile

    def __init__(self, file: TusUploadFile, params: Optional[TusUploadParams] = None):
        self.file = file
        self._params = params
        self._loaded = params is not None  # If params provided, consider them loaded
        # create if doesn't exist
        if params and not self.exists:
            self._serialize_sync()

    @property
    def params(self):
        # Only deserialize if we haven't loaded params yet
        # This prevents overwriting in-memory params on every access
        if not self._loaded:
            self._deserialize_sync()
            self._loaded = True
        return self._params

    @params.setter
    def params(self, value):
        self._params = value
        self._loaded = True  # Mark as loaded since we're explicitly setting params
        self._serialize_sync()

    async def update_params(self, value):
        self._params = value
        self._loaded = True
        await self.serialize()

    async def load_params(self):
        if not self._loaded:
            await self.deserialize()
            self._loaded = True
        return self._params

    @property
    def path(self) -> str:
        return os.path.join(self.file.options.files_dir, f"{self.file.uid}.info")

    @property
    def exists(self) -> bool:
        return os.path.exists(self.path)

    def _serialize_sync(self) -> None:
        """
        Synchronous serialize for backwards compatibility.
        Use serialize() async method for non-blocking writes.
        """
        temp_path = f"{self.path}.tmp"
        try:
            with open(temp_path, "w") as f:
                json_string = json.dumps(
                    self._params, indent=4, default=lambda k: k.__dict__
                )
                f.write(json_string)
                f.flush()
                os.fsync(f.fileno())
            os.rename(temp_path, self.path)
        except (IOError, OSError) as io_err:
            logger.error(f"I/O error serializing upload info to {self.path}: {io_err}")
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception as cleanup_err:
                logger.warning(f"Failed to cleanup temp file {temp_path}: {cleanup_err}")
            raise
        except (TypeError, ValueError) as json_err:
            logger.error(f"JSON serialization error for {self.path}: {json_err}")
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception as cleanup_err:
                logger.warning(f"Failed to cleanup temp file {temp_path}: {cleanup_err}")
            raise

    async def serialize(self) -> None:
        """
        Atomically serialize params to info file using aiofiles.

        Uses a temporary file and atomic rename to prevent corruption
        from concurrent writes.
        """
        temp_path = f"{self.path}.tmp"
        try:
            async with aiofiles.open(temp_path, "w") as f:
                json_string = json.dumps(
                    self._params, indent=4, default=lambda k: k.__dict__
                )
                await f.write(json_string)
                await f.flush()
                # Ensure data is written to disk
                os.fsync(f.fileno())

            await aiofiles.os.rename(temp_path, self.path)
        except (IOError, OSError) as io_err:
            logger.error(f"I/O error serializing upload info to {self.path}: {io_err}")
            try:
                if os.path.exists(temp_path):
                    await aiofiles.os.remove(temp_path)
            except Exception as cleanup_err:
                logger.warning(f"Failed to cleanup temp file {temp_path}: {cleanup_err}")
            raise
        except (TypeError, ValueError) as json_err:
            logger.error(f"JSON serialization error for {self.path}: {json_err}")
            try:
                if os.path.exists(temp_path):
                    await aiofiles.os.remove(temp_path)
            except Exception as cleanup_err:
                logger.warning(f"Failed to cleanup temp file {temp_path}: {cleanup_err}")
            raise

    def _deserialize_sync(self) -> Optional[TusUploadParams]:
        if self.exists:
            try:
                with open(self.path, "r") as f:
                    content = f.read().strip()
                    if not content:
                        logger.warning(f"Upload info file {self.path} is empty")
                        return None
                    json_dict = json.loads(content)
                    if json_dict:
                        self._params = TusUploadParams(**json_dict)
                    else:
                        self._params = None
            except json.JSONDecodeError as json_err:
                logger.error(f"Corrupted JSON in upload info file {self.path}: {json_err}")
                self._params = None
            except FileNotFoundError:
                logger.debug(f"Upload info file {self.path} not found during read")
                self._params = None
            except (KeyError, TypeError) as data_err:
                logger.error(f"Invalid data structure in upload info file {self.path}: {data_err}")
                self._params = None
            except (IOError, OSError) as io_err:
                logger.error(f"I/O error reading upload info file {self.path}: {io_err}")
                self._params = None
        else:
            self._params = None
        return self._params

    async def deserialize(self) -> Optional[TusUploadParams]:
        if self.exists:
            try:
                async with aiofiles.open(self.path, "r") as f:
                    content = await f.read()
                    content = content.strip()
                    if not content:
                        logger.warning(f"Upload info file {self.path} is empty")
                        return None
                    json_dict = json.loads(content)
                    if json_dict:  # Only create params if we have valid data
                        self._params = TusUploadParams(**json_dict)
                    else:
                        self._params = None
            except json.JSONDecodeError as json_err:
                # Corrupted JSON file
                logger.error(f"Corrupted JSON in upload info file {self.path}: {json_err}")
                self._params = None
            except FileNotFoundError:
                logger.debug(f"Upload info file {self.path} not found during read")
                self._params = None
            except (KeyError, TypeError) as data_err:
                logger.error(f"Invalid data structure in upload info file {self.path}: {data_err}")
                self._params = None
            except (IOError, OSError) as io_err:
                logger.error(f"I/O error reading upload info file {self.path}: {io_err}")
                self._params = None
        else:
            self._params = None

        return self._params
