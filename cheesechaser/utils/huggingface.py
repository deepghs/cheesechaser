import os
from functools import lru_cache
from typing import Optional

from huggingface_hub import HfApi, HfFileSystem


@lru_cache()
def get_hf_token() -> Optional[str]:
    return os.environ.get('HF_TOKEN')


@lru_cache()
def get_hf_client() -> HfApi:
    return HfApi(token=get_hf_token())


@lru_cache()
def get_hf_fs() -> HfFileSystem:
    return HfFileSystem(token=get_hf_token())
