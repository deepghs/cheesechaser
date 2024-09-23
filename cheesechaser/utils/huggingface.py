"""
This module provides utility functions for interacting with the Hugging Face Hub.

It includes functions to retrieve the Hugging Face API token, create API clients,
and access the Hugging Face file system. These utilities are designed to be
efficient through the use of caching mechanisms.
"""

import os
from functools import lru_cache
from typing import Optional

from huggingface_hub import HfApi, HfFileSystem


@lru_cache()
def get_hf_token() -> Optional[str]:
    """
    Retrieve the Hugging Face API token from the environment variables.

    This function is cached to avoid repeated environment variable lookups.

    :return: The Hugging Face API token if set, otherwise None.
    :rtype: Optional[str]

    :usage:
        >>> token = get_hf_token()
        >>> if token:
        >>>     print("Token found")
        >>> else:
        >>>     print("Token not set in environment variables")
    """
    return os.environ.get('HF_TOKEN')


@lru_cache()
def get_hf_client() -> HfApi:
    """
    Create and return a Hugging Face API client.

    This function is cached to reuse the same client instance across multiple calls.
    The client is initialized with the API token retrieved from get_hf_token().

    :return: An instance of the Hugging Face API client.
    :rtype: HfApi

    :usage:
        >>> client = get_hf_client()
        >>> # Use the client to interact with the Hugging Face Hub
        >>> models = client.list_models()
    """
    return HfApi(token=get_hf_token())


@lru_cache()
def get_hf_fs() -> HfFileSystem:
    """
    Create and return a Hugging Face file system instance.

    This function is cached to reuse the same file system instance across multiple calls.
    The file system is initialized with the API token retrieved from get_hf_token().

    :return: An instance of the Hugging Face file system.
    :rtype: HfFileSystem

    :usage:
        >>> fs = get_hf_fs()
        >>> # Use the file system to interact with files on the Hugging Face Hub
        >>> files = fs.ls('username/repo_name')
    """
    return HfFileSystem(token=get_hf_token())
