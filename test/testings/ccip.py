import json
from functools import lru_cache
from typing import Dict

import numpy as np
from huggingface_hub import hf_hub_download
from imgutils.data import ImageTyping
from imgutils.metrics import ccip_extract_feature, ccip_batch_same


@lru_cache()
def _get_character_dict() -> Dict[str, dict]:
    with open(hf_hub_download(
            repo_id='deepghs/character_index',
            repo_type='dataset',
            filename='characters.json'
    ), 'r') as f:
        return {item['tag']: item for item in json.load(f)}


@lru_cache()
def _get_character_embs(character: str) -> np.ndarray:
    meta_info = _get_character_dict()[character]
    embs = np.load(hf_hub_download(
        repo_id='deepghs/character_index',
        repo_type='dataset',
        filename=f'{meta_info["hprefix"]}/{meta_info["short_tag"]}/feat.npy'
    ))
    return embs


def is_character_ratio(image: ImageTyping, character: str) -> float:
    embedding = ccip_extract_feature(image)
    embs = _get_character_embs(character)
    return ccip_batch_same([embedding, *embs])[0][1:].mean().item()
