from .base import IncrementIDDataPool

_CIVITAI_REPO = 'deepghs/civitai_full'


class CivitaiDataPool(IncrementIDDataPool):
    def __init__(self, revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_CIVITAI_REPO,
            data_revision=revision,
            idx_repo_id=_CIVITAI_REPO,
            idx_revision=revision,
            base_level=4,
        )
