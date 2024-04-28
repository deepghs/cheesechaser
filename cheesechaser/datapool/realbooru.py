from .base import IncrementIDDataPool

_REALBOORU_REPO = 'deepghs/realbooru_full'


class RealbooruDataPool(IncrementIDDataPool):
    def __init__(self, revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_REALBOORU_REPO,
            data_revision=revision,
            idx_repo_id=_REALBOORU_REPO,
            idx_revision=revision,
        )
