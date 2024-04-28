from .base import IncrementIDDataPool

_KONACHAN_REPO = 'deepghs/konachan_full'


class KonachanDataPool(IncrementIDDataPool):
    def __init__(self, revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_KONACHAN_REPO,
            data_revision=revision,
            idx_repo_id=_KONACHAN_REPO,
            idx_revision=revision,
        )
