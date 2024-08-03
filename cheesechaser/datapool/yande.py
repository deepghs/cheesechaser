from .base import IncrementIDDataPool

_YANDE_REPO = 'deepghs/yande_full'


class YandeDataPool(IncrementIDDataPool):
    def __init__(self, revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_YANDE_REPO,
            data_revision=revision,
            idx_repo_id=_YANDE_REPO,
            idx_revision=revision,
        )
