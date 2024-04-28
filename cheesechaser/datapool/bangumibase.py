from .base import IncrementIDDataPool

_BANGUMIBASE_REPO = 'deepghs/bangumibase_full'


class BangumiBaseDataPool(IncrementIDDataPool):
    def __init__(self, revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_BANGUMIBASE_REPO,
            data_revision=revision,
            idx_repo_id=_BANGUMIBASE_REPO,
            idx_revision=revision,
        )
