from .base import IncrementIDDataPool

_FANCAPS_REPO = 'deepghs/fancaps_full'


class FancapsDataPool(IncrementIDDataPool):
    def __init__(self, revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_FANCAPS_REPO,
            data_revision=revision,
            idx_repo_id=_FANCAPS_REPO,
            idx_revision=revision,
        )
