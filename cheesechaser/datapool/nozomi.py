from .base import IncrementIDDataPool

_NOZOMI_REPO = 'deepghs/nozomi_standalone_full'


class NozomiDataPool(IncrementIDDataPool):
    def __init__(self, revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_NOZOMI_REPO,
            data_revision=revision,
            idx_repo_id=_NOZOMI_REPO,
            idx_revision=revision,
        )
