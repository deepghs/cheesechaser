from .image import ImageOnlyDataPool

_KONACHAN_REPO = 'deepghs/konachan_full'


class KonachanDataPool(ImageOnlyDataPool):
    def __init__(self, revision: str = 'main'):
        ImageOnlyDataPool.__init__(
            self,
            data_repo_id=_KONACHAN_REPO,
            data_revision=revision,
            idx_repo_id=_KONACHAN_REPO,
            idx_revision=revision,
        )
