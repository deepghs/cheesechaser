from .image import ImageOnlyDataPool

_REALBOORU_REPO = 'deepghs/realbooru_full'


class RealbooruDataPool(ImageOnlyDataPool):
    def __init__(self, revision: str = 'main'):
        ImageOnlyDataPool.__init__(
            self,
            data_repo_id=_REALBOORU_REPO,
            data_revision=revision,
            idx_repo_id=_REALBOORU_REPO,
            idx_revision=revision,
        )
