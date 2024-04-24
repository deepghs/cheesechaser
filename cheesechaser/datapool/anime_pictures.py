from .image import ImageOnlyDataPool

_ANIME_PICTURES_REPO = 'deepghs/anime_pictures_full'


class AnimePicturesDataPool(ImageOnlyDataPool):
    def __init__(self, revision: str = 'main'):
        ImageOnlyDataPool.__init__(
            self,
            data_repo_id=_ANIME_PICTURES_REPO,
            data_revision=revision,
            idx_repo_id=_ANIME_PICTURES_REPO,
            idx_revision=revision,
        )
