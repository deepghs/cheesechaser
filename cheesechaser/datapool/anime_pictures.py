from .base import IncrementIDDataPool

_ANIME_PICTURES_REPO = 'deepghs/anime_pictures_full'


class AnimePicturesDataPool(IncrementIDDataPool):
    def __init__(self, revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_ANIME_PICTURES_REPO,
            data_revision=revision,
            idx_repo_id=_ANIME_PICTURES_REPO,
            idx_revision=revision,
        )
