from .base import IncrementIDDataPool

_GELBOORU_REPO = 'deepghs/bangumibase_full'


class GelbooruDataPool(IncrementIDDataPool):
    def __init__(self, revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_GELBOORU_REPO,
            data_revision=revision,
            idx_repo_id=_GELBOORU_REPO,
            idx_revision=revision,
            base_level=4,
        )


_GELBOORU_WEBP_REPO = 'deepghs/gelbooru-webp-4Mpixel'


class GelbooruWebpDataPool(IncrementIDDataPool):
    def __init__(self, revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_GELBOORU_WEBP_REPO,
            data_revision=revision,
            idx_repo_id=_GELBOORU_WEBP_REPO,
            idx_revision=revision,
            base_level=3,
        )
