from .base import IncrementIDDataPool

_ZEROCHAN_REPO = 'deepghs/zerochan_full'


class ZerochanDataPool(IncrementIDDataPool):
    def __init__(self, revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_ZEROCHAN_REPO,
            data_revision=revision,
            idx_repo_id=_ZEROCHAN_REPO,
            idx_revision=revision,
            base_level=3,
        )


_ZEROCHAN_WEBP_REPO = 'deepghs/zerochan-webp-4Mpixel'


class ZerochanWebpDataPool(IncrementIDDataPool):
    def __init__(self, revision: str = 'main'):
        IncrementIDDataPool.__init__(
            self,
            data_repo_id=_ZEROCHAN_WEBP_REPO,
            data_revision=revision,
            idx_repo_id=_ZEROCHAN_WEBP_REPO,
            idx_revision=revision,
            base_level=3,
        )
