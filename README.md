# cheesechaser

[![PyPI](https://img.shields.io/pypi/v/cheesechaser)](https://pypi.org/project/cheesechaser/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/cheesechaser)
![Loc](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/narugo1992/eedf334ff9d7ff02e7ec9535e43a1faa/raw/loc.json)
![Comments](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/narugo1992/eedf334ff9d7ff02e7ec9535e43a1faa/raw/comments.json)

[![Code Test](https://github.com/deepghs/cheesechaser/workflows/Code%20Test/badge.svg)](https://github.com/deepghs/cheesechaser/actions?query=workflow%3A%22Code+Test%22)
[![Package Release](https://github.com/deepghs/cheesechaser/workflows/Package%20Release/badge.svg)](https://github.com/deepghs/cheesechaser/actions?query=workflow%3A%22Package+Release%22)
[![codecov](https://codecov.io/gh/deepghs/cheesechaser/branch/main/graph/badge.svg?token=XJVDP4EFAT)](https://codecov.io/gh/deepghs/cheesechaser)

![GitHub Org's stars](https://img.shields.io/github/stars/deepghs)
[![GitHub stars](https://img.shields.io/github/stars/deepghs/cheesechaser)](https://github.com/deepghs/cheesechaser/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/deepghs/cheesechaser)](https://github.com/deepghs/cheesechaser/network)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/deepghs/cheesechaser)
[![GitHub issues](https://img.shields.io/github/issues/deepghs/cheesechaser)](https://github.com/deepghs/cheesechaser/issues)
[![GitHub pulls](https://img.shields.io/github/issues-pr/deepghs/cheesechaser)](https://github.com/deepghs/cheesechaser/pulls)
[![Contributors](https://img.shields.io/github/contributors/deepghs/cheesechaser)](https://github.com/deepghs/cheesechaser/graphs/contributors)
[![GitHub license](https://img.shields.io/github/license/deepghs/cheesechaser)](https://github.com/deepghs/cheesechaser/blob/master/LICENSE)

Swiftly get tons of images from indexed tars on Huggingface

(Still WIP ...)

## Installation

```shell
git clone https://github.com/deepghs/cheesechaser.git
cd cheesechaser
pip install -r requirements.txt
```

## How this library works

This library is based on the mirror datasets on huggingface.

## Batch Download Images

* Danbooru

```python
from cheesechaser.datapool import DanbooruNewestDataPool

pool = DanbooruNewestDataPool()

# download danbooru #2010000-2010300, to directory /data/exp2
pool.batch_download_to_directory(
    resource_ids=range(2010000, 2010300),
    dst_dir='/data/exp2',
    max_workers=12,
)
```

* Konachan

```python
from cheesechaser.datapool import KonachanDataPool

pool = KonachanDataPool()

# download konachan #210000-210300, to directory /data/exp2
pool.batch_download_to_directory(
    resource_ids=range(210000, 210300),
    dst_dir='/data/exp2',
    max_workers=12,
)
```

* Civitai (this mirror repository on hf is private for now, you have to use hf token of an authorized account)

```python
from cheesechaser.datapool import CivitaiDataPool

pool = CivitaiDataPool()

# download civitai #7810000-7810300, to directory /data/exp2
# should contain one image and one json metadata file
pool.batch_download_to_directory(
    resource_ids=range(7810000, 7810300),
    dst_dir='/data/exp2',
    max_workers=12,
)
```

More supported:

* `RealbooruDataPool`
* `ThreedbooruDataPool`
* `FancapsDataPool`
* `BangumiBaseDataPool`
* `AnimePicturesDataPool`

## Batch Retrieving Images

```python
from itertools import islice

from cheesechaser.datapool import DanbooruNewestDataPool
from cheesechaser.pipe import SimpleImagePipe, PipeItem

pool = DanbooruNewestDataPool()
pipe = SimpleImagePipe(pool)

# select from danbooru 7349990-7359990
ids = range(7349990, 7359990)
with pipe.batch_retrieve(ids) as session:
    # only need 20 images
    for i, item in enumerate(islice(session, 20)):
        item: PipeItem
        print(i, item)

```
