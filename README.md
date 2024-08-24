# cheesechaser

[![PyPI](https://img.shields.io/pypi/v/cheesechaser)](https://pypi.org/project/cheesechaser/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/cheesechaser)
![Loc](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/narugo1992/eedf334ff9d7ff02e7ec9535e43a1faa/raw/loc.json)
![Comments](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/narugo1992/eedf334ff9d7ff02e7ec9535e43a1faa/raw/comments.json)

[![Code Test](https://github.com/deepghs/cheesechaser/workflows/Code%20Test/badge.svg)](https://github.com/deepghs/cheesechaser/actions?query=workflow%3A%22Code+Test%22)
[![Package Release](https://github.com/deepghs/cheesechaser/workflows/Package%20Release/badge.svg)](https://github.com/deepghs/cheesechaser/actions?query=workflow%3A%22Package+Release%22)
[![codecov](https://codecov.io/gh/deepghs/cheesechaser/branch/main/graph/badge.svg?token=XJVDP4EFAT)](https://codecov.io/gh/deepghs/cheesechaser)

[![Discord](https://img.shields.io/discord/1157587327879745558?style=social&logo=discord&link=https%3A%2F%2Fdiscord.gg%2FTwdHJ42N72)](https://discord.gg/TwdHJ42N72)
![GitHub Org's stars](https://img.shields.io/github/stars/deepghs)
[![GitHub stars](https://img.shields.io/github/stars/deepghs/cheesechaser)](https://github.com/deepghs/cheesechaser/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/deepghs/cheesechaser)](https://github.com/deepghs/cheesechaser/network)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/deepghs/cheesechaser)
[![GitHub issues](https://img.shields.io/github/issues/deepghs/cheesechaser)](https://github.com/deepghs/cheesechaser/issues)
[![GitHub pulls](https://img.shields.io/github/issues-pr/deepghs/cheesechaser)](https://github.com/deepghs/cheesechaser/pulls)
[![Contributors](https://img.shields.io/github/contributors/deepghs/cheesechaser)](https://github.com/deepghs/cheesechaser/graphs/contributors)
[![GitHub license](https://img.shields.io/github/license/deepghs/cheesechaser)](https://github.com/deepghs/cheesechaser/blob/master/LICENSE)

Swiftly get tons of images from indexed tars on Huggingface

## Installation

```shell
pip install cheesechaser
```

## How this library works

This library is based on the mirror datasets on huggingface.

For the Gelbooru mirror dataset repository, such
as [deepghs/gelbooru_full](https://huggingface.co/datasets/deepghs/gelbooru_full), each data packet includes a tar
archive file and a corresponding JSON index file. The JSON index file contains detailed information about the files
within the tar archive, including file size, offset, and file fingerprint.

The files in this dataset repository are organized according to a fixed pattern based on their IDs. For example, a file
with the ID 114514 will have a modulus result of 4514 when divided by 10000. Consequently, it is stored
in `images/4/0514.tar`.

Utilizing the quick download feature
from [hfutils.index](https://deepghs.github.io/hfutils/main/api_doc/index/index.html), users can instantly access
individual files. Since the download service is provided through Huggingface's LFS service and not the original website
or an image CDN, there is no risk of IP or account blocking. **The only limitations to your download speed are your
network bandwidth and disk read/write speeds.**

This efficient system ensures a seamless and reliable access to the dataset without any restrictions.

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

* Danbooru With Tags Query

```python
from cheesechaser.datapool import DanbooruNewestDataPool
from cheesechaser.query import DanbooruIdQuery

pool = DanbooruNewestDataPool()
my_waifu_ids = DanbooruIdQuery(['surtr_(arknights)', 'solo'])

# download danbooru images with surtr+solo, to directory /data/exp2_surtr
pool.batch_download_to_directory(
    resource_ids=my_waifu_ids,
    dst_dir='/data/exp2_surtr',
    max_workers=12,
)
```

* Konachan (Gated dataset, you should be granted first and set `HF_TOKEN` environment variable)

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

* `RealbooruDataPool` (Gated Dataset)
* `ThreedbooruDataPool` (Gated Dataset)
* `FancapsDataPool` (Gated Dataset)
* `BangumiBaseDataPool` (Gated Dataset)
* `AnimePicturesDataPool` (Gated Dataset)
* `KonachanDataPool` (Gated Dataset)
* `YandeDataPool` (Gated Dataset)
* `ZerochanDataPool` (Gated Dataset)
* `GelbooruDataPool` and `GelbooruWebpDataPool` (Gated Dataset)
* `DanbooruNewestDataPool` and `DanbooruNewestWebpDataPool`

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
