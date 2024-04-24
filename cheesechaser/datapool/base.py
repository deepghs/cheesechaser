import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from queue import Queue, Full
from threading import Thread, Event
from typing import List, Iterable, ContextManager, Tuple

from hbutils.system import TemporaryDirectory
from hfutils.index import hf_tar_list_files, hf_tar_file_download
from huggingface_hub.utils import EntryNotFoundError, RepositoryNotFoundError
from tqdm import tqdm


@dataclass
class DataLocation:
    tar_file: str
    filename: str


def _n_path(path):
    """
    Normalize a file path.

    :param path: The file path to normalize.
    :type path: str
    :return: The normalized file path.
    :rtype: str
    """
    return os.path.normpath(os.path.join('/', path))


class InvalidResourceDataError(Exception):
    pass


class ResourceNotFoundError(InvalidResourceDataError):
    pass


class FileUnrecognizableError(Exception):
    pass


class GenericDataPool:
    def __init__(self, data_repo_id: str, data_revision: str = 'main',
                 idx_repo_id: str = None, idx_revision: str = 'main'):
        self.data_repo_id = data_repo_id
        self.data_revision = data_revision

        self.idx_repo_id = idx_repo_id or data_repo_id
        self.idx_revision = idx_revision

        self._tar_infos = {}

    def _file_to_resource_id(self, tar_file: str, body: str):
        raise NotImplementedError

    def _make_tar_info(self, tar_file: str, force: bool = False):
        key = _n_path(tar_file)
        if force or key not in self._tar_infos:
            data = {}
            for file in hf_tar_list_files(
                    repo_id=self.data_repo_id,
                    repo_type='dataset',
                    archive_in_repo=tar_file,
                    revision=self.data_revision,

                    idx_repo_id=self.idx_repo_id,
                    idx_repo_type='dataset',
                    idx_revision=self.idx_revision,
            ):
                try:
                    resource_id = self._file_to_resource_id(tar_file, file)
                except FileUnrecognizableError:
                    continue
                if resource_id not in data:
                    data[resource_id] = []
                data[resource_id].append(file)
            self._tar_infos[key] = data

        return self._tar_infos[key]

    def _request_possible_archives(self, resource_id) -> List[str]:
        raise NotImplementedError

    def _request_resource_by_id(self, resource_id) -> List[DataLocation]:
        for archive_file in self._request_possible_archives(resource_id):
            try:
                info = self._make_tar_info(archive_file, force=False)
            except (EntryNotFoundError, RepositoryNotFoundError):
                # no information found, skipped
                continue

            if resource_id in info:
                return [
                    DataLocation(tar_file=archive_file, filename=file)
                    for file in info[resource_id]
                ]
        else:
            return []

    @contextmanager
    def _mock_resource(self, resource_id) -> ContextManager[str]:
        with TemporaryDirectory() as td:
            for location in self._request_resource_by_id(resource_id):
                dst_filename = os.path.join(td, os.path.basename(location.filename))
                hf_tar_file_download(
                    repo_id=self.data_repo_id,
                    repo_type='dataset',
                    archive_in_repo=location.tar_file,
                    file_in_archive=location.filename,
                    local_file=dst_filename,
                    revision=self.data_revision,

                    idx_repo_id=self.idx_repo_id,
                    idx_repo_type='dataset',
                    idx_revision=self.idx_revision,
                )
            yield td

    def batch_download_to_directory(self, resource_ids, dst_dir: str, max_workers: int = 12):
        pg_res = tqdm(resource_ids, desc='Batch Downloading')
        pg_downloaded = tqdm(desc='Files Downloaded')

        def _func(resource_id):
            try:
                with self._mock_resource(resource_id) as td:
                    copied = False
                    for root, dirs, files in os.walk(td):
                        for file in files:
                            src_file = os.path.abspath(os.path.join(root, file))
                            dst_file = os.path.join(dst_dir, os.path.relpath(src_file, td))
                            if os.path.dirname(dst_file):
                                os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                            shutil.copyfile(src_file, dst_file)
                            pg_downloaded.update()
                            copied = True

                    if not copied:
                        logging.warning(f'No files found for resource {resource_id!r}.')

            except Exception as err:
                logging.error(f'Error occurred when downloading resource {resource_id!r} - {err!r}')
            finally:
                pg_res.update()

        tp = ThreadPoolExecutor(max_workers=max_workers)
        for rid in resource_ids:
            tp.submit(_func, rid)

        tp.shutdown(wait=True)

    def retrieve_resource_data(self, resource_id):
        raise NotImplementedError

    def batch_retrieve_resource_queue(self, resource_ids, max_workers: int = 12) -> Tuple[Queue, Event]:
        pg = tqdm(resource_ids, desc='Batch Retrieving')
        queue = Queue(maxsize=max_workers * 3)
        is_stopped = Event()

        def _func(resource_id):
            if is_stopped.is_set():
                return

            try:
                try:
                    data = self.retrieve_resource_data(resource_id)
                except ResourceNotFoundError:
                    logging.warning(f'Resource {resource_id!r} not found.')
                    return
                except InvalidResourceDataError as err:
                    logging.warning(f'Resource {resource_id!r} is invalid - {err}.')
                    return
                finally:
                    pg.update()

                while True:
                    try:
                        queue.put(data, block=True, timeout=1.0)
                    except Full:
                        if is_stopped.is_set():
                            break
                        continue
                    else:
                        break

            except Exception as err:
                logging.error(f'Error occurred when retrieving resource {resource_id!r} - {err!r}')

        def _productor():
            tp = ThreadPoolExecutor(max_workers=max_workers)
            for rid in resource_ids:
                if is_stopped.is_set():
                    break
                tp.submit(_func, rid)

            tp.shutdown(wait=True)
            is_stopped.set()

        t_productor = Thread(target=_productor)
        t_productor.start()
        return queue, is_stopped


def id_modulo_cut(id_text: str):
    id_text = id_text[::-1]
    data = []
    for i in range(0, len(id_text), 3):
        data.append(id_text[i:i + 3][::-1])
    return data[::-1]


class IncrementIDDataPool(GenericDataPool):
    def __init__(self, data_repo_id: str, data_revision: str = 'main',
                 idx_repo_id: str = None, idx_revision: str = 'main',
                 base_level: int = 3, base_dir: str = 'images'):
        GenericDataPool.__init__(self, data_repo_id, data_revision, idx_repo_id, idx_revision)
        self.base_level = base_level
        self.base_dir = base_dir

    def _file_to_resource_id(self, tar_file: str, filename: str):
        try:
            body, _ = os.path.splitext(os.path.basename(filename))
            return int(body)
        except (ValueError, TypeError):
            raise FileUnrecognizableError

    def _request_possible_archives(self, resource_id) -> Iterable[str]:
        modulo = resource_id % (10 ** self.base_level)
        modulo_str = str(modulo)
        if len(modulo_str) < self.base_level:
            modulo_str = '0' * (self.base_level - len(modulo_str)) + modulo_str

        modulo_segments = id_modulo_cut(modulo_str)
        modulo_segments[-1] = f'0{modulo_segments[-1]}'
        return [f'{self.base_dir}/{"/".join(modulo_segments)}.tar']
