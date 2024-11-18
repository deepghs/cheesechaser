"""
This module provides functionality for managing and downloading resources from a data pool,
particularly focused on Hugging Face-based datasets. It includes classes for handling data locations,
managing data pools, and specific implementations for incremental ID-based data pools.

The module offers features such as:

- Normalized path handling
- Custom exception classes for resource-related errors
- A generic DataPool class with batch downloading capabilities
- A HuggingFace-based data pool implementation
- An incremental ID-based data pool implementation

Key components:

- DataLocation: Represents the location of a file within a tar archive
- DataPool: Abstract base class for data pool operations
- HfBasedDataPool: Implementation of DataPool for Hugging Face datasets
- IncrementIDDataPool: Specialized implementation for incremental ID-based datasets

This module is useful for efficiently managing and retrieving resources from large datasets,
especially those hosted on Hugging Face.
"""

import json
import logging
import os
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from typing import List, Iterable, ContextManager, Tuple, Any, Optional, Union

from hbutils.system import TemporaryDirectory
from hfutils.index import hf_tar_list_files, hf_tar_file_download
from huggingface_hub.utils import EntryNotFoundError, RepositoryNotFoundError
from tqdm import tqdm


@dataclass
class DataLocation:
    """
    Represents the location of a file within a tar archive.

    :param resource_id: The unique identifier for the resource.
    :type resource_id: int
    :param tar_file: The name of the tar file containing the data.
    :type tar_file: str
    :param filename: The name of the file within the tar archive.
    :type filename: str
    """
    resource_id: int
    tar_file: str
    filename: str


def _n_path(path):
    """
    Normalize a file path.

    This function takes a file path and normalizes it by joining it with the root directory
    and then normalizing the resulting path. It's useful for ensuring consistent path formats
    across different operating systems.

    :param path: The file path to normalize.
    :type path: str
    :return: The normalized file path.
    :rtype: str

    :example:
    >>> _n_path('dir1/dir2/../file.txt')
    '/dir1/file.txt'
    """
    return os.path.normpath(os.path.join('/', path))


class InvalidResourceDataError(Exception):
    """
    Base exception for invalid resource data.

    This exception is raised when there's an issue with the resource data that prevents
    it from being processed or used correctly.
    """
    pass


class ResourceNotFoundError(InvalidResourceDataError):
    """
    Exception raised when a requested resource is not found.

    This exception is typically raised when attempting to access or download a resource
    that does not exist in the data pool.
    """
    pass


class FileUnrecognizableError(Exception):
    """
    Exception raised when a file cannot be recognized or processed.

    This exception is used when the system encounters a file that it cannot parse or
    interpret according to the expected format or structure.
    """
    pass


class DataPool:
    """
    Abstract base class for data pool operations.

    This class defines the interface for data pool operations and provides a method
    for batch downloading resources to a directory. Subclasses should implement the
    `mock_resource` method to provide specific functionality for different types of data pools.

    The DataPool class is designed to be extended for various data sources and storage mechanisms.
    """

    @contextmanager
    def mock_resource(self, resource_id, resource_info, silent: bool = False) -> ContextManager[Tuple[str, Any]]:
        """
        Context manager to mock a resource.

        This method should be implemented by subclasses to provide a way to temporarily
        access a resource. It's typically used to download or generate a temporary copy
        of the resource for processing.

        :param resource_id: The ID of the resource to mock.
        :param resource_info: Additional information about the resource.
        :param silent: If True, suppresses progress bar of each standalone files during the mocking process.
        :type silent: bool
        :return: A tuple containing the path to the mocked resource and its info.
        :raises NotImplementedError: If not implemented by a subclass.
        """
        raise NotImplementedError  # pragma: no cover

    def batch_download_to_directory(self, resource_ids, dst_dir: str, max_workers: int = 12,
                                    save_metainfo: bool = True, metainfo_fmt: str = '{resource_id}_metainfo.json',
                                    max_downloads: Optional[int] = None, silent: bool = False):
        """
        Download multiple resources to a directory in parallel.

        This method efficiently downloads a batch of resources to a specified directory,
        optionally saving metadata for each resource. It utilizes a thread pool to parallelize
        downloads for improved performance.

        :param resource_ids: List of resource IDs or tuples of (resource_id, resource_info) to download.
        :type resource_ids: Iterable[Union[str, int, Tuple[str, Any]]]
        :param dst_dir: Destination directory for downloaded files.
        :type dst_dir: str
        :param max_workers: Maximum number of worker threads for parallel downloads. Defaults to 12.
        :type max_workers: int
        :param save_metainfo: Whether to save metadata information for each resource. Defaults to True.
        :type save_metainfo: bool
        :param metainfo_fmt: Format string for metadata filenames. Defaults to '{resource_id}_metainfo.json'.
        :type metainfo_fmt: str
        :param max_downloads: Maximum number of downloads to perform.
                              If None, all resources will be downloaded. Defaults to None.
        :type max_downloads: Optional[int]
        :param silent: If True, suppresses progress bar of each standalone file during the download process.
                       Defaults to False.
        :type silent: bool

        :raises OSError: If there's an issue creating the destination directory or copying files.

        .. note::
            The `max_downloads` argument provides a rough limit on the download count.
            Due to parallel processing, the actual number of downloads may slightly exceed this limit.

        :example:
        >>> data_pool = SomeDataPoolImplementation()
        >>> data_pool.batch_download_to_directory([111, 'resource1', 'resource2'], '/path/to/destination')
        """
        pg_res = tqdm(resource_ids, desc='Batch Downloading')
        pg_file_download = tqdm(desc='Files Downloaded')
        pg_download = tqdm(desc='Download Count', total=max_downloads)
        os.makedirs(dst_dir, exist_ok=True)

        is_completed = threading.Event()
        downloaded_count = 0

        def _func(resource_id, resource_info):
            nonlocal downloaded_count
            if is_completed.is_set():
                return

            try:
                with self.mock_resource(resource_id, resource_info, silent=silent) as (td, resource_info):
                    copied = False
                    for root, dirs, files in os.walk(td):
                        for file in files:
                            src_file = os.path.abspath(os.path.join(root, file))
                            dst_file = os.path.join(dst_dir, os.path.relpath(src_file, td))
                            if os.path.dirname(dst_file):
                                os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                            shutil.copyfile(src_file, dst_file)
                            if save_metainfo and resource_info is not None:
                                meta_file = os.path.join(td, metainfo_fmt.format(resource_id=resource_id))
                                with open(meta_file, 'w') as f:
                                    json.dump(resource_info, f, indent=4, sort_keys=True, ensure_ascii=False)
                            pg_file_download.update()
                            copied = True

                    if not copied:
                        logging.warning(f'No files found for resource {resource_id!r}.')
                    downloaded_count += 1
                    if max_downloads is not None and downloaded_count >= max_downloads:
                        is_completed.set()
                    pg_download.update()
            except ResourceNotFoundError:
                logging.warning(f'Resource {resource_id!r} not found, skipped.')
            except Exception as err:
                logging.exception(f'Error occurred when downloading resource {resource_id!r} - {err!r}')
            finally:
                pg_res.update()

        tp = ThreadPoolExecutor(max_workers=max_workers)
        for ritem in resource_ids:
            if isinstance(ritem, tuple):
                rid, rinfo = ritem
            else:
                rid, rinfo = ritem, None
            tp.submit(_func, rid, rinfo)

        tp.shutdown(wait=True)


class HfBasedDataPool(DataPool):
    """
    Implementation of DataPool for Hugging Face datasets.

    This class provides methods to interact with and download resources from Hugging Face datasets.
    It handles the complexities of working with Hugging Face's repository structure and file organization.

    :param data_repo_id: The ID of the Hugging Face dataset repository.
    :type data_repo_id: str
    :param data_revision: The revision of the dataset to use.
    :type data_revision: str
    :param idx_repo_id: The ID of the index repository (defaults to data_repo_id if not provided).
    :type idx_repo_id: str
    :param idx_revision: The revision of the index to use.
    :type idx_revision: str
    :param hf_token: Optional Hugging Face authentication token.
    :type hf_token: Optional[str]

    :example:
    >>> data_pool = HfBasedDataPool('username/dataset', data_revision='main')
    >>> with data_pool.mock_resource('resource1', None) as (path, info):
    ...     # Work with the resource at 'path'
    ...     pass
    """

    def __init__(self, data_repo_id: str, data_revision: str = 'main',
                 idx_repo_id: str = None, idx_revision: str = 'main', hf_token: Optional[str] = None):
        self.data_repo_id = data_repo_id
        self.data_revision = data_revision

        self.idx_repo_id = idx_repo_id or data_repo_id
        self.idx_revision = idx_revision

        self._hf_token = hf_token
        self._tar_infos = {}

    def _file_to_resource_id(self, tar_file: str, body: str):
        """
        Convert a file path to a resource ID.

        This method should be implemented by subclasses to define how file paths are mapped to resource IDs.

        :param tar_file: The name of the tar file containing the resource.
        :type tar_file: str
        :param body: The file path within the tar file.
        :type body: str
        :return: The resource ID.
        :raises NotImplementedError: If not implemented by a subclass.
        """
        raise NotImplementedError  # pragma: no cover

    def _make_tar_info(self, tar_file: str, force: bool = False):
        """
        Create or retrieve information about a tar file.

        This method lists the files in a tar archive and maps them to resource IDs.
        It caches the information to avoid repeated API calls.

        :param tar_file: The name of the tar file.
        :type tar_file: str
        :param force: Whether to force a refresh of the information.
        :type force: bool
        :return: A dictionary mapping resource IDs to lists of file paths.
        :rtype: dict

        :raises EntryNotFoundError: If the specified tar file is not found in the repository.
        :raises RepositoryNotFoundError: If the specified repository is not found.
        """
        key = _n_path(tar_file)
        if force or key not in self._tar_infos:
            data = {}
            all_files = hf_tar_list_files(
                repo_id=self.data_repo_id,
                repo_type='dataset',
                archive_in_repo=tar_file,
                revision=self.data_revision,

                idx_repo_id=self.idx_repo_id,
                idx_repo_type='dataset',
                idx_revision=self.idx_revision,
                hf_token=self._hf_token,
            )

            for file in all_files:
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
        """
        Get a list of possible archive files for a given resource ID.

        This method should be implemented by subclasses to define how to find potential archives for a resource.

        :param resource_id: The ID of the resource.
        :return: A list of possible archive file names.
        :raises NotImplementedError: If not implemented by a subclass.
        """
        raise NotImplementedError  # pragma: no cover

    def _request_resource_by_id(self, resource_id) -> List[DataLocation]:
        """
        Request resource locations for a given resource ID.

        This method searches for the resource in possible archives and returns its locations.

        :param resource_id: The ID of the resource to request.
        :return: A list of DataLocation objects representing the resource's locations.
        :raises ResourceNotFoundError: If the resource is not found in any archive.

        :example:
        >>> data_pool = HfBasedDataPool('username/dataset')
        >>> locations = data_pool._request_resource_by_id('resource1')
        >>> print(locations[0].tar_file, locations[0].filename)
        """
        for archive_file in self._request_possible_archives(resource_id):
            try:
                info = self._make_tar_info(archive_file, force=False)
            except (EntryNotFoundError, RepositoryNotFoundError):
                # no information found, skipped
                continue

            if resource_id in info:
                return [
                    DataLocation(resource_id=resource_id, tar_file=archive_file, filename=file)
                    for file in info[resource_id]
                ]
        else:
            raise ResourceNotFoundError(f'Resource {resource_id!r} not found.')

    def _get_dst_filename(self, location: DataLocation):
        """
        Get the destination filename for a given DataLocation.

        This method determines the filename to use when saving a resource locally.

        :param location: The DataLocation object containing information about the resource.
        :type location: DataLocation
        :return: The filename to use for the local copy of the resource.
        :rtype: str
        """
        return os.path.basename(location.filename)

    @contextmanager
    def mock_resource(self, resource_id, resource_info, silent: bool = False) -> ContextManager[Tuple[str, Any]]:
        """
        Context manager to temporarily access a resource.

        This method downloads the requested resource to a temporary directory and provides access to it.

        :param resource_id: The ID of the resource to mock.
        :param resource_info: Additional information about the resource.
        :param silent: If True, suppresses progress bar of each standalone files during the mocking process.
        :type silent: bool
        :return: A tuple containing the path to the temporary directory and the resource info.
        :raises ResourceNotFoundError: If the resource cannot be found or downloaded.

        :example:
        >>> data_pool = HfBasedDataPool('username/dataset')
        >>> with data_pool.mock_resource('resource1', {'metadata': 'value'}) as (path, info):
        ...     # Work with the resource at 'path'
        ...     print(f"Resource path: {path}")
        ...     print(f"Resource info: {info}")
        """
        with TemporaryDirectory() as td:
            for location in self._request_resource_by_id(resource_id):
                dst_filename = os.path.join(td, self._get_dst_filename(location))
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
                    hf_token=self._hf_token,
                    silent=silent,
                )
            yield td, resource_info


def id_modulo_cut(id_text: str):
    """
    Cut an ID string into segments of 3 characters each, starting from the end.

    This function is used to create a hierarchical structure for IDs, which can be useful
    for organizing files in a directory structure based on their IDs.

    :param id_text: The ID string to cut.
    :type id_text: str
    :return: A list of 3-character segments.
    :rtype: List[str]

    :example:
    >>> id_modulo_cut('123456789')
    ['789', '456', '123']
    """
    id_text = id_text[::-1]
    data = []
    for i in range(0, len(id_text), 3):
        data.append(id_text[i:i + 3][::-1])
    return data[::-1]


class IncrementIDDataPool(HfBasedDataPool):
    """
    A specialized implementation of HfBasedDataPool for incremental ID-based datasets.

    This class is designed to work with datasets where resources are identified by incremental integer IDs
    and are organized in a hierarchical directory structure.

    :param data_repo_id: The ID of the Hugging Face dataset repository.
    :type data_repo_id: str
    :param data_revision: The revision of the dataset to use.
    :type data_revision: str
    :param idx_repo_id: The ID of the index repository (defaults to data_repo_id if not provided).
    :type idx_repo_id: str
    :param idx_revision: The revision of the index to use.
    :type idx_revision: str
    :param base_level: The base level for the hierarchical structure.
    :type base_level: int
    :param base_dir: The base directory for the dataset files.
    :type base_dir: str
    """

    def __init__(self, data_repo_id: str, data_revision: str = 'main',
                 idx_repo_id: str = None, idx_revision: str = 'main',
                 base_level: Union[int, List[int]] = 3, base_dir: str = 'images', hf_token: Optional[str] = None):
        HfBasedDataPool.__init__(
            self,
            data_repo_id=data_repo_id,
            data_revision=data_revision,
            idx_repo_id=idx_repo_id,
            idx_revision=idx_revision,
            hf_token=hf_token,
        )
        self.base_level = base_level
        self.base_dir = base_dir

    def _file_to_resource_id(self, tar_file: str, filename: str):
        """
        Convert a filename to a resource ID.

        This method extracts the resource ID from the filename, assuming it's an integer.

        :param tar_file: The name of the tar file (unused in this implementation).
        :type tar_file: str
        :param filename: The name of the file.
        :type filename: str
        :return: The resource ID as an integer.
        :rtype: int
        :raises FileUnrecognizableError: If the filename cannot be converted to an integer ID.
        """
        try:
            body, _ = os.path.splitext(os.path.basename(filename))
            return int(body)
        except (ValueError, TypeError):
            raise FileUnrecognizableError

    def _request_possible_archives(self, resource_id) -> Iterable[str]:
        """
        Generate possible archive paths for a given resource ID.

        This method creates a hierarchical path structure based on the resource ID.

        :param resource_id: The ID of the resource to request.
        :return: A list of DataLocation objects representing the resource's locations.
        :raises ResourceNotFoundError: If the resource is not found in any archive.
        """
        if isinstance(self.base_level, int):
            base_levels = [self.base_level]
        else:
            base_levels = self.base_level

        archives = []
        for base_level in base_levels:
            modulo = resource_id % (10 ** base_level)
            modulo_str = str(modulo)
            if len(modulo_str) < base_level:
                modulo_str = '0' * (base_level - len(modulo_str)) + modulo_str

            modulo_segments = id_modulo_cut(modulo_str)
            modulo_segments[-1] = f'0{modulo_segments[-1]}'
            archives.append(f'{self.base_dir}/{"/".join(modulo_segments)}.tar')
        return archives
