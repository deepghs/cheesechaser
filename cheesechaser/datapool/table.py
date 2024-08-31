"""
This module provides classes for managing and accessing data pools stored on Hugging Face.

It includes implementations for table-based data pools, allowing efficient retrieval and 
management of data resources stored in archives on Hugging Face repositories. The module 
supports various file formats and provides mechanisms for mapping between resource IDs 
and their corresponding archive locations.
"""

import os.path
from threading import Lock
from typing import List, Optional, Tuple, Dict

import pandas as pd
from hfutils.operate import get_hf_client
from tqdm import tqdm

from .base import HfBasedDataPool, _n_path, FileUnrecognizableError, DataLocation


class TableBasedHfDataPool(HfBasedDataPool):
    """
    A class representing a table-based data pool stored on Hugging Face.

    This class extends HfBasedDataPool to provide functionality for managing data
    that is organized in a tabular format, where each row represents a data item
    stored in an archive file.

    :param data_repo_id: The ID of the Hugging Face repository containing the data.
    :type data_repo_id: str
    :param archive_column: The name of the column containing archive filenames.
    :type archive_column: str
    :param file_in_archive_column: The name of the column containing filenames within archives.
    :type file_in_archive_column: str
    :param id_column: The name of the column containing unique identifiers for each data item.
    :type id_column: str
    :param data_revision: The revision of the data to use (default is 'main').
    :type data_revision: str
    :param mock_use_id: Whether to use the ID as part of the filename when extracting (default is True).
    :type mock_use_id: bool
    :param hf_token: An optional Hugging Face API token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, data_repo_id: str, archive_column: str, file_in_archive_column: str,
                 id_column: str = 'id', data_revision: str = 'main', mock_use_id: bool = True,
                 hf_token: Optional[str] = None):
        HfBasedDataPool.__init__(
            self,
            data_repo_id=data_repo_id,
            data_revision=data_revision,
            idx_repo_id=data_repo_id,
            idx_revision=data_revision,
            hf_token=hf_token,
        )
        self._archive_column = archive_column
        self._file_in_archive_column = file_in_archive_column
        self._id_column = id_column

        self._mock_use_id = mock_use_id
        self._st = None
        self._lock = Lock()

    def _get_dst_filename(self, location: DataLocation) -> str:
        """
        Get the destination filename for a given data location.

        :param location: The data location object.
        :type location: DataLocation
        :return: The destination filename.
        :rtype: str
        """
        if self._mock_use_id:
            _, ext = os.path.splitext(location.filename)
            return f'{location.resource_id}{ext.lower()}'
        else:
            return super()._get_dst_filename(location)

    def _load_df(self) -> pd.DataFrame:
        """
        Load the dataframe containing the data pool information.

        This method should be implemented by subclasses to define how the
        dataframe is loaded.

        :return: The loaded dataframe.
        :rtype: pd.DataFrame
        :raises NotImplementedError: If not implemented in a subclass.
        """
        raise NotImplementedError  # pragma: no cover

    def _get_st(self) -> Tuple[Dict[int, str], Dict[Tuple[str, str], int]]:
        """
        Get the internal state of the data pool.

        This method loads and caches the mapping between resource IDs and
        their corresponding archive locations.

        :return: A tuple containing two dictionaries:
                 1. Mapping from resource ID to archive filename
                 2. Mapping from (archive filename, file in archive) to resource ID
        :rtype: Tuple[Dict[int, str], Dict[Tuple[str, str], int]]
        """
        with self._lock:
            if self._st is None:
                _d_int_to_archive: Dict[int, str] = {}
                _d_archive_to_id: Dict[Tuple[str, str], int] = {}
                for row in tqdm(self._load_df().to_dict('records'), desc='Table Scanning'):
                    _d_int_to_archive[row[self._id_column]] = row[self._archive_column]
                    _d_archive_to_id[
                        (_n_path(row[self._archive_column]), _n_path(row[self._file_in_archive_column]))
                    ] = row[self._id_column]
                self._st = _d_int_to_archive, _d_archive_to_id

        return self._st

    def _file_to_resource_id(self, tar_file: str, filename: str) -> int:
        """
        Convert a file path to its corresponding resource ID.

        :param tar_file: The name of the archive file.
        :type tar_file: str
        :param filename: The name of the file within the archive.
        :type filename: str
        :return: The resource ID corresponding to the file.
        :rtype: int
        :raises FileUnrecognizableError: If the file is not recognized in the data pool.
        """
        _, _d_archive_to_id = self._get_st()
        token = (_n_path(tar_file), _n_path(filename))
        if token in _d_archive_to_id:
            return _d_archive_to_id[token]
        else:
            raise FileUnrecognizableError(f'Resource in tar {tar_file!r}\'s file {filename!r} unrecognizable.')

    def _request_possible_archives(self, resource_id) -> List[str]:
        """
        Get a list of possible archive filenames for a given resource ID.

        :param resource_id: The ID of the resource to look up.
        :type resource_id: int
        :return: A list of possible archive filenames containing the resource.
        :rtype: List[str]
        """
        _d_int_to_archive, _ = self._get_st()
        if resource_id in _d_int_to_archive:
            return [_d_int_to_archive[resource_id]]
        else:
            return []


class SimpleTableHfDataPool(TableBasedHfDataPool):
    """
    A simple implementation of TableBasedHfDataPool that loads data from a single table file.

    This class provides functionality to load data from a CSV or Parquet file stored
    in a Hugging Face repository.

    :param data_repo_id: The ID of the Hugging Face repository containing the data.
    :type data_repo_id: str
    :param archive_column: The name of the column containing archive filenames.
    :type archive_column: str
    :param file_in_archive_column: The name of the column containing filenames within archives.
    :type file_in_archive_column: str
    :param table_file: The name of the file containing the data table.
    :type table_file: str
    :param id_column: The name of the column containing unique identifiers for each data item.
    :type id_column: str
    :param data_revision: The revision of the data to use (default is 'main').
    :type data_revision: str
    :param mock_use_id: Whether to use the ID as part of the filename when extracting (default is True).
    :type mock_use_id: bool
    :param hf_token: An optional Hugging Face API token for authentication.
    :type hf_token: Optional[str]
    """

    def __init__(self, data_repo_id: str, archive_column: str, file_in_archive_column: str,
                 table_file: str, id_column: str = 'id', data_revision: str = 'main',
                 mock_use_id: bool = True, hf_token: Optional[str] = None):
        TableBasedHfDataPool.__init__(
            self,
            data_repo_id=data_repo_id,
            archive_column=archive_column,
            file_in_archive_column=file_in_archive_column,
            id_column=id_column,
            data_revision=data_revision,
            mock_use_id=mock_use_id,
            hf_token=hf_token,
        )
        self._table_file = table_file

    def _load_df(self) -> pd.DataFrame:
        """
        Load the dataframe from the specified table file in the Hugging Face repository.

        This method supports loading from CSV and Parquet files.

        :return: The loaded dataframe containing the data pool information.
        :rtype: pd.DataFrame
        :raises RuntimeError: If the file format is not supported or cannot be determined.
        """
        hf_client = get_hf_client(hf_token=self._hf_token)
        _, ext = os.path.splitext(self._table_file.lower())
        if ext == '.csv':
            fn_reader = pd.read_csv
        elif ext == '.parquet':
            fn_reader = pd.read_parquet
        else:
            raise RuntimeError(f'Unable to determine the reading operation of file {self._table_file!r}.')

        df = fn_reader(hf_client.hf_hub_download(
            repo_id=self.data_repo_id,
            repo_type='dataset',
            revision=self.data_revision,
            filename=self._table_file,
        ))
        return df
