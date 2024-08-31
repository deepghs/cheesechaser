import os.path
from threading import Lock
from typing import List, Optional, Tuple, Dict

import pandas as pd
from hfutils.operate import get_hf_client
from tqdm import tqdm

from .base import HfBasedDataPool, _n_path, FileUnrecognizableError, DataLocation


class TableBasedHfDataPool(HfBasedDataPool):
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

    def _get_dst_filename(self, location: DataLocation):
        if self._mock_use_id:
            _, ext = os.path.splitext(location.filename)
            return f'{location.resource_id}{ext.lower()}'
        else:
            return super()._get_dst_filename(location)

    def _load_df(self) -> pd.DataFrame:
        raise NotImplementedError

    def _get_st(self) -> Tuple[Dict[int, str], Dict[Tuple[str, str], int]]:
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

    def _file_to_resource_id(self, tar_file: str, filename: str):
        _, _d_archive_to_id = self._get_st()
        token = (_n_path(tar_file), _n_path(filename))
        if token in _d_archive_to_id:
            return _d_archive_to_id[token]
        else:
            raise FileUnrecognizableError(f'Resource in tar {tar_file!r}\'s file {filename!r} unrecognizable.')

    def _request_possible_archives(self, resource_id) -> List[str]:
        _d_int_to_archive, _ = self._get_st()
        # print(resource_id)
        # pprint(_d_int_to_archive)
        if resource_id in _d_int_to_archive:
            return [_d_int_to_archive[resource_id]]
        else:
            return []


class SimpleTableHfDataPool(TableBasedHfDataPool):
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
