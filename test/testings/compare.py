import os.path
import pathlib

from hfutils.utils import is_binary_file, walk_files


def file_compare(file1, file2):
    file1_type = 'binary' if is_binary_file(file1) else 'text'
    file2_type = 'binary' if is_binary_file(file2) else 'text'
    if file1_type != file2_type:
        assert False, f'{file1!r} is {file1_type}, but {file2!r} is {file2_type}.'

    if file1_type == 'text':
        assert pathlib.Path(file1).read_text(encoding='utf-8').splitlines(keepends=False) == \
               pathlib.Path(file2).read_text(encoding='utf-8').splitlines(keepends=False)
    else:
        assert pathlib.Path(file1).read_bytes() == pathlib.Path(file2).read_bytes()


def dir_compare(dir1, dir2):
    files1 = sorted(walk_files(dir1))
    files2 = sorted(walk_files(dir2))
    assert files1 == files2

    for file in files1:
        file_compare(os.path.join(dir1, file), os.path.join(dir2, file))
