from dane.s3_util import digest_file_list
import pytest
import os
import shutil
import tarfile


@pytest.fixture()
def tar_archive_paths():
    paths = ("tmp_tar_for_tests.tar.gz", "tmp_untarred_for_tests")
    for path in paths:
        if os.path.exists(path):
            print("Test path exists. Abort test.")
            assert False
    os.makedirs(paths[1])
    yield paths
    os.remove(paths[0])
    shutil.rmtree(paths[1])


@pytest.fixture()
def setup_output_dir():
    base = "tmp_dir_for_tests"
    outputtypes = ["provenance", "assets"]
    for outputtype in outputtypes:
        os.makedirs(os.path.join(base, outputtype))
    for fn in [
        os.path.join(base, "tmp_file_top_level"),
        os.path.join(base, outputtypes[0], "tmp_file_in_subdir"),
    ]:
        with open(fn, "w") as f:
            f.write("Hello world")
    yield base

    shutil.rmtree(base)


def test_digest_file_list_no_tar(setup_output_dir):
    prefix = "test-prefix"
    list_of_files_and_keys = digest_file_list(
        file_list=[setup_output_dir], prefix=prefix
    )
    for f, k in list_of_files_and_keys:
        assert os.path.isfile(f)
        assert k.startswith(prefix)


def test_digest_file_list_tar(setup_output_dir, tar_archive_paths):
    tar_archive_path, untarred_archive_path = tar_archive_paths
    prefix = "test-prefix"
    list_of_files_and_keys = digest_file_list(
        file_list=[setup_output_dir], prefix=prefix, tar_archive_path=tar_archive_path
    )
    assert len(list_of_files_and_keys) == 1
    assert os.path.isfile(tar_archive_path)
    assert list_of_files_and_keys[0][1].startswith(prefix)
    with tarfile.open(tar_archive_path) as tar:
        tar.extractall(path=untarred_archive_path, filter="data")  # type: ignore
