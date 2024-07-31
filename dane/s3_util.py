from dataclasses import dataclass
import boto3
import logging
import ntpath
import os
from pathlib import Path
import tarfile
from typing import List, Tuple, Optional
from time import time
from dane.provenance import Provenance

logger = logging.getLogger("DANE")
COMPRESSED_TAR_EXTENSION = ".tar.gz"


@dataclass
class DownloadResult:
    success: bool
    file_path: str  # target_file_path,  # TODO harmonize with dane-download-worker
    download_time: float = -1  # time (msecs) taken to receive data after request
    mime_type: str = "unknown"  # download_data.get("mime_type", "unknown"),
    content_length: int = -1  # download_data.get("content_length", -1),

    def to_download_provenance(
        self,
        input_file_path: str,
        start_time: float,
        software_version: str,
    ) -> Provenance:
        return Provenance(
            activity_name="Download S3 data",
            activity_description="Download data from S3 bucket",
            start_time_unix=start_time,
            processing_time_ms=self.download_time,
            software_version=software_version,
            input_data={"input_file_path": input_file_path},
            output_data={"file_path": self.file_path},
        )


# the file name without extension is used as an asset ID to save the results
def generate_asset_id_from_input_file(
    input_file: str, with_extension: bool = False
) -> str:
    logger.debug(f"generating asset ID for {input_file}")
    file_name = ntpath.basename(input_file)  # grab the file_name from the path
    if with_extension:
        return file_name

    # otherwise cut off the extension
    asset_id, extension = os.path.splitext(file_name)
    return asset_id


def is_valid_tar_path(archive_path: str) -> bool:
    logger.info(f"Validating {archive_path}")
    if not os.path.exists(Path(archive_path).parent):
        logger.error(f"Parent dir does not exist: {archive_path}")
        return False
    if archive_path[-7:] != COMPRESSED_TAR_EXTENSION:
        logger.error(
            f"Archive file should have correct extension: {COMPRESSED_TAR_EXTENSION}"
        )
        return False
    return True


def digest_file_list(
    file_list: List[str], prefix: str, tar_archive_path: str = ""
) -> List[Tuple[str, str]]:
    """
    Go over the file list and return a list of (filepath, key) tuples for S3 transfer.
    The key a combination of the specified prefix and the basename of the file.
    If tar_archive_path is given, the content of the file_list is tarred and a singleton
    list of (tar_archive, key) is returned.
    If not, the file list (which may contain directories) is processed recursively,
    adding each (file, key). Directory structure is maintained as infix.
    """

    # first check if the file_list needs to be compressed (into tar)
    if tar_archive_path:
        tar_success = tar_list_of_files(tar_archive_path, file_list)
        if not tar_success:
            logger.error("Could not archive the file list before transferring to S3")
            raise Exception("Could not digest file list: archive error")
        key = os.path.join(
            prefix,
            generate_asset_id_from_input_file(  # file name with extension
                tar_archive_path, True
            ),
        )
        file_and_key_list = [(tar_archive_path, key)]
    else:
        file_and_key_list = []
        for f in file_list:
            if os.path.isdir(f):
                for root, dirs, files in os.walk(f):
                    file_and_key_list.extend(
                        digest_file_list(
                            file_list=[os.path.join(root, fn) for fn in dirs + files],
                            prefix=os.path.join(prefix, os.path.basename(f)),
                            tar_archive_path=tar_archive_path,
                        )
                    )
            else:
                key = os.path.join(
                    prefix,
                    generate_asset_id_from_input_file(  # file name with extension
                        f, True
                    ),
                )
                file_and_key_list.append((f, key))
    return file_and_key_list


def tar_list_of_files(archive_path: str, file_list: List[str]) -> bool:
    logger.info(f"Tarring {len(file_list)} into {archive_path}")
    if not is_valid_tar_path(archive_path):
        return False
    try:
        with tarfile.open(archive_path, "w:gz") as tar:
            for item in file_list:
                logger.info(os.path.basename(item))
                tar.add(item, arcname=os.path.basename(item))
        logger.info(f"Succesfully created {archive_path}")
        return True
    except tarfile.TarError:
        logger.exception(f"Failed to created archive: {archive_path}")
    except FileNotFoundError:
        logger.exception("File in file list not found")
    except Exception:
        logger.exception("Unhandled error")
    logger.error("Unknown error")
    return False


def validate_s3_uri(s3_uri: str) -> bool:
    if s3_uri[0:5] != "s3://":
        logger.error(f"Invalid protocol in {s3_uri}")
        return False
    if len(s3_uri[5:].split("/")) < 2:
        logger.error(f"No object_name specified {s3_uri}")
        return False
    return True


# e.g. "s3://beng-daan-visxp/jaap-dane-test/dane-test.tar.gz"
def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    logger.info(f"Parsing s3 URI {s3_uri}")
    tmp = s3_uri[5:]
    bucket = tmp[: tmp.find("/")]  # beng-daan-visxp
    object_name = s3_uri[len(bucket) + 6 :]  # jaap-dane-test/dane-test.tar.gz
    return bucket, object_name


def download_s3_uri(s3_uri: str, output_folder: str) -> DownloadResult:
    logger.info(f"Downloading {s3_uri}")
    if not validate_s3_uri(s3_uri):
        logger.error("Invalid S3 URI")
        return DownloadResult(success=False, file_path="")
    s3_store = S3Store()
    bucket, object_name = parse_s3_uri(s3_uri)
    logger.debug(f"OBJECT NAME: {object_name}")
    return s3_store.download_file(bucket, object_name, output_folder)


class S3Store:

    """
    requires environment:
        - "AWS_ACCESS_KEY_ID=your-key"
        - "AWS_SECRET_ACCESS_KEY=your-secret"
    TODO read from .aws/config, so boto3 can assume an IAM role
    see: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html

    """

    def __init__(self, s3_endpoint_url: Optional[str] = None, unit_testing=False):
        self.client = boto3.client("s3", endpoint_url=s3_endpoint_url)

    def clear_prefix(self, bucket: str, prefix: str):
        """
        Clean up the prefix in the bucket: delete all keys with the specified prefix.
        """
        response = self.client.list_objects(Bucket=bucket, Prefix=prefix)
        if len(response["Contents"]) > 0:
            logger.warn(f"Prefix '{prefix}' exists in bucket '{bucket}'.")
            to_delete = []
            while True:
                to_delete += [{"Key": item["Key"]} for item in response["Contents"]]
                if not response["IsTruncated"]:
                    break
                response = self.client.list_objects(
                    Bucket=bucket, Prefix=prefix, Marker=response["Contents"][-1]["Key"]
                )
            logger.warn(
                f"Removing {len(to_delete)} item with '{prefix}' as prefix from bucket."
            )
            self.client.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
        # TODO: Add error handling

    def transfer_to_s3(
        self, bucket: str, prefix: str, file_list: List[str], tar_archive_path: str = ""
    ) -> bool:
        """
        Transfer the contents of file_list to the specified S3 bucket with the
        specified prefix.
        If tar_archive_path is specified, the file_list is wrapped into a single
        archive first. Otherwise, the directory structure in file_list is maintained
        in the prefix.
        """
        self.clear_prefix(bucket, prefix)  # clean up any old stuff
        file_and_key_list = digest_file_list(
            file_list=file_list, prefix=prefix, tar_archive_path=tar_archive_path
        )

        # now go ahead and upload whatever is in the file list
        for f, k in file_and_key_list:
            try:
                self.client.upload_file(
                    Filename=f,
                    Bucket=bucket,
                    Key=k,
                )
            except Exception:  # TODO figure out which Exception(s) to catch specifically
                logger.exception(f"Failed to upload {f}")
                return False
        return True

    def download_file(
        self, bucket: str, object_name: str, output_folder: str
    ) -> DownloadResult:
        start_time = time()
        logger.info(f"Downloading {bucket}:{object_name} into {output_folder}")
        if not os.path.exists(output_folder):
            logger.info("Output folder does not exist, creating it...")
            os.makedirs(output_folder)
        output_path = os.path.join(output_folder, os.path.basename(object_name))
        if "tar.gz" in object_name:
            try:
                with open(output_path, "wb") as f:
                    self.client.download_fileobj(bucket, object_name, f)
                success = True
            except Exception:
                logger.exception(f"Failed to download {object_name}")
                success = False
        else:
            response = self.client.list_objects_v2(Bucket=bucket, Prefix=object_name)
            if response['KeyCount'] == 0:
                logger.error(
                    f"No content available in bucket '{bucket}' for prefix '{object_name}'."
                )
                success = False
            else:

                to_download = []
                while True:
                    to_download += [
                        {"Key": item["Key"]} for item in response["Contents"]
                    ]
                    if not response["IsTruncated"]:
                        break
                    response = self.client.list_objects_v2(
                        Bucket=bucket,
                        Prefix=object_name,
                        Marker=response["Contents"][-1]["Key"],
                    )
                success = True  # Upon failure, will be set to False
                for item in to_download:
                    # path within bucket, e.g. 'test_items/1411058.1366653.WEEKNUMMER404-HRE000042FF_924200_1089200/keyframes/105320.jpg'}
                    splitpath = item["Key"].split("/")
                    subpath, basename = splitpath[2:-1], splitpath[-1]
                    location = os.path.join(output_path, *subpath)
                    if not os.path.exists(location):
                        os.makedirs(location)
                    try:
                        with open(os.path.join(location, basename), "wb") as f:
                            self.client.download_fileobj(Bucket=bucket, Key=item['Key'], Fileobj=f)
                    except Exception:
                        logger.exception(
                            f"Failed to download {item} from bucket {bucket}."
                        )
                        success = False

        return DownloadResult(
            success=success,
            file_path=output_path,
            download_time=time() - start_time,
        )
