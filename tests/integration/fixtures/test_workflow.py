import requests

from ghga_service_chassis_lib.s3 import ObjectStorageS3, S3Credentials

bucket1_id = "mytestbucket1"
bucket2_id = "mytestbucket1"
object_id = "mytestfile"

TEST_CREDENTIALS = S3Credentials(aws_access_key_id="test", aws_secret_access_key="test")
TEST_FILE_PATH = "/workspace/tests/integration/fixtures/test_file.txt"


storage = ObjectStorageS3(
    endpoint_url="http://s3-localstack:4566", credentials=TEST_CREDENTIALS
).__enter__()

storage.create_bucket(bucket1_id)

upload_url = storage.get_object_upload_url(bucket_id=bucket1_id, object_id=object_id)


with open(TEST_FILE_PATH, "r", encoding="utf8") as test_file:
    files = {"file": (str(TEST_FILE_PATH), test_file)}
    response = requests.post(upload_url.url, data=upload_url.fields, files=files)

response.raise_for_status()
