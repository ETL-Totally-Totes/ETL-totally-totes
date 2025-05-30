import json
import boto3
from botocore.exceptions import ClientError

BUCKET_NAME = "abby-test-bucket-test"


def get_state(s3_client):
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key="status_check.json")
        data = json.loads(response["Body"].read().decode("utf-8"))
        return data["is_first_run"]
    except ClientError as e:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key="status_check.json",
            Body=json.dumps({"is_first_run": True}),
        )
        return True


def change_state(s3_client, status: bool):
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key="status_check.json",
            Body=json.dumps({"is_first_run": status}),
        )
    except ClientError as e:
        print(e)


if __name__ == "__main__":
    print(get_state(boto3.client("s3")))
    print(change_state(boto3.client("s3"), False))
    print(get_state(boto3.client("s3")))
