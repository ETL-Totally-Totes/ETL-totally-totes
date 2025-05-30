import json
import boto3
from botocore.exceptions import ClientError

BUCKET_NAME = "abby-test-bucket-test" #CHANGE THIS PLEASE!
STATUS_KEY = "status_check.json"


def get_state(s3_client):

    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=STATUS_KEY)
        data = json.loads(response["Body"].read().decode("utf-8"))
        return data["is_first_run"]
    except ClientError as e:
        # if it is the first run and the file is not there
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=STATUS_KEY,
            Body=json.dumps({"is_first_run": True}),
        )
        return True


def change_state(s3_client, status: bool):
    if isinstance(status, bool):
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=STATUS_KEY,
                Body=json.dumps({"is_first_run": status}),
            )
        except ClientError as e:
            print(e)
    else:
        raise TypeError("Only arguments of type bool accepted")


if __name__ == "__main__":
    print(get_state(boto3.client("s3")))
    print(change_state(boto3.client("s3"), False))
    print(get_state(boto3.client("s3")))
