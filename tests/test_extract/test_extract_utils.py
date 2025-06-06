import json
import pytest
from moto import mock_aws
from src.extract import get_state, change_state, BUCKET, STATUS_KEY


class TestChangeState:

    def test_for_changed_state(self, s3_with_bucket, s3_client):
        change_state(s3_with_bucket, status=True)

        response = s3_client.get_object(Bucket=BUCKET, Key=STATUS_KEY)
        data = json.loads(response["Body"].read().decode("utf-8"))
        assert data["is_first_run"] == True

        change_state(s3_with_bucket, status=False)

        response = s3_client.get_object(Bucket=BUCKET, Key=STATUS_KEY)
        data = json.loads(response["Body"].read().decode("utf-8"))
        assert data["is_first_run"] == False

    @pytest.mark.skip("Need to revisit")
    def test_for_error_handling(self, s3_client, capsys):
        captured = capsys.readouterr()
        # assert "NoSuchBucket" in captured.out

    def test_raises_error_for_wrong_arg_type(self, s3_with_bucket):
        with pytest.raises(TypeError) as err:
            change_state(s3_with_bucket, 3)

        assert str(err.value) == "Only arguments of type bool accepted"


class TestGetState:
    def test_first_run_creates_file(self, s3_with_bucket, s3_client):
        response = get_state(s3_with_bucket)

        assert response == True

        get_object = s3_client.get_object(Bucket=BUCKET, Key=STATUS_KEY)

        assert get_object["ContentLength"] > 0

    def test_first_run_has_status_true(self, s3_with_bucket, s3_client):
        response = get_state(s3_with_bucket)

        assert response == True

        get_object = s3_client.get_object(Bucket=BUCKET, Key=STATUS_KEY)
        data = json.loads(get_object["Body"].read().decode("utf-8"))

        assert data["is_first_run"] == True

    def test_for_subsequent_runs(self, s3_with_bucket, s3_client):
        get_state(s3_with_bucket)
        change_state(s3_with_bucket, False)
        response = get_state(s3_with_bucket)

        assert response == False

        get_object = s3_client.get_object(Bucket=BUCKET, Key=STATUS_KEY)
        data = json.loads(get_object["Body"].read().decode("utf-8"))

        assert data["is_first_run"] == False
