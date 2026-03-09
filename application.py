from datetime import datetime
from fastapi import FastAPI, HTTPException
import boto3
import json

from app.schemas import RequestPayload
from app.config import get_jwt_secret, SQS_QUEUE_NAME, AWS_REGION

app = FastAPI(title="Email API Service")

sqs = boto3.client("sqs", region_name=AWS_REGION)
JWT_SECRET = get_jwt_secret()
QUEUE_URL = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]


def verify_token(token: str):
    expected = JWT_SECRET.strip()
    received = token.strip()

    if received != expected:
        raise HTTPException(status_code=401, detail="Invalid token")


def validate_timestamp(ts: str):
    if not ts.isdigit():
        raise HTTPException(
            status_code=400,
            detail="email_timestream must be a Unix timestamp ",
        )

    try:
        datetime.utcfromtimestamp(int(ts))
    except (ValueError, OSError):
        raise HTTPException(
            status_code=400,
            detail="email_timestream is not a valid timestamp",
        )


@app.post("/send")
def send_message(payload: RequestPayload):
    verify_token(payload.token)

    validate_timestamp(payload.data.email_timestream)
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(payload.dict()),
    )

    return {
        "status": "sent",
        "message_id": response["MessageId"],
    }


@app.get("/health")
def health_check():
    return {"status": "ok"}
