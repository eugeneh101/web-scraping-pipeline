import io
import os
import random
import zipfile

import boto3
import pandas as pd


queue = boto3.resource("sqs").get_queue_by_name(QueueName=os.environ["QUEUE_NAME"])
with open("data-engineering-bezant-assignement-dataset.zip", "rb") as f:  # hard coded file
    with zipfile.ZipFile(f, "r") as zf:
        in_memory_csv_file = io.BytesIO(zf.read("telegram.csv"))  # hard coded file
        df_messages = pd.read_csv(in_memory_csv_file)


def lambda_handler(event, context) -> None:
    while True:  # do-while loop
        df_sample = df_messages.sample(random.randint(1, 1000)).reset_index(drop=True)  # hard coded between 1 and 1000 messages
        if len(df_sample.to_json()) < 256_000:  # max SQS message is 256 KB
            break
    queue.send_message(MessageBody=df_sample.to_json())
    return
