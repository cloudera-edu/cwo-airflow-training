"""S3KeyArrivalEventTrigger - a pull-based watcher trigger for AssetWatcher.

Fires once when the object appears, then deletes it, so the next upload
fires again (the same pattern as Airflow's built-in FileDeleteTrigger).

Everything here is async (aiobotocore through S3Hook.get_async_conn), like the
built-in S3KeyTrigger. Don't call the synchronous S3Hook from a thread inside a
trigger: the hook fetches its connection over the triggerer's supervisor
channel, and doing that from another thread can crash the triggerer process
for every DAG on the instance.
"""
from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.triggers.base import BaseEventTrigger, TriggerEvent


class S3KeyArrivalEventTrigger(BaseEventTrigger):
    def __init__(self, bucket: str, key: str, aws_conn_id: str = "object_store", poke_interval: float = 15.0):
        super().__init__()
        self.bucket = bucket
        self.key = key
        self.aws_conn_id = aws_conn_id
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "training_triggers.s3_watch.S3KeyArrivalEventTrigger",
            {
                "bucket": self.bucket,
                "key": self.key,
                "aws_conn_id": self.aws_conn_id,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = S3Hook(aws_conn_id=self.aws_conn_id)
        async with await hook.get_async_conn() as client:
            while True:
                if await hook.check_key_async(client, self.bucket, self.key, False):
                    await client.delete_object(Bucket=self.bucket, Key=self.key)
                    self.log.info("Found and consumed s3://%s/%s", self.bucket, self.key)
                    yield TriggerEvent({"bucket": self.bucket, "key": self.key})
                    return
                await asyncio.sleep(self.poke_interval)
