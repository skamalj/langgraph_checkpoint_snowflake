import random
from collections.abc import Sequence
from typing import Any, Optional, cast

from langchain_core.runnables import RunnableConfig

from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    get_checkpoint_id,
)
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.serde.types import TASKS, ChannelProtocol
from langgraph_checkpoint_snowflake.snowflakeSerializer import SnowflakeSerializer
import json

MetadataInput = Optional[dict[str, Any]]

"""
To add a new migration, add a new string to the MIGRATIONS list.
The position of the migration in the list is the version number.
"""
MIGRATIONS = [
    """
    CREATE TABLE IF NOT EXISTS checkpoint_migrations (
        v INTEGER PRIMARY KEY
    );
    """,
    
    """
    CREATE TABLE IF NOT EXISTS checkpoints (
        thread_id STRING NOT NULL,
        checkpoint_ns STRING NOT NULL DEFAULT '',
        checkpoint_id STRING NOT NULL,
        parent_checkpoint_id STRING,
        type STRING,
        checkpoint OBJECT NOT NULL, -- JSONB equivalent in Snowflake
        metadata OBJECT NOT NULL DEFAULT OBJECT_CONSTRUCT(), -- JSONB equivalent with default empty object
        PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
    );
    """,
    
    """
    CREATE TABLE IF NOT EXISTS checkpoint_blobs (
        thread_id STRING NOT NULL,
        checkpoint_ns STRING NOT NULL DEFAULT '',
        channel STRING NOT NULL,
        version STRING NOT NULL,
        type STRING NOT NULL,
        blob STRING, -- BYTEA stored as base64 using cutom serializer in Snowflake
        PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
    );
    """,
    
    """
    CREATE TABLE IF NOT EXISTS checkpoint_writes (
        thread_id STRING NOT NULL,
        checkpoint_ns STRING NOT NULL DEFAULT '',
        checkpoint_id STRING NOT NULL,
        task_id STRING NOT NULL,
        idx INTEGER NOT NULL,
        channel STRING NOT NULL,
        type STRING,
        blob STRING NOT NULL,
        PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
    );
    """,  
    """
    ALTER TABLE checkpoint_blobs MODIFY COLUMN blob STRING;
    """
]

SELECT_SQL = f"""
WITH checkpoint_blob_data AS (
    SELECT c.thread_id, c.checkpoint_ns,c.checkpoint_id,
        ARRAY_AGG(ARRAY_CONSTRUCT(bl.channel, bl.type, bl.blob)) AS channel_values
    FROM checkpoints as c,
    LATERAL FLATTEN(INPUT => c.checkpoint:channel_versions) as  j
    JOIN checkpoint_blobs as bl
        WHERE bl.thread_id =  c.thread_id
        AND bl.checkpoint_ns = c.checkpoint_ns
        AND bl.channel = j.KEY
        AND bl.version = j.VALUE
    GROUP BY
        c.thread_id,  c.checkpoint_ns, c.checkpoint_id
)
SELECT
    c.thread_id,
    c.checkpoint,
    c.checkpoint_ns,
    c.checkpoint_id,
    c.parent_checkpoint_id,
    c.metadata,
    blob_data.channel_values,
    pending_writes.pending_writes,
    pending_sends.pending_sends
FROM checkpoints c
JOIN checkpoint_blob_data blob_data
    ON blob_data.thread_id = c.thread_id
    AND blob_data.checkpoint_ns = c.checkpoint_ns
    AND blob_data.checkpoint_id = c.checkpoint_id
, LATERAL (
    SELECT ARRAY_AGG(ARRAY_CONSTRUCT(cw.task_id, cw.channel, cw.type, cw.blob)) AS pending_writes
    FROM checkpoint_writes cw
    WHERE cw.thread_id = c.thread_id
      AND cw.checkpoint_ns = c.checkpoint_ns
      AND cw.checkpoint_id = c.checkpoint_id
) AS pending_writes,
LATERAL (
    SELECT ARRAY_AGG(ARRAY_CONSTRUCT(cw.task_id, cw.channel, cw.blob)) AS pending_sends
    FROM checkpoint_writes cw
    WHERE cw.thread_id = c.thread_id
      AND cw.checkpoint_ns = c.checkpoint_ns
      AND cw.checkpoint_id = c.checkpoint_id
      AND cw.channel = '{TASKS}'
) AS pending_sends """

UPSERT_CHECKPOINT_BLOBS_SQL = """
MERGE INTO checkpoint_blobs AS target
USING (SELECT 
           %s AS thread_id,
           %s AS checkpoint_ns,
           %s AS channel,
           %s AS version,
           %s AS type,
           %s AS blob
       ) AS source
ON target.thread_id = source.thread_id 
   AND target.checkpoint_ns = source.checkpoint_ns
   AND target.channel = source.channel
   AND target.version = source.version
WHEN NOT MATCHED THEN
    INSERT (thread_id, checkpoint_ns, channel, version, type, blob)
    VALUES (source.thread_id, source.checkpoint_ns, source.channel, source.version, source.type, source.blob);
"""

UPSERT_CHECKPOINTS_SQL = """
MERGE INTO checkpoints AS target
USING (SELECT 
           %s AS thread_id,
           %s AS checkpoint_ns,
           %s AS checkpoint_id,
           %s AS parent_checkpoint_id,
           PARSE_JSON(%s) AS checkpoint,
           PARSE_JSON(%s) AS metadata
       ) AS source
ON target.thread_id = source.thread_id 
   AND target.checkpoint_ns = source.checkpoint_ns
   AND target.checkpoint_id = source.checkpoint_id
WHEN MATCHED THEN
    UPDATE SET 
        checkpoint = source.checkpoint,
        metadata = source.metadata
WHEN NOT MATCHED THEN
    INSERT (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata)
    VALUES (source.thread_id, source.checkpoint_ns, source.checkpoint_id, source.parent_checkpoint_id, source.checkpoint, source.metadata);
"""

UPSERT_CHECKPOINT_WRITES_SQL = """
MERGE INTO checkpoint_writes AS target
USING (SELECT 
           %s AS thread_id,
           %s AS checkpoint_ns,
           %s AS checkpoint_id,
           %s AS task_id,
           %s AS idx,
           %s AS channel,
           %s AS type,
           %s AS blob
       ) AS source
ON target.thread_id = source.thread_id 
   AND target.checkpoint_ns = source.checkpoint_ns
   AND target.checkpoint_id = source.checkpoint_id
   AND target.task_id = source.task_id
   AND target.idx = source.idx
WHEN MATCHED THEN
    UPDATE SET 
        channel = source.channel,
        type = source.type,
        blob = source.blob
WHEN NOT MATCHED THEN
    INSERT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, blob)
    VALUES (source.thread_id, source.checkpoint_ns, source.checkpoint_id, source.task_id, source.idx, source.channel, source.type, source.blob);
"""

INSERT_CHECKPOINT_WRITES_SQL = """
MERGE INTO checkpoint_writes AS target
USING (SELECT 
           %s AS thread_id,
           %s AS checkpoint_ns,
           %s AS checkpoint_id,
           %s AS task_id,
           %s AS idx,
           %s AS channel,
           %s AS type,
           %s AS blob
       ) AS source
ON target.thread_id = source.thread_id 
   AND target.checkpoint_ns = source.checkpoint_ns
   AND target.checkpoint_id = source.checkpoint_id
   AND target.task_id = source.task_id
   AND target.idx = source.idx
WHEN NOT MATCHED THEN
    INSERT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, blob)
    VALUES (source.thread_id, source.checkpoint_ns, source.checkpoint_id, source.task_id, source.idx, source.channel, source.type, source.blob);
"""


class BaseSnowflakeSaver(BaseCheckpointSaver[str]):
    SELECT_SQL = SELECT_SQL
    MIGRATIONS = MIGRATIONS
    UPSERT_CHECKPOINT_BLOBS_SQL = UPSERT_CHECKPOINT_BLOBS_SQL
    UPSERT_CHECKPOINTS_SQL = UPSERT_CHECKPOINTS_SQL
    UPSERT_CHECKPOINT_WRITES_SQL = UPSERT_CHECKPOINT_WRITES_SQL
    INSERT_CHECKPOINT_WRITES_SQL = INSERT_CHECKPOINT_WRITES_SQL

    jsonplus_serde = JsonPlusSerializer()
    supports_pipeline: bool

    def __init__(self):
        super().__init__()
        self.snowflake_serde = SnowflakeSerializer(self.serde)

    def _load_checkpoint(
        self,
        checkpoint: dict[str, Any],
        channel_values: list[tuple[bytes, bytes, bytes]],
        pending_sends: list[tuple[bytes, bytes]],
    ) -> Checkpoint:
       
        return {
            **checkpoint,
            "pending_sends": [
                self.snowflake_serde.loads_typed((c.decode(), b)) for c, b in pending_sends or []
            ],
            "channel_values": self._load_blobs(channel_values),
        }

    def _dump_checkpoint(self, checkpoint: Checkpoint) -> dict[str, Any]:
        return {**checkpoint, "pending_sends": []}

    def _load_blobs(
        self, blob_values: list[tuple[bytes, bytes, bytes]]
    ) -> dict[str, Any]:
        if not blob_values:
            return {}
        return {
            k: self.snowflake_serde.loads_typed((t, v))
            for k, t, v in blob_values
            if t != "empty"
        }
        

    def _dump_blobs(
        self,
        thread_id: str,
        checkpoint_ns: str,
        values: dict[str, Any],
        versions: ChannelVersions,
    ) -> list[tuple[str, str, str, str, str, Optional[bytes]]]:
        if not versions:
            return []

        return [
            (
                thread_id,
                checkpoint_ns,
                k,
                cast(str, ver),
                *(
                    self.snowflake_serde.dumps_typed(values[k])
                    if k in values
                    else ("empty", None)
                ),
            )
            for k, ver in versions.items()
        ]

    def _load_writes(
        self, writes: list[tuple[bytes, bytes, bytes, bytes]]
    ) -> list[tuple[str, str, Any]]:
        return (
            [
                (
                    tid,
                    channel,
                    self.snowflake_serde.loads_typed((t, v)),
                )
                for tid, channel, t, v in json.loads(writes)
            ]
            if writes
            else []
        )

    def _dump_writes(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        task_id: str,
        writes: Sequence[tuple[str, Any]],
    ) -> list[tuple[str, str, str, str, int, str, str, bytes]]:
        return [
            (
                thread_id,
                checkpoint_ns,
                checkpoint_id,
                task_id,
                WRITES_IDX_MAP.get(channel, idx),
                channel,
                *self.snowflake_serde.dumps_typed(value),
            )
            for idx, (channel, value) in enumerate(writes)
        ]

    def _load_metadata(self, metadata: dict[str, Any]) -> CheckpointMetadata:
        metadata =  self.jsonplus_serde.loads(self.jsonplus_serde.dumps(metadata))
        return json.loads(metadata)

    def _dump_metadata(self, metadata: CheckpointMetadata) -> str:
        serialized_metadata = self.jsonplus_serde.dumps(metadata)
        # NOTE: we're using JSON serializer (not msgpack), so we need to remove null characters before writing
        return serialized_metadata.decode().replace("\\u0000", "")

    def get_next_version(self, current: Optional[str], channel: ChannelProtocol) -> str:
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        next_v = current_v + 1
        next_h = random.random()
        return f"{next_v:032}.{next_h:016}"

    def _search_where(
        self,
        config: Optional[RunnableConfig],
        filter: MetadataInput,
        before: Optional[RunnableConfig] = None,
    ) -> tuple[str, list[Any]]:
        """Return WHERE clause predicates for alist() given config, filter, before.

        This method returns a tuple of a string and a tuple of values. The string
        is the parametered WHERE clause predicate (including the WHERE keyword):
        "WHERE column1 = $1 AND column2 IS $2". The list of values contains the
        values for each of the corresponding parameters.
        """
        wheres = []
        param_values = []

        # construct predicate for config filter
        if config:
            wheres.append("c.thread_id = %s ")
            param_values.append(config["configurable"]["thread_id"])
            checkpoint_ns = config["configurable"].get("checkpoint_ns")
            if checkpoint_ns is not None:
                wheres.append("c.checkpoint_ns = %s")
                param_values.append(checkpoint_ns)

            if checkpoint_id := get_checkpoint_id(config):
                wheres.append("c.checkpoint_id = %s ")
                param_values.append(checkpoint_id)

        # construct predicate for metadata filter
        if filter:
            for key, value in filter.items():
                wheres.append(f"JSON_VALUE(metadata, '$.{key}') = %s")
                param_values.append(value)

        # construct predicate for `before`
        if before is not None:
            wheres.append("c.checkpoint_id < %s ")
            param_values.append(get_checkpoint_id(before))

        return (
            "WHERE " + " AND ".join(wheres) if wheres else "",
            param_values,
        )