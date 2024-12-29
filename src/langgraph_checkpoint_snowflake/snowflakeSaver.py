import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any, Optional

from langchain_core.runnables import RunnableConfig


from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
)

from langgraph_checkpoint_snowflake.base import BaseSnowflakeSaver
import snowflake.connector
from snowflake.connector import DictCursor
import json

class SnowflakeSaver(BaseSnowflakeSaver):

    def __init__(self, connection_name, 
                 warehouse=None, 
                 database=None, 
                 role=None):
        super().__init__()
        self.warehouse = warehouse
        self.database = database
        self.connection_name = connection_name
        self.role = role 

        try:
            conn_params = {}

            if self.connection_name:
                conn_params['connection_name'] = self.connection_name
            if self.warehouse:
                conn_params['warehouse'] = self.warehouse
            if self.database:
                conn_params['database'] = self.database
            if self.role:
                conn_params['role'] = self.role

            self.conn = snowflake.connector.connect(**conn_params)
            self.setup()
        except Exception as e:
            raise Exception(f"Error connecting to Snowflake: {e}")

    @classmethod
    @contextmanager
    def from_conn_string(cls, conn_name: str) -> Iterator["SnowflakeSaver"]:     
        with snowflake.connector.connect(connection_name=conn_name) as conn:
            yield cls(conn)

    def setup(self) -> None:
        """Set up the checkpoint database asynchronously.

        This method creates the necessary tables in the Postgres database if they don't
        already exist and runs database migrations. It MUST be called directly by the user
        the first time checkpointer is used.
        """
        with self.conn.cursor(DictCursor) as cur:
            cur.execute(self.MIGRATIONS[0])
            results = cur.execute(
                "SELECT v FROM checkpoint_migrations ORDER BY v DESC LIMIT 1"
            )
            row = results.fetchone()
            
            if row is None:
                version = -1
            else:
                version = row["V"]
            for v, migration in zip(
                range(version + 1, len(self.MIGRATIONS)),
                self.MIGRATIONS[version + 1 :],
            ):
                cur.execute(migration)
                cur.execute(f"INSERT INTO checkpoint_migrations (v) VALUES ({v})")

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:

        where, args = self._search_where(config, filter, before)
        query = self.SELECT_SQL + where + " ORDER BY checkpoint_id DESC"
        if limit:
            query += f" LIMIT {limit}"
        with self._cursor() as cur:
            cur.execute(query, args)
            for value in cur:
                channel_values = value.get("channel_values", value.get("CHANNEL_VALUES")).replace("undefined", "null")
                yield CheckpointTuple(
                    {
                        "configurable": {
                            "thread_id": value.get("thread_id", value.get("THREAD_ID")),
                            "checkpoint_ns": value.get("checkpoint_ns", value.get("CHECKPOINT_NS")),
                            "checkpoint_id": value.get("checkpoint_id", value.get("CHECKPOINT_ID")),
                        }
                    },
                    self._load_checkpoint(
                        json.loads(value.get("checkpoint", value.get("CHECKPOINT"))),
                        json.loads(channel_values),
                        json.loads(value.get("pending_sends", value.get("PENDING_SENDS"))),
                    ),
                    self._load_metadata(value.get("metadata", value.get("METADATA"))),
                    (
                        {
                            "configurable": {
                                "thread_id": value.get("thread_id", value.get("THREAD_ID")),
                                "checkpoint_ns": value.get("checkpoint_ns", value.get("CHECKPOINT_NS")),
                                "checkpoint_id": value.get("parent_checkpoint_id", value.get("PARENT_CHECKPOINT_ID")),
                            }
                        }
                        if value.get("parent_checkpoint_id", value.get("PARENT_CHECKPOINT_ID"))
                        else None
                    ),
                    self._load_writes(value.get("pending_writes", value.get("PENDING_WRITES"))),
                )

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:

        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = get_checkpoint_id(config)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        if checkpoint_id:
            args: tuple[Any, ...] = (thread_id, checkpoint_ns, checkpoint_id)
            where = "WHERE c.thread_id = %s AND c.checkpoint_ns = %s AND c.checkpoint_id = %s"
        else:
            args = (thread_id, checkpoint_ns)
            where = "WHERE c.thread_id = %s AND c.checkpoint_ns = %s ORDER BY c.checkpoint_id DESC LIMIT 1"

        with self._cursor() as cur:
            cur.execute(
                self.SELECT_SQL + where,
                args
            )

            for value in cur:
                channel_values = value.get("channel_values", value.get("CHANNEL_VALUES")).replace("undefined", "null")                
                return CheckpointTuple(
                    {
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": value.get("CHECKPOINT_ID"),
                        }
                    },
                    self._load_checkpoint(
                        json.loads(value.get("checkpoint", value.get("CHECKPOINT"))),
                        json.loads(channel_values),
                        json.loads(value.get("pending_sends", value.get("PENDING_SENDS")))
                    ),
                    self._load_metadata(value.get("metadata", value["METADATA"])),
                    (
                        {
                            "configurable": {
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": value.get("parent_checkpoint_id" , value["PARENT_CHECKPOINT_ID"]),
                            }
                        }
                        if (value.get("parent_checkpoint_id", value["PARENT_CHECKPOINT_ID"]))
                        else None
                    ),
                    self._load_writes(value.get("pending_writes" ,value["PENDING_WRITES"])),
                )

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:

        configurable = config["configurable"].copy()
        thread_id = configurable.pop("thread_id")
        checkpoint_ns = configurable.pop("checkpoint_ns")
        checkpoint_id = configurable.pop(
            "checkpoint_id", configurable.pop("thread_ts", None)
        )

        copy = checkpoint.copy()
        next_config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

        with self._cursor() as cur:
            cur.executemany(
                self.UPSERT_CHECKPOINT_BLOBS_SQL,
                self._dump_blobs(
                    thread_id,
                    checkpoint_ns,
                    copy.pop("channel_values"),  # type: ignore[misc]
                    new_versions,
                ),
            )
            cur.execute(
                self.UPSERT_CHECKPOINTS_SQL,
                (
                    thread_id,
                    checkpoint_ns,
                    checkpoint["id"],
                    checkpoint_id,
                    json.dumps(self._dump_checkpoint(copy)),
                    self._dump_metadata(metadata),
                ),
            )
        return next_config

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
    ) -> None:
        query = (
            self.UPSERT_CHECKPOINT_WRITES_SQL
            if all(w[0] in WRITES_IDX_MAP for w in writes)
            else self.INSERT_CHECKPOINT_WRITES_SQL
        )
        with self._cursor() as cur:
            cur.executemany(
                query,
                self._dump_writes(
                    config["configurable"]["thread_id"],
                    config["configurable"]["checkpoint_ns"],
                    config["configurable"]["checkpoint_id"],
                    task_id,
                    writes,
                ),
            )

    @contextmanager
    def _cursor(self) -> Iterator[snowflake.connector.cursor]:
        with self.conn.cursor(DictCursor) as cur:
            yield cur