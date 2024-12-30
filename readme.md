# SnowflakeSaver

The `SnowflakeSaver` class is a part of the `langgraph_checkpoint_snowflake` module and is designed to manage checkpoints in a Snowflake database. 


## Usage

### Initialization

To create an instance of `SnowflakeSaver`, you can provide optional parameters for the connection:
- The connection name is from `connections.toml` file.
- Optionally, database, schema, warehouse and role can be overridden.

```
saver = SnowflakeSaver(connection_name='my_conn', warehouse='my_warehouse', database='my_db', role='my_role', schema='myschema')
```

