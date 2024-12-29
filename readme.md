// @! create readme file to help end user usage of this included code include=src/langgraph_checkpoint_snowflake/snowflakesaver.py

# SnowflakeSaver

The `SnowflakeSaver` class is a part of the `langgraph_checkpoint_snowflake` module and is designed to manage checkpoints in a Snowflake database. 


## Usage

### Initialization

To create an instance of `SnowflakeSaver`, you can provide optional parameters for the connection:

python
saver = SnowflakeSaver(connection_name='my_conn', warehouse='my_warehouse', database='my_db', role='my_role')


