from langgraph.graph import START, StateGraph
from langgraph_checkpoint_snowflake import SnowflakeSaver
from typing import TypedDict


# Subgraph definitions
class SubgraphState(TypedDict):
    foo: str  # note that this key is shared with the parent graph state
    bar: str


def subgraph_node_1(state: SubgraphState):
    return {"bar": "bar"}


def subgraph_node_2(state: SubgraphState):
    return {"foo": state["foo"] + state["bar"]}


subgraph_builder = StateGraph(SubgraphState)
subgraph_builder.add_node(subgraph_node_1)
subgraph_builder.add_node(subgraph_node_2)
subgraph_builder.add_edge(START, "subgraph_node_1")
subgraph_builder.add_edge("subgraph_node_1", "subgraph_node_2")
subgraph = subgraph_builder.compile()


# Parent graph definitions
class State(TypedDict):
    foo: str


def node_1(state: State):
    return {"foo": "hi! " + state["foo"]}


builder = StateGraph(State)
builder.add_node("node_1", node_1)
builder.add_node("node_2", subgraph)
builder.add_edge(START, "node_1")
builder.add_edge("node_1", "node_2")

checkpointer = SnowflakeSaver(connection_name="langgraph")

graph = builder.compile(checkpointer=checkpointer)
config = {"configurable": {"thread_id": "318"}}
for _, chunk in graph.stream({"foo": "foo"}, config, subgraphs=True):
    print(chunk)

print(graph.get_state(config).values)

state_with_subgraph = [
    s for s in graph.get_state_history(config) if s.next == ("node_2",)
][0]

subgraph_config = state_with_subgraph.tasks[0].state
print(subgraph_config)

print(graph.get_state(subgraph_config).values)
