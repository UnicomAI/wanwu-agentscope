# -*- coding: utf-8 -*-
"""
AgentScope workstation DAG running engine.

This module defines various workflow nodes that can be used to construct
a computational DAG. Each node represents a step in the DAG and
can perform certain actions when called.
"""
import copy
import uuid

from agentscope.web.workstation.workflow_node import (
    NODE_NAME_MAPPING,
    WorkflowNodeType,
    ASDiGraph,
)
from agentscope.web.workstation.workflow_utils import (
    is_callable_expression,
    kwarg_converter,
)


try:
    import networkx as nx
except ImportError:
    nx = None


def sanitize_node_data(raw_info: dict) -> dict:
    """
    Clean and validate node data, evaluating callable expressions where
    necessary.

    Processes the raw node information, removes empty arguments, and evaluates
    any callable expressions provided as string literals.

    Args:
        raw_info (dict): The raw node information dictionary that may contain
            callable expressions as strings.

    Returns:
        dict: The sanitized node information with callable expressions
            evaluated.
    """

    copied_info = copy.deepcopy(raw_info)
    raw_info["data"]["source"] = copy.deepcopy(
        copied_info["data"].get(
            "args",
            {},
        ),
    )
    for key, value in copied_info["data"].get("args", {}).items():
        if value == "":
            raw_info["data"]["args"].pop(key)
            raw_info["data"]["source"].pop(key)
        elif is_callable_expression(value):
            raw_info["data"]["args"][key] = eval(value)
    return raw_info


def build_dag(config: dict) -> ASDiGraph:
    """
    Construct a Directed Acyclic Graph (DAG) from the provided configuration.

    Initializes the graph nodes based on the configuration, adds model nodes
    first, then non-model nodes, and finally adds edges between the nodes.

    Args:
        config (dict): The configuration to build the graph from, containing
            node info such as name, type, arguments, and connections.

    Returns:
        ASDiGraph: The constructed directed acyclic graph.

    Raises:
        ValueError: If the resulting graph is not acyclic.
    """
    dag = ASDiGraph(**{'uuid': str(uuid.uuid4())})

    dag.config = config
    for node_id, node_info in config.items():
        config[node_id] = sanitize_node_data(node_info)

    # Add and init model nodes first
    for node_id, node_info in config.items():
        if (
            NODE_NAME_MAPPING[node_info["name"]].node_type
            == WorkflowNodeType.MODEL
        ):
            dag.add_as_node(node_id, node_info, config)

    # Add and init non-model nodes
    for node_id, node_info in config.items():
        if (
            NODE_NAME_MAPPING[node_info["name"]].node_type
            != WorkflowNodeType.MODEL
        ):
            dag.add_as_node(node_id, node_info, config)

    # Add edges
    for node_id, node_info in config.items():
        outputs = node_info.get("outputs", {})
        for output_key, output_val in outputs.items():
            connections = output_val.get("connections", [])
            for conn in connections:
                target_node_id = conn.get("node")
                # Here it is assumed that the output of the connection is
                # only connected to one of the inputs. If there are more
                # complex connections, modify the logic accordingly
                dag.add_edge(node_id, target_node_id, output_key=output_key)

    # Check if the graph is a DAG
    if not nx.is_directed_acyclic_graph(dag):
        raise ValueError("The provided configuration does not form a DAG.")

    return dag


def build_dag_for_aibigmodel(config: dict, extra_inner_parameter: dict, workflow_id, user_id: str) -> ASDiGraph:
    """
    Construct a Directed Acyclic Graph (DAG) from the provided configuration.

    Initializes the graph nodes based on the configuration, adds model nodes
    first, then non-model nodes, and finally adds edges between the nodes.

    Args:
        config (dict): The configuration to build the graph from, containing
            node info such as name, type, arguments, and connections.

    Returns:
        ASDiGraph: The constructed directed acyclic graph.

    Raises:
        ValueError: If the resulting graph is not acyclic.
    """
    dag = ASDiGraph(**{'uuid': user_id + '_' + str(uuid.uuid4())[:5]})
    dag.workflow_id = workflow_id
    dag.config = config

    for node_id, node_info in config.items():
        config[node_id] = sanitize_node_data(node_info)

    # Add and init model nodes first
    for node_id, node_info in config.items():
        if (
            NODE_NAME_MAPPING[node_info["name"]].node_type
            == WorkflowNodeType.MODEL
        ):
            dag.add_as_node(node_id, node_info, config)

    # Add and init non-model nodes
    for node_id, node_info in config.items():
        if (
            NODE_NAME_MAPPING[node_info["name"]].node_type
            != WorkflowNodeType.MODEL
        ):
            dag.add_as_node(node_id, node_info, config)

    # Add edges
    for node_id, node_info in config.items():
        outputs = node_info.get("outputs", {})
        for output_key, output_val in outputs.items():
            connections = output_val.get("connections", [])
            for conn in connections:
                target_node_id = conn.get("node")
                # Here it is assumed that the output of the connection is
                # only connected to one of the inputs. If there are more
                # complex connections, modify the logic accordingly
                dag.add_edge(node_id, target_node_id, output_key=output_key)

    # Check if the graph is a DAG
    if not nx.is_directed_acyclic_graph(dag):
        raise ValueError("The provided configuration does not form a DAG.")

    return dag
