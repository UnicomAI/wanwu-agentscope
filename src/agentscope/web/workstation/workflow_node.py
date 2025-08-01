# -*- coding: utf-8 -*-
"""Workflow node opt."""
import asyncio
import base64
import copy
import json
import threading
import time
import traceback
import weakref
import jinja2

from abc import ABC, abstractmethod
from enum import IntEnum
from typing import List, Optional
from agentscope.aibigmodel_workflow.loghandler import loghandler
from typing import Any
from concurrent.futures import ThreadPoolExecutor
import agentscope
from agentscope.web.workstation.workflow_utils import (
    kwarg_converter,
    deps_converter,
    dict_converter,
)
from agentscope.service import (
    bing_search,
    google_search,
    read_text_file,
    write_text_file,
    execute_python_code,
    ServiceFactory,
    api_request,
    service_status,
)

from agentscope.service.web.apiservice import api_request_for_big_model, api_request_for_big_model_stream
from agentscope.service.web.mcp_client import MCPClient
from agentscope.web.workstation.workflow_utils import WorkflowNodeStatus
from agentscope.aibigmodel_workflow.config import LLM_URL, RAG_URL, FILE_GENERATE_URL, FILE_PARSE_URL, GUI_URL, \
    redis_queue

try:
    import networkx as nx
except ImportError:
    nx = None

DEFAULT_FLOW_VAR = "flow"


def remove_duplicates_from_end(lst: list) -> list:
    """remove duplicates element from end on a list"""
    seen = set()
    result = []
    for item in reversed(lst):
        if item not in seen:
            seen.add(item)
            result.append(item)
    result.reverse()
    return result


def parse_json_to_dict(extract_text: str) -> dict:
    # Parse the content into JSON object
    try:
        parsed_json = json.loads(extract_text, strict=False)
        if not isinstance(parsed_json, dict):
            raise Exception(f"json text type({type(parsed_json)}) is not dict")
        return parsed_json
    except json.decoder.JSONDecodeError as e:
        raise e


class ASDiGraph(nx.DiGraph):
    """
    A class that represents a directed graph, extending the functionality of
    networkx's DiGraph to suit specific workflow requirements in AgentScope.

    This graph supports operations such as adding nodes with associated
    computations and executing these computations in a topological order.

    Attributes:
        nodes_not_in_graph (set): A set of nodes that are not included in
        the computation graph.
    """

    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        """
        Initialize the ASDiGraph instance.
        """
        super().__init__(*args, **kwargs)
        self.nodes_not_in_graph = set()

        # Prepare the header of the file with necessary imports and any
        # global definitions
        self.imports = [
            "import agentscope",
        ]

        self.inits = [
            'agentscope.init(loghandler.logger_level="DEBUG")',
            f"{DEFAULT_FLOW_VAR} = None",
        ]

        self.execs = ["\n"]
        self.config = {}
        kwargs.setdefault('uuid', None)
        self.uuid = kwargs['uuid']
        self.workflow_id = ""
        # 支持流式NodeList，用于结束节点获取订阅消息
        self.predecessor_node_id = []
        # TODO: 未来将内置参数全部移到sys_params_pool
        self.sys_params_pool = {}
        self.params_pool = {}
        self.conditions_pool = {}
        self.selected_branch = -1

    def clear(self):
        """清理图中所有节点引用"""
        for node_id in self.nodes:
            self.nodes[node_id]["opt"] = None

        if isinstance(self.nodes, dict):
            self.nodes.clear()

    def generate_node_param_real(self, param_spec: dict) -> dict:
        param_name = param_spec['name']

        if self.params_pool and param_spec['value']['type'] == 'ref':
            reference_node_name = param_spec['value']['content']['ref_node_id']
            reference_param_name = param_spec['value']['content']['ref_var_name']
            if self.nodes[reference_node_name]["opt"].running_status == WorkflowNodeStatus.RUNNING_SKIP:
                return {param_name: None}

            # 防御性检查
            if reference_node_name not in self.params_pool:
                raise Exception(f"{reference_node_name} error, the params_pool empty")

            if reference_param_name not in self.params_pool[reference_node_name]:
                raise Exception(f"{reference_node_name} params_pool invalid, missing key=reference_param_name, "
                                f"values={self.params_pool[reference_node_name]}")
            param_value = self.params_pool[reference_node_name][reference_param_name]
            return {param_name: param_value}

        return {param_name: param_spec['value']['content']}

    # 获取开始节点系统参数
    def generate_setting_param_real(self) -> dict:
        # TODO: 未来获取开始节点中所有的系统参数
        self.log_info(f"sys_params_pool: {self.sys_params_pool}")
        if self.sys_params_pool:
            return self.sys_params_pool

        return {}

    def generate_node_param_real_for_switch_input(self, param_spec: dict) -> dict:
        for condition in param_spec['conditions']:
            if not condition['left'] and not condition['right']:
                raise Exception("Switch node format invalid")
            if condition['left']['value']['type'] == 'ref':
                reference_node_name = condition['left']['value']['content']['ref_node_id']
                reference_param_name = condition['left']['value']['content']['ref_var_name']
                param_value = self.params_pool[reference_node_name][reference_param_name]
                condition['left']['value']['content'] = param_value
            elif condition['right']['value']['type'] == 'ref':
                reference_node_name = condition['right']['value']['content']['ref_node_id']
                reference_param_name = condition['right']['value']['content']['ref_var_name']
                param_value = self.params_pool[reference_node_name][reference_param_name]
                condition['right']['value']['content'] = param_value

        return param_spec

    def generate_and_execute_condition_python_code(self, switch_node_id, condition_data, branch):
        logic = condition_data['logic']
        target_node_id = condition_data['target_node_id']
        if not target_node_id:
            raise Exception("分支器节点存在未连接的分支")
        conditions = condition_data['conditions']
        condition_str = ""

        # conditions为空列表且之前的条件不满足时，代表条件为else
        if not conditions:
            if (switch_node_id not in self.conditions_pool or
                    all(value is False for value in self.conditions_pool[switch_node_id].values())):
                self.update_conditions_pool_with_running_node(switch_node_id, target_node_id, True)
            else:
                self.update_conditions_pool_with_running_node(switch_node_id, target_node_id, False)
        else:
            for i, condition in enumerate(conditions):
                left = condition['left']
                right = condition['right']
                operator = condition['operator']
                # 右边参数类型支持ref和literal
                right_data_type = right['value']['type']

                left_value = left['value']['content']
                right_value = right['value']['content']

                condition_str += self.generate_operator_comparison(operator, left_value, right_value, right_data_type)

                if i < len(conditions) - 1:
                    condition_str += f" {logic} "
            self.log_info(f"======= Switch Node condition_str {condition_str}")
            try:
                # 使用 eval 函数进行求值
                condition_func = eval(condition_str)
            except Exception as e:
                error_message = f"条件语句错误: {e}"
                raise Exception(error_message)

            # 确认当前条件成功的分支
            if self.selected_branch == -1:
                self.selected_branch = branch + 1 if condition_func else -1

            # 添加执行结果到conditions_pool中，后续节点进行判断
            self.update_conditions_pool_with_running_node(switch_node_id, target_node_id, condition_func)

    def generate_operator_comparison(self, operator, left_value, right_value, data_type) -> str:
        if data_type == "ref":
            if isinstance(left_value, str):
                left_value = f"{repr(left_value)}"
            elif isinstance(left_value, list):
                left_value = repr(json.dumps(left_value))
            if isinstance(right_value, str):
                right_value = f"{repr(right_value)}"
            switcher = {
                'eq': f"{left_value} == {right_value}",
                'not_eq': f"{left_value} != {right_value}",
                'len_ge': f"len({left_value}) >= len({right_value})",
                'len_gt': f"len({left_value}) > len({right_value})",
                'len_le': f"len({left_value}) <= len({right_value})",
                'len_lt': f"len({left_value}) < len({right_value})",
                'empty': f"{left_value} is None",
                'not_empty': f"{left_value} is not None",
                'in': f"{right_value} in {left_value}",
                'not_in': f"{right_value} not in {left_value}"
            }
        else:
            if isinstance(left_value, str):
                left_value = f"{repr(left_value)}"
            elif isinstance(left_value, list):
                left_value = repr(json.dumps(left_value))
            switcher = {
                'eq': f"{left_value} == '{right_value}'",
                'not_eq': f"{left_value} != '{right_value}'",
                'len_ge': f"len({left_value}) >= {right_value}",
                'len_gt': f"len({left_value}) > {right_value}",
                'len_le': f"len({left_value}) <= {right_value}",
                'len_lt': f"len({left_value}) < {right_value}",
                'empty': f"{left_value} is None",
                'not_empty': f"{left_value} is not None",
                'in': f"'{right_value}' in {left_value}",
                'not_in': f"'{right_value}' not in {left_value}"
            }
        try:
            return switcher.get(operator)
        except KeyError:
            raise ValueError(f"Unsupported operator: {operator}")

    def generate_node_param_spec(param_spec: dict) -> dict:
        param_name = param_spec['name']
        return {param_name: param_spec['value']['content']}

    def generate_node_param_real_for_aggregation_input(self, param_spec: dict) -> dict:
        if "variables" in param_spec:
            # 这是一个分组，需要处理其中的所有变量
            result = {}
            for param_spec in param_spec["variables"]:
                param_name = param_spec["name"]

                if self.params_pool and param_spec['value']['type'] == 'ref':
                    loghandler.logger.info(f"聚合param_spec:{param_spec}")
                    reference_node_name = param_spec['value']['content']['ref_node_id']
                    reference_param_name = param_spec['value']['content']['ref_var_name']

                    if self.nodes[reference_node_name]["opt"].running_status == WorkflowNodeStatus.RUNNING_SKIP:
                        result[param_name] = None
                        continue

                    # 防御性检查
                    if reference_node_name not in self.params_pool:
                        raise Exception(
                            f"{reference_node_name} params_pool invalid, missing key={reference_param_name}, "
                            f"values={self.params_pool.get(reference_node_name, {})}")

                    result[param_name] = self.params_pool[reference_node_name][reference_param_name]
                else:
                    result[param_name] = param_spec['value']['content']
            loghandler.logger.info(f"聚合result:{result}")
            return result

    def confirm_current_node_running_status(self, node_id, input_params) -> (str, bool):
        predecessors_list = list(self.predecessors(node_id))
        # 0. 适配单节点, 节点无前驱节点且输入参数为空时，节点为等待状态
        if len(input_params) == 0 and not predecessors_list:
            return WorkflowNodeStatus.INIT, False
        # 1. 节点前驱节点存在失败节点, 节点为等待状态
        if any(
                self.nodes[node]["opt"].running_status == WorkflowNodeStatus.FAILED for node
                in predecessors_list):
            return WorkflowNodeStatus.INIT, False
        # 2. 节点前驱节点存在Switch/Intention节点, 且节点所在分支满足条件或不存在在Switch/Intention分支后，
        # 优先级1 (前序节点存在未运行节点不影响当前节点状态)
        if self.validate_node_condition(node_id, predecessors_list):
            return WorkflowNodeStatus.RUNNING, True
        # 3. 节点前驱节点存在Switch/Intention节点, 且节点所在分支不满足条件, 置为未运行状态，节点返回空值
        # 3.1 节点前驱节点不存在Switch/Intention节点, 且节点前序节点存在未运行状态时，该节点状态为未运行
        if not self.validate_predecessor_condition(node_id, predecessors_list):
            return WorkflowNodeStatus.RUNNING_SKIP, False
        # 3.2 节点前驱节点不存在Switch/Intention节点, 且节点前序节点均为运行时，该节点为运行
        else:
            return WorkflowNodeStatus.RUNNING, True

    def validate_node_condition(self, node_id, predecessors_list) -> bool:
        if not self.conditions_pool:
            return True

        self.log_info(f"Switch Node判断池 {self.conditions_pool}")
        switch_node = next(
            (item for item in predecessors_list if self.nodes[item]["opt"].node_type
             in (WorkflowNodeType.SWITCH,
                 WorkflowNodeType.Intention)), None)

        if switch_node not in self.conditions_pool:
            return False

        if switch_node and self.conditions_pool[switch_node][node_id]:
            return True
        else:
            return False

    def validate_predecessor_condition(self, node_id, predecessors_list) -> bool:
        predecessor_status = {}
        predecessor_node_type = {}
        for item in predecessors_list:
            predecessor_status[item] = self.nodes[item]["opt"].running_status
            predecessor_node_type[item] = self.nodes[item]["opt"].node_type

        self.log_info(
            f"节点 {node_id}, predecessor_status: {predecessor_status}, "
            f"predecessor_node_type: {predecessor_node_type}")

        # 如果全部前驱节点处于 'running_skip' 状态，则不执行当前节点
        if all(s == WorkflowNodeStatus.RUNNING_SKIP for s in predecessor_status.values()):
            return False

        # 检查前驱节点是否有运行成功的节点，如果前驱节点全部为成功节点，则当前节点为"running"并且排除分支器和意图节点
        if (all(s in {WorkflowNodeStatus.SUCCESS, WorkflowNodeStatus.RUNNING_SKIP}
                for s in predecessor_status.values()) and not
        any(t in {WorkflowNodeType.SWITCH, WorkflowNodeType.Intention}
            for t in predecessor_node_type.values())):
            return True

        return False

    def generate_node_param_real_for_api_input(self, param_spec: dict) -> (dict, dict):
        query_param_result, json_body_param_result = {}, {}

        param_name = param_spec['name']
        param_spec.setdefault('extra', {})
        param_spec['extra'].setdefault('location', '')
        param_type_location = param_spec['extra']['location']

        if param_type_location == 'query':
            param_result = self.generate_node_param_real(param_spec)
            query_param_result = param_result
        elif param_type_location == 'body':
            param_result = self.generate_node_param_real(param_spec)
            json_body_param_result = param_result
        else:
            raise Exception("param: {param_spec} location type: {param_type_location} invalid")

        return query_param_result, json_body_param_result

    def generate_node_param_real_for_mcp_input(self, param_spec: dict) -> dict:
        # 验证参数规范结构
        param_name = param_spec.get('name')
        # 确保extra字段存在并获取位置类型
        param_spec.setdefault('extra', {})
        param_type_location = param_spec['extra'].get('location', '')

        # 只允许body类型的参数
        if param_type_location != 'body':
            raise Exception("param: {param_spec} location type: {param_type_location} invalid")

        # 生成参数并返回
        return self.generate_node_param_real(param_spec)

    # 初始化用户自定义变量池
    def init_params_pool_with_running_node(self, node_id: str):
        # 注意到多个节点并发运行时，字典params_pool是共享变量，
        # 但是由于GIL的机制天然保证了多线程之间的串行运行顺序，以及每个节点只操作字典params_pool的各自不同key，这里简单起见，可以不加锁
        # TODO 加 threading lock
        self.params_pool.setdefault(node_id, {})
        return

    # 初始化系统变量池
    def init_sys_params_pool_with_running_node(self, node_id: str):
        # 注意到多个节点并发运行时，字典params_pool是共享变量，
        # 但是由于GIL的机制天然保证了多线程之间的串行运行顺序，以及每个节点只操作字典params_pool的各自不同key，这里简单起见，可以不加锁
        # TODO 加 threading lock
        self.sys_params_pool.setdefault(node_id, {})
        return

    # 更新用户自定义变量池
    def update_params_pool_with_running_node(self, node_id: str, node_output_params: dict):
        # 注意到多个节点并发运行时，字典params_pool是共享变量，
        # 但是由于GIL的机制天然保证了多线程之间的串行运行顺序，以及每个节点只操作字典params_pool的各自不同key，这里简单起见，可以不加锁
        # TODO 加 threading lock
        self.params_pool[node_id] |= node_output_params

    # 更新系统变量池
    def update_sys_params_pool_with_running_node(self, node_output_params: dict):
        # 注意到多个节点并发运行时，字典params_pool是共享变量，
        # 但是由于GIL的机制天然保证了多线程之间的串行运行顺序，以及每个节点只操作字典params_pool的各自不同key，这里简单起见，可以不加锁
        # TODO 加 threading lock
        self.sys_params_pool |= node_output_params

    def update_conditions_pool_with_running_node(self, switch_node_id, target_node_id: str, condition_result: bool):
        # 注意到多个节点并发运行时，字典params_pool是共享变量，
        # 但是由于GIL的机制天然保证了多线程之间的串行运行顺序，以及每个节点只操作字典params_pool的各自不同key，这里简单起见，可以不加锁
        # TODO 加 threading lock
        self.conditions_pool.setdefault(switch_node_id, {})

        # 当switch节点有除当前分支之外分支连到了同一节点，且另一分支执行结果为True时，不覆盖条件判断池中的值
        if (target_node_id in self.conditions_pool[switch_node_id] and
                self.conditions_pool[switch_node_id][target_node_id] is True):
            self.conditions_pool[switch_node_id][target_node_id] = True
            return

        # 条件从第一个到最后一个分支优先级排列，前面的条件优先级更高
        if not any(value is True for value in self.conditions_pool[switch_node_id].values()):
            self.conditions_pool[switch_node_id][target_node_id] = condition_result
        else:
            self.conditions_pool[switch_node_id][target_node_id] = False

    @staticmethod
    def get_static_auth_token(data: dict) -> str:
        return data.get('staticAuthToken')

    # TODO 待优化适配List&Object
    @staticmethod
    def generate_node_param_spec(param_spec: dict) -> dict:
        param_name = param_spec['name']
        return {param_name: param_spec['value']['content']}

    @staticmethod
    def set_initial_nodes_result(config: dict) -> list:
        nodes = config.get("nodes", [])
        node_result = []
        for node in nodes:
            node_result_dict = {
                "node_id": node["id"],
                "node_status": "",
                "node_message": "",
                "node_type": node["type"],
                "inputs": {},
                "outputs": {},
                "node_execute_cost": ""
            }
            node_result.append(node_result_dict)
        return node_result

    @staticmethod
    def update_nodes_with_values(nodes, output_values, input_values, status_values, message_values):
        for index, node in enumerate(nodes):
            node_id = node['node_id']
            node['node_status'] = status_values.get(node_id, WorkflowNodeStatus.INIT)
            node['node_message'] = message_values.get(node_id, "")
            node['inputs'] = input_values.get(node_id, {})
            node['outputs'] = output_values.get(node_id, {})
            nodes[index] = node

        return nodes

    @staticmethod
    def get_nodes_wrong_status_message(dag_id: str, nodes: list) -> str:
        if not isinstance(nodes, list):
            return f'dag: {dag_id}, nodes_result nodes is not list: {nodes}'

        failed_nodes = [
            node for node in nodes
            if node.get('node_status') not in [
                WorkflowNodeStatus.SUCCESS, WorkflowNodeStatus.RUNNING_SKIP, WorkflowNodeStatus.RUNNING]
        ]

        failed_node_to_message = {}
        for node in failed_nodes:
            # 提取所有失败节点的错误信息
            node_id = node['node_id']
            node_message = node['node_message']
            failed_node_to_message[node_id] = node_message

        if len(failed_node_to_message) == 0:
            return ''
        else:
            return f'dag: {dag_id}, ' + str(failed_node_to_message)

    def save(self, save_filepath: str = "", ) -> None:
        if len(self.config) > 0:
            # Write the script to file
            with open(save_filepath, "w", encoding="utf-8") as file:
                json.dump(self.config, file)

    def run(self) -> None:
        """
        Execute the computations associated with each node in the graph.

        The method initializes AgentScope, performs a topological sort of
        the nodes, and then runs each node's computation sequentially using
        the outputs from its predecessors as inputs.
        """
        # agentscope.init(logger_level="DEBUG")
        sorted_nodes = list(nx.topological_sort(self))
        sorted_nodes = [
            node_id
            for node_id in sorted_nodes
            if node_id not in self.nodes_not_in_graph
        ]
        loghandler.logger.info(f"sorted_nodes: {sorted_nodes}")
        loghandler.logger.info(f"nodes_not_in_graph: {self.nodes_not_in_graph}")

        # Cache output
        values = {}

        # Run with predecessors outputs
        for node_id in sorted_nodes:
            inputs = [
                values[predecessor]
                for predecessor in self.predecessors(node_id)
            ]
            if not inputs:
                values[node_id] = self.exec_node(node_id, None)
            elif len(inputs):
                # Note: only support exec with the first predecessor now
                values[node_id] = self.exec_node(node_id, inputs[0])
            else:
                raise ValueError("Too many predecessors!")

    def run_with_param(self, input_param: dict, config: dict) -> (Any, dict):
        """
        Execute the computations associated with each node in the graph.

        The method initializes AgentScope, performs a topological sort of
        the nodes, and then runs each node's computation sequentially using
        the outputs from its predecessors as inputs.
        """
        # agentscope.init(logger_level="DEBUG")

        sorted_nodes_generations = list(nx.topological_generations(self))
        # loghandler.logger.info(f"topological generation sorted nodes: {sorted_nodes_generations}")

        total_input_values, total_output_values, total_status_values, total_message_values = {}, {}, {}, {}
        output_values = {}
        # Run with predecessors outputs
        for nodes_each_generation in sorted_nodes_generations:
            # 串行运行
            # loghandler.logger.info(f"nodes_each_generation: {nodes_each_generation}")
            if len(nodes_each_generation) == 1:
                for node_id in nodes_each_generation:
                    inputs = [
                        copy.deepcopy(output_values[predecessor])
                        for predecessor in self.predecessors(node_id)
                    ]

                    # 运行节点，并保存输出参数
                    all_predecessor_node_output_params = {}
                    for i, predecessor_node_output_params in enumerate(inputs):
                        all_predecessor_node_output_params |= predecessor_node_output_params
                    # 特殊情况：注意开始节点
                    if len(all_predecessor_node_output_params) == 0:
                        all_predecessor_node_output_params = input_param
                    output_values[node_id] = self.exec_node(node_id, all_predecessor_node_output_params)
                continue

            # 并发运行
            if len(nodes_each_generation) > 1:
                node_and_inputparams = [[], []]
                for node_id in nodes_each_generation:
                    inputs = [
                        copy.deepcopy(output_values[predecessor])
                        for predecessor in self.predecessors(node_id)
                    ]

                    all_predecessor_node_output_params = {}
                    for i, predecessor_node_output_params in enumerate(inputs):
                        all_predecessor_node_output_params |= predecessor_node_output_params
                    # 注意到这里一定不是开始节点，所以省略 all_predecessor_node_output_params = input_param
                    node_and_inputparams[0].append(node_id)
                    node_and_inputparams[1].append(all_predecessor_node_output_params)

                # 运行节点，并保存输出参数
                with ThreadPoolExecutor() as executor:
                    res = executor.map(self.exec_node, node_and_inputparams[0], node_and_inputparams[1])
                    for index, result in enumerate(res):
                        node_id = node_and_inputparams[0][index]
                        output_values[node_id] = result
                continue

            # 异常代码区
            raise Exception("dag图拓扑排序失败！")

        end_node_id = -1
        for nodes_each_generation in sorted_nodes_generations:
            for node_id in nodes_each_generation:
                # 保存各个节点的信息
                total_input_values[node_id] = self.nodes[node_id]["opt"].input_params
                total_output_values[node_id] = self.nodes[node_id]["opt"].output_params
                total_status_values[node_id] = self.nodes[node_id]["opt"].running_status
                total_message_values[node_id] = self.nodes[node_id]["opt"].running_message
                end_node_id = node_id

        # 初始化节点运行结果并更新
        nodes_result = ASDiGraph.set_initial_nodes_result(config)
        updated_nodes_result = ASDiGraph.update_nodes_with_values(
            nodes_result, total_output_values, total_input_values, total_status_values, total_message_values)
        # loghandler.logger.info(f"workflow total running result: {updated_nodes_result}")
        self.clear()
        return total_input_values[end_node_id], updated_nodes_result

    def run_with_param_for_streaming(self, input_param: dict, config: dict) -> (Any, list):
        """
        Execute the computations associated with each node in the graph.

        The method initializes AgentScope, performs a topological sort of
        the nodes, and then runs each node's computation sequentially using
        the outputs from its predecessors as inputs.
        """
        # agentscope.init(logger_level="DEBUG")

        sorted_nodes_generations = list(nx.topological_generations(self))
        self.log_info(f"topological generation sorted nodes: {sorted_nodes_generations}")

        total_input_values, total_output_values, total_status_values, total_message_values = {}, {}, {}, {}
        output_values = {}
        # Run with predecessors outputs
        for nodes_each_generation in sorted_nodes_generations:
            # 串行运行
            # loghandler.logger.info(f"nodes_each_generation: {nodes_each_generation}")
            if len(nodes_each_generation) == 1:
                for node_id in nodes_each_generation:
                    inputs = [
                        copy.deepcopy(output_values[predecessor])
                        for predecessor in self.predecessors(node_id)
                    ]

                    # 运行节点，并保存输出参数
                    all_predecessor_node_output_params = {}
                    for i, predecessor_node_output_params in enumerate(inputs):
                        all_predecessor_node_output_params |= predecessor_node_output_params
                    # 特殊情况：注意开始节点
                    if len(all_predecessor_node_output_params) == 0:
                        all_predecessor_node_output_params = input_param
                    output_values[node_id] = self.exec_node(node_id, all_predecessor_node_output_params)
                continue

            # 并发运行
            if len(nodes_each_generation) > 1:
                node_and_inputparams = [[], []]
                for node_id in nodes_each_generation:
                    inputs = [
                        copy.deepcopy(output_values[predecessor])
                        for predecessor in self.predecessors(node_id)
                    ]

                    all_predecessor_node_output_params = {}
                    for i, predecessor_node_output_params in enumerate(inputs):
                        all_predecessor_node_output_params |= predecessor_node_output_params
                    # 注意到这里一定不是开始节点，所以省略 all_predecessor_node_output_params = input_param
                    node_and_inputparams[0].append(node_id)
                    node_and_inputparams[1].append(all_predecessor_node_output_params)

                # 运行节点，并保存输出参数
                with ThreadPoolExecutor() as executor:
                    res = executor.map(self.exec_node, node_and_inputparams[0], node_and_inputparams[1])
                    for index, result in enumerate(res):
                        node_id = node_and_inputparams[0][index]
                        output_values[node_id] = result
                continue

            # 异常代码区
            raise Exception(f"dag: {self.uuid}, dag图拓扑排序失败！")

        end_node_id = -1
        for nodes_each_generation in sorted_nodes_generations:
            for node_id in nodes_each_generation:
                # 保存各个节点的信息
                total_input_values[node_id] = self.nodes[node_id]["opt"].input_params
                total_output_values[node_id] = self.nodes[node_id]["opt"].output_params
                total_status_values[node_id] = self.nodes[node_id]["opt"].running_status
                total_message_values[node_id] = self.nodes[node_id]["opt"].running_message
                end_node_id = node_id

        # 初始化节点运行结果并更新
        nodes_result = ASDiGraph.set_initial_nodes_result(config)
        updated_nodes_result = ASDiGraph.update_nodes_with_values(
            nodes_result, total_output_values, total_input_values, total_status_values, total_message_values)
        self.log_info(f"workflow total running result: {updated_nodes_result}")
        self.clear()
        return total_input_values[end_node_id], updated_nodes_result

    def compile(  # type: ignore[no-untyped-def]
            self,
            compiled_filename: str = "",
            **kwargs,
    ) -> str:
        """Compile DAG to a runnable python code"""

        def format_python_code(code: str) -> str:
            try:
                from black import FileMode, format_str

                loghandler.logger.debug("Formatting Code with black...")
                return format_str(code, mode=FileMode())
            except Exception:
                return code

        self.inits[
            0
        ] = f'agentscope.init(loghandler.logger_level="DEBUG", {kwarg_converter(kwargs)})'

        sorted_nodes = list(nx.topological_sort(self))
        sorted_nodes = [
            node_id
            for node_id in sorted_nodes
            if node_id not in self.nodes_not_in_graph
        ]

        for node_id in sorted_nodes:
            node = self.nodes[node_id]
            self.execs.append(node["compile_dict"]["execs"])

        header = "\n".join(self.imports)

        # Remove duplicate import
        new_imports = remove_duplicates_from_end(header.split("\n"))
        header = "\n".join(new_imports)
        body = "\n    ".join(self.inits + self.execs)

        main_body = f"def main():\n    {body}"

        # Combine header and body to form the full script
        script = (
            f"{header}\n\n\n{main_body}\n\nif __name__ == "
            f"'__main__':\n    main()\n"
        )

        formatted_code = format_python_code(script)

        loghandler.logger.info(f"compiled_filename: {compiled_filename}")
        if len(compiled_filename) > 0:
            # Write the script to file
            with open(compiled_filename, "w", encoding="utf-8") as file:
                file.write(formatted_code)
        return formatted_code

    # pylint: disable=R0912
    def add_as_node(
            self,
            node_id: str,
            node_info: dict,
            config: dict,
    ) -> Any:
        """
        Add a node to the graph based on provided node information and
        configuration.

        Args:
            node_id (str): The identifier for the node being added.
            node_info (dict): A dictionary containing information about the
                node.
            config (dict): Configuration information for the node dependencies.

        Returns:
            The computation object associated with the added node.
        """
        node_cls = NODE_NAME_MAPPING[node_info.get("name", "")]
        # 适配新增节点
        if node_cls.node_type not in [
            WorkflowNodeType.MODEL,
            WorkflowNodeType.AGENT,
            WorkflowNodeType.MESSAGE,
            WorkflowNodeType.PIPELINE,
            WorkflowNodeType.COPY,
            WorkflowNodeType.SERVICE,
            WorkflowNodeType.START,
            WorkflowNodeType.END,
            WorkflowNodeType.PYTHON,
            WorkflowNodeType.API,
            WorkflowNodeType.LLM,
            WorkflowNodeType.SWITCH,
            WorkflowNodeType.RAG,
            WorkflowNodeType.GUIAGENT,
            WorkflowNodeType.EndStreaming,
            WorkflowNodeType.LLMStreaming,
            WorkflowNodeType.FileGenerate,
            WorkflowNodeType.FileParse,
            WorkflowNodeType.MCPClient,
            WorkflowNodeType.Intention,
            WorkflowNodeType.Aggregation,
            WorkflowNodeType.TemplateTransform
        ]:
            raise NotImplementedError(node_cls)

        if self.has_node(node_id):
            return self.nodes[node_id]["opt"]

        # Init dep nodes
        deps = [str(n) for n in node_info.get("data", {}).get("elements", [])]

        # Exclude for dag when in a Group
        if node_cls.node_type != WorkflowNodeType.COPY:
            self.nodes_not_in_graph = self.nodes_not_in_graph.union(set(deps))

        dep_opts = []
        for dep_node_id in deps:
            if not self.has_node(dep_node_id):
                dep_node_info = config[dep_node_id]
                self.add_as_node(dep_node_id, dep_node_info, config)
            dep_opts.append(self.nodes[dep_node_id]["opt"])

        node_opt = node_cls(
            node_id=node_id,
            opt_kwargs=node_info["data"].get("args", {}),
            source_kwargs=node_info["data"].get("source", {}),
            dep_opts=dep_opts,
            dag_obj=self,
        )

        # Add build compiled python code
        compile_dict = node_opt.compile()

        self.add_node(
            node_id,
            opt=node_opt,
            compile_dict=compile_dict,
            **node_info,
        )

        # Insert compile information to imports and inits
        self.imports.append(compile_dict["imports"])

        if node_cls.node_type == WorkflowNodeType.MODEL:
            self.inits.insert(1, compile_dict["inits"])
        else:
            self.inits.append(compile_dict["inits"])
        return node_opt

    def exec_node(self, node_id: str, x_in: Any) -> Any:
        """
        Execute the computation associated with a given node in the graph.

        Args:
            node_id (str): The identifier of the node whose computation is
                to be executed.
            x_in: The input to the node's computation. Defaults to None.

        Returns:
            The output of the node's computation.
        """
        opt = self.nodes[node_id]["opt"]
        # loghandler.logger.info(f"{node_id}, {opt}, x_in: {x_in}")
        if not x_in and not isinstance(x_in, dict):
            raise Exception(f'x_in type:{type(x_in)} not dict')
        out_values = opt(**x_in)
        return out_values

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"dag: {self.uuid}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"dag: {self.uuid}, {message}")


class WorkflowNodeType(IntEnum):
    """Enum for workflow node.
    添加了两种类型，START和END类型"""

    MODEL = 0
    AGENT = 1
    PIPELINE = 2
    SERVICE = 3
    MESSAGE = 4
    COPY = 5
    START = 6
    END = 7
    PYTHON = 8
    API = 9
    LLM = 10
    SWITCH = 11
    RAG = 12
    GUIAGENT = 13
    LLMStreaming = 14
    EndStreaming = 15
    FileGenerate = 16
    FileParse = 17
    MCPClient = 18
    Intention = 19
    Aggregation = 20
    TemplateTransform = 21


class WorkflowNode(ABC):
    """
    Abstract base class representing a generic node in a workflow.

    WorkflowNode is designed to be subclassed with specific logic implemented
    in the subclass methods. It provides an interface for initialization and
    execution of operations when the node is called.
    """

    node_type = None

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
    ) -> None:
        """
        Initialize nodes. Implement specific initialization logic in
        subclasses.
        """
        self.node_id = node_id
        self.opt_kwargs = opt_kwargs
        self.source_kwargs = source_kwargs
        self.dep_opts = dep_opts
        self.dep_vars = [opt.var_name for opt in self.dep_opts]
        self.var_name = f"{self.node_type.name.lower()}_{self.node_id}"

    def __call__(self, x: dict = None):  # type: ignore[no-untyped-def]
        """
        Performs the operations of the node. Implement specific logic in
        subclasses.
        """

    @abstractmethod
    def compile(self) -> dict:
        """
        Compile Node to python executable code dict
        """
        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }


# 20240813
# 新增的开始节点
class StartNode(WorkflowNode):
    """
    开始节点代表输入参数列表.
    source_kwargs字段，用户定义了该节点的输入和输出变量名、类型、value

        "inputs": [],
        "outputs": [
            {
                "name": "poi",
                "type": "string",
                "desc": "节点功能中文描述",
                "object_schema": null,
                "list_schema": null,
                "value": {
                    "type": "generated",
                    "content": null
                }
            },
        ],
        "settings": {}

    """

    node_type = WorkflowNodeType.START

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
            dag_obj: Optional[ASDiGraph] = None,
    ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        # opt_kwargs 包含用户定义的节点输入变量
        self.input_params = {}
        self.output_params = {}
        # init --> running -> success/failed
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ""
        if dag_obj is None:
            self.dag_obj = None
        else:
            self.dag_obj = weakref.proxy(dag_obj)
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid

    def __call__(self, *args, **kwargs):
        # 注意，这里是开始节点，所以不需要判断入参是否为空

        self.running_status = WorkflowNodeStatus.RUNNING
        try:
            self.run(*args, **kwargs)
            self.running_status = WorkflowNodeStatus.SUCCESS
            return self.output_params
        except Exception as err:
            self.log_error(f'{traceback.format_exc()}')
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = f'{repr(err)}'
            return {}
        finally:
            # 运行结束后，清理资源
            self.clear()

    def run(self, *args, **kwargs):
        if len(kwargs) == 0:
            raise Exception("input param dict kwargs empty")

        # 1. 建立参数映射
        self.dag_obj.init_params_pool_with_running_node(self.node_id)
        self.dag_obj.init_sys_params_pool_with_running_node(self.node_id)

        params_dict = self.opt_kwargs

        # 输入参数类型一致性校验
        self.validate_params(params_dict, kwargs)
        # 用户自定义参数映射
        for i, param_spec in enumerate(params_dict['outputs']):
            # param_spec 举例
            # {
            #     "name": "apiKeywords",
            #     "type": "string",
            #     "desc": "",
            #     "required": false,
            #     "value": {
            #         "type": "ref",
            #         "content": {
            #             "ref_node_id": "4daf0d1a33af497e9819fe515133eb5f",
            #             "ref_var_name": "keywords"
            #         }
            #     },
            #     "object_schema": null,
            #     "list_schema": null,
            # }

            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict = self.dag_obj.generate_node_param_real(param_spec)
            self.output_params |= param_one_dict

        # 2. 解析实际的取值
        for k, v in kwargs.items():
            if k not in self.output_params:
                continue
            self.output_params[k] = v

        # 更新用户自定义变量到自定义变量池中
        self.dag_obj.update_params_pool_with_running_node(self.node_id, self.output_params)

        # 更新系统变量到系统变量池中
        self.dag_obj.update_sys_params_pool_with_running_node(params_dict['settings'])

        self.log_info(
            f"{self.var_name}, run success, input params: {self.input_params}, output params: {self.output_params}")
        return self.output_params

    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None

    def __str__(self) -> str:
        message = f'dag: {self.dag_id}, {self.var_name}'
        return message

    def compile(self) -> dict:
        """
        入参在这里初始化.
        Returns:

        """
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        if 'inputs' not in self.opt_kwargs:
            raise Exception("inputs key not found")
        if 'outputs' not in self.opt_kwargs:
            raise Exception("outputs key not found")
        if 'settings' not in self.opt_kwargs:
            raise Exception("settings key not found")

        if not isinstance(self.opt_kwargs['inputs'], list):
            raise Exception(f"inputs:{self.opt_kwargs['inputs']} type is not list")
        if not isinstance(self.opt_kwargs['outputs'], list):
            raise Exception(f"outputs:{self.opt_kwargs['outputs']} type is not list")
        if not isinstance(self.opt_kwargs['settings'], dict):
            raise Exception(f"settings:{self.opt_kwargs['settings']} type is not dict")

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    @staticmethod
    def validate_params(param_dict, args):
        def validate_list_items(value, expected_type):
            if expected_type == 'string':
                return all(isinstance(item, str) for item in value)
            elif expected_type == 'integer':
                return all(isinstance(item, int) for item in value)
            elif expected_type == 'boolean':
                return all(isinstance(item, bool) for item in value)
            # TODO: 扩展多个类型
            else:
                raise ValueError(f"不支持的类型: {expected_type}")

        # 遍历 param_dict 中的 outputs
        for output in param_dict.get('outputs', []):
            if output.get('type') == 'array':
                param_name = output.get('name')
                list_schema_type = output.get('list_schema', {}).get('type')

                # 检查输入参数是否存在
                if param_name in args:
                    param_value = args[param_name]

                    # 检查参数值是否为列表且列表中的每个元素是否符合预期类型
                    if not isinstance(param_value, list) or not validate_list_items(param_value, list_schema_type):
                        raise Exception(f"参数 '{param_name}' 要求必须是一个由 {list_schema_type} 组成的数组")

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")


class EndNode(WorkflowNode):
    """
    结束节点只有输入没有输出
    """
    node_type = WorkflowNodeType.END

    def __init__(self,
                 node_id: str,
                 opt_kwargs: dict,
                 source_kwargs: dict,
                 dep_opts: list,
                 dag_obj: Optional[ASDiGraph] = None,
                 ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        # opt_kwargs 包含用户定义的节点输入变量
        self.input_params = {}
        self.output_params = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ""
        if dag_obj is None:
            self.dag_obj = None
        else:
            self.dag_obj = weakref.proxy(dag_obj)
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid

    def __call__(self, *args, **kwargs) -> dict:
        # 判断当前节点的运行状态
        predecessors_list = list(self.dag_obj.predecessors(self.node_id))
        self.log_info(f"predecessors_list {predecessors_list}")
        # 0. 节点无前驱节点且输入参数为空时，节点为等待状态
        if len(kwargs) == 0 and not predecessors_list:
            self.running_status = WorkflowNodeStatus.INIT
            self.clear()
            return {}
        # 1. 节点前驱节点存在失败节点, 节点为等待状态
        if any(
                self.dag_obj.nodes[node]["opt"].running_status in WorkflowNodeStatus.FAILED for node
                in predecessors_list):
            self.running_status = WorkflowNodeStatus.INIT
            self.clear()
            return {}
        try:
            self.run(*args, **kwargs)
            self.running_status = WorkflowNodeStatus.SUCCESS
            # 尾节点，没有输出值
            return {}
        except Exception as err:
            self.log_error(f'{traceback.format_exc()}')
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = f'{repr(err)}'
            return {}
        finally:
            # 运行结束后，清理资源
            self.clear()

    def run(self, *args, **kwargs):
        self.dag_obj.init_params_pool_with_running_node(self.node_id)

        # 1. 建立参数映射
        params_dict = self.opt_kwargs
        for i, param_spec in enumerate(params_dict['inputs']):
            # param_spec 举例
            # {
            #     "name": "apiKeywords",
            #     "type": "string",
            #     "desc": "",
            #     "required": false,
            #     "value": {
            #         "type": "ref",
            #         "content": {
            #             "ref_node_id": "4daf0d1a33af497e9819fe515133eb5f",
            #             "ref_var_name": "keywords"
            #         }
            #     },
            #     "object_schema": null,
            #     "list_schema": null,
            # }

            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict = self.dag_obj.generate_node_param_real(param_spec)
            self.input_params |= param_one_dict

        # 注意，尾节点不需要再放到全局变量池里
        self.log_info(
            f"{self.var_name}, run success, input params: {self.input_params}, output params: {self.output_params}")
        return self.input_params

    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None

    def __str__(self) -> str:
        message = f'dag: {self.dag_id}, {self.var_name}'
        return message

    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs

        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")


class EndStreamingNode(WorkflowNode):
    """
    结束节点只有输入没有输出
    """
    node_type = WorkflowNodeType.EndStreaming

    def __init__(self,
                 node_id: str,
                 opt_kwargs: dict,
                 source_kwargs: dict,
                 dep_opts: list,
                 dag_obj: Optional[ASDiGraph] = None,
                 ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        # opt_kwargs 包含用户定义的节点输入变量
        self.input_params = {}
        self.output_params = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ""
        if dag_obj is None:
            self.dag_obj = None
        else:
            self.dag_obj = weakref.proxy(dag_obj)
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid
            self.workflow_id = self.dag_obj.workflow_id

        # 流式处理相关属性
        self._processing_thread = None  # 处理线程
        # 添加线程锁确保状态安全
        self._status_lock = threading.Lock()

        # 流式队列配置
        self.input_queues = []  # 上游节点队列模式
        self.output_queue = f"final_stream:{self.workflow_id}_{self.dag_id}"
        self._stream_active = threading.Event()  # 流状态控制

    def __call__(self, *args, **kwargs) -> dict:
        predecessors_list = list(self.dag_obj.predecessors(self.node_id))
        # 找出所有前置节点为流式输出节点的队列
        stream_nodes = set(predecessors_list) & set(self.dag_obj.predecessor_node_id)

        self.log_info(f"所有前置节点为stream的节点{stream_nodes}")
        # 判断当前节点的运行状态
        self.input_queues = [f"llm_stream:{self.dag_id}:{node}" for node in stream_nodes]
        self.log_info(f"End节点需要订阅的队列{self.input_queues}")

        # 0. 节点无前驱节点且输入参数为空时，节点为等待状态
        if len(kwargs) == 0 and not predecessors_list:
            self.running_status = WorkflowNodeStatus.INIT
            # 这里强行remove的原因是，保证上游llm流式节点的队列也被正确清理
            self.clear()
            return {}
        # 1. 节点前驱节点存在失败节点, 节点为等待状态
        if any(
                self.dag_obj.nodes[node]["opt"].running_status in WorkflowNodeStatus.FAILED for node
                in predecessors_list):
            self.running_status = WorkflowNodeStatus.INIT
            # 这里强行remove的原因是，保证上游llm流式节点的队列也被正确清理
            self.clear()
            return {}

        with self._status_lock:
            try:
                self.run()

                # 启动异步处理线程
                self.log_info(f"End节点进入异步处理线程")
                self.log_info(f"End节点进入异步处理线程异步流处理列表: {self.input_queues}")
                self._stream_active.set()
                self.running_status = WorkflowNodeStatus.RUNNING

                self._processing_thread = threading.Thread(
                    target=self._async_stream_processor,  # 传递函数对象，而不是调用结果
                    daemon=True
                )
                self._processing_thread.start()

                # 返回最终的redis输出队列供接口转换SSE数据流
                self.input_params = self.output_queue
                return {self.output_queue: "OK"}

            except Exception as err:
                exausted_err = traceback.format_exc()
                self.log_error(f'{exausted_err}')
                self._handle_failure(exausted_err)
                return {}

    def clear(self):
        """析构时确保停止处理"""
        self._cleanup_resources()
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None
        self._stream_active.clear()

    def __str__(self) -> str:
        message = f'dag: {self.dag_id}, {self.var_name}'
        return message

    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs

        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    def run(self, *args, **kwargs):
        self.dag_obj.init_params_pool_with_running_node(self.node_id)

        # 1. 建立参数映射
        params_dict = self.opt_kwargs
        for i, param_spec in enumerate(params_dict['inputs']):

            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict = self.dag_obj.generate_node_param_real(param_spec)
            self.input_params |= param_one_dict

        # 注意，尾节点不需要再放到全局变量池里
        self.log_info(
            f"{self.var_name}, run success, input params: {self.input_params}, output params: {self.output_params}")
        return self.input_params

    def _async_stream_processor(self):
        """异步流处理核心"""
        active_queues = self.input_queues
        self.log_info(f"异步流处理列表: {active_queues}, node type: {self.node_type}, node id: {self.node_id}")
        try:
            # 初始化消费者
            self.log_info(f"开始处理输出流output_queue: {self.output_queue}")

            # 判断队列是否有效
            non_empty_queue_list, empty_queue_list = self._get_none_empty_and_empty_queue_list(active_queues)
            self.log_info(f"non empty queue list: {non_empty_queue_list}, empty queue list {empty_queue_list}")
            if len(non_empty_queue_list) == 0:
                raise Exception(f"大模型节点(流式)没有输出，流式画布打字机效果要求，至少有一个上游节点流式输出")

            start_time = time.time()
            while self._stream_active.is_set() and non_empty_queue_list:
                # 轮询所有上游队列
                for queue in list(non_empty_queue_list):
                    self.log_info(f"End节点进入异步处理线程, 正在轮询{queue}")
                    # 阻塞式获取消息
                    message = redis_queue.dequeue(queue, timeout=30)
                    self.log_info(f"End节点获取信息{message}")
                    if not message:
                        continue

                    self._send_message({}, message)

                    # 处理结束标记
                    if message == 'data: [DONE]':
                        self.log_info(f"End节点处理结束标记{message}")
                        non_empty_queue_list.remove(queue)
                        continue

                    # 防御性手段，最多允许流式输出10分钟，超时就截断
                    if time.time() - start_time > 600:
                        self.log_info(f"End节点处理超时，结束处理")
                        self._send_message({}, '超时结束: 最长允许10分钟')
                        self._send_message({}, 'data: [DONE]')
                        non_empty_queue_list.remove(queue)
                        continue

            self.log_info(f"结束处理输出流output_queue")
            self.running_status = WorkflowNodeStatus.SUCCESS

        except Exception as e:
            self._send_message({
                "event_type": "error",
                "message": str(e),
                "trace": traceback.format_exc()
            }, '')
            self._send_message({}, 'data: [DONE]')
            self.running_status = WorkflowNodeStatus.FAILED
        finally:
            self.clear()

    def _get_none_empty_and_empty_queue_list(self, active_queues) -> (list, list):
        """获取非空队列和空队列列表"""
        if not active_queues:
            return [], []

        non_empty_queue_list = []
        empty_queue_list = []
        max_empty_attempts = 600  # 最大尝试次数
        empty_attempt_counts = {queue: 0 for queue in active_queues}  # 每个队列的空检查计数
        while active_queues and len(non_empty_queue_list) + len(empty_queue_list) < len(active_queues):
            # 轮询所有活跃队列
            for queue in list(active_queues):  # 使用list()创建副本避免在迭代中修改
                if queue in non_empty_queue_list:
                    continue

                queue_length = redis_queue.llen(queue)
                if queue_length != 0:
                    non_empty_queue_list.append(queue)
                    continue

                # 如果队列为空，增加尝试计数
                empty_attempt_counts[queue] += 1
                if empty_attempt_counts[queue] >= max_empty_attempts:
                    empty_queue_list.append(queue)
                    self.log_info(f"队列{queue}空闲超过最大尝试次数，放入空队列统计")
                else:
                    # 继续计数
                    time.sleep(0.5)
                    continue

        return non_empty_queue_list, empty_queue_list

    def _send_message(self, data: dict, data_str: str):
        """通过单例队列发送消息"""
        if not self._stream_active.is_set():
            self.log_info("流控制失效")
            return

        try:
            if data:
                redis_queue.enqueue(self.output_queue, json.dumps(data, ensure_ascii=False))
            else:
                redis_queue.enqueue(self.output_queue, data_str)
        except Exception as e:
            self.log_error(f"消息入队失败: {str(e)}")

    def _cleanup_resources(self):
        """资源清理"""
        try:
            # 清理上游队列
            for q in self.input_queues:
                redis_queue.delete_queue(q, dag_id=self.dag_id)

            # 延迟清理输出队列
            time.sleep(3)  # 确保消息被消费
            redis_queue.delete_queue(self.output_queue, dag_id=self.dag_id)

        except Exception as e:
            self.log_error(f"[WARN] 资源清理异常: {str(e)}")

    def _handle_failure(self, exausted_err: str):
        """异常处理"""
        self.log_error(f"节点执行失败: {exausted_err}")
        self.running_status = WorkflowNodeStatus.FAILED
        self.running_message = exausted_err

        self._send_message({
            "event_type": "error",
            "message": exausted_err
        }, '')

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")


class PythonServiceUserTypingNode(WorkflowNode):
    """
    Execute Python Node,支持用户输入
    使用 source_kwargs 获取用户输入的 Python 代码
    这个代码将作为 execute_python_code 函数的 code 参数传入。
    """

    node_type = WorkflowNodeType.PYTHON

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
            dag_obj: Optional[ASDiGraph] = None,
    ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        # opt_kwargs 包含用户定义的节点输入变量
        self.input_params = {'params': {}}
        self.output_params = {}
        self.output_params_spec = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ""
        if dag_obj is None:
            self.dag_obj = None
        else:
            self.dag_obj = weakref.proxy(dag_obj)
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid

        self.python_code = ""

    def __call__(self, *args, **kwargs) -> dict:
        # 判断当前节点的运行状态
        self.running_status, is_running = self.dag_obj.confirm_current_node_running_status(self.node_id, kwargs)
        if not is_running:
            self.clear()
            return {}

        try:
            self.run(*args, **kwargs)
            self.running_status = WorkflowNodeStatus.SUCCESS
            return self.output_params
        except Exception as err:
            self.log_error(f'{traceback.format_exc()}')
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = f'{repr(err)}'
            return {}
        finally:
            # 运行结束后，清理资源
            self.clear()

    def run(self, *args, **kwargs):
        self.dag_obj.init_params_pool_with_running_node(self.node_id)

        # 1. 建立参数映射
        params_dict = self.opt_kwargs
        for i, param_spec in enumerate(params_dict['inputs']):
            # param_spec 举例
            # {
            #     "name": "apiKeywords",
            #     "type": "string",
            #     "desc": "",
            #     "required": false,
            #     "value": {
            #         "type": "ref",
            #         "content": {
            #             "ref_node_id": "4daf0d1a33af497e9819fe515133eb5f",
            #             "ref_var_name": "keywords"
            #         }
            #     },
            #     "object_schema": null,
            #     "list_schema": null,
            # }

            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict = self.dag_obj.generate_node_param_real(param_spec)
            self.input_params['params'] |= param_one_dict

        self.log_info(f"self.input_params, {self.input_params}")
        self.log_info(f"self.output_params, {self.output_params}")
        # 2. 运行python解释器代码        # 单个节点调试运行场景"
        if len(self.output_params_spec) == 0:
            if len(kwargs) > 0 and len(self.input_params['params']) > 0:
                raise Exception("single node debug run, but real input param not empty list")
            self.input_params['params'] = kwargs

        response = execute_python_code(
            self.python_code, use_docker=False, extra_readonly_input_params=self.input_params['params'])
        if response.status == service_status.ServiceExecStatus.ERROR:
            raise Exception(str(response.content))

        # 单个节点调试场景，不解析出参，直接返回调试的结果
        if len(self.output_params_spec) == 0:
            self.output_params = response.content
            self.log_info(
                f"{self.var_name}, run success, input params: {self.input_params}, output params: {self.output_params}")
            return self.output_params

        self.output_params = response.content
        # 检查返回值是否对应
        for k in self.output_params_spec:
            if k not in self.output_params:
                raise Exception(f"user defined output parameter '{k}' not found in 'output_params' code return value:"
                                f"{self.output_params}")

        self.dag_obj.update_params_pool_with_running_node(self.node_id, self.output_params)
        self.log_info(
            f"{self.var_name}, run success, input params: {self.input_params}, output params: {self.output_params}")
        return self.output_params

    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None

    def __str__(self) -> str:
        message = f'dag: {self.dag_id}, {self.var_name}'
        return message

    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs

        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")

        base64_python_code = params_dict['settings'].get('code', None)
        if not base64_python_code:
            raise Exception("python code empty")

        isBase64 = False
        try:
            base64.b64encode(base64.b64decode(base64_python_code)) == base64_python_code
            isBase64 = True
        except Exception:
            isBase64 = False
            raise Exception("python code str not base64")

        self.python_code = base64.b64decode(base64_python_code).decode('utf-8')
        if self.python_code == "":
            raise Exception("python code empty")

        for i, param_spec in enumerate(params_dict['outputs']):
            # param_spec 举例
            # {
            #     "name": "apiKeywords",
            #     "type": "string",
            #     "desc": "",
            #     "required": false,
            #     "value": {
            #         "type": "ref",
            #         "content": {
            #             "ref_node_id": "4daf0d1a33af497e9819fe515133eb5f",
            #             "ref_var_name": "keywords"
            #         }
            #     },
            #     "object_schema": null,
            #     "list_schema": null,
            # }

            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict = ASDiGraph.generate_node_param_spec(param_spec)
            self.output_params_spec |= param_one_dict
        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")


# 新增通用api调用节点
# api请求的所需参数从setting中获取
class ApiNode(WorkflowNode):
    """
    API GET Node for executing HTTP requests using the api_request function.
    """

    node_type = WorkflowNodeType.API

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
            dag_obj: Optional[ASDiGraph] = None,
    ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        self.input_params = {}
        self.output_params = {}
        self.output_params_spec = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ''
        if dag_obj is None:
            self.dag_obj = None
        else:
            self.dag_obj = weakref.proxy(dag_obj)
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid

        # GET or POST
        self.api_type = ""
        self.api_url = ""
        self.api_header = {}
        self.input_params_for_query = {}
        self.input_params_for_body = {}

    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs

        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")

        if 'url' not in params_dict['settings']:
            raise Exception("url key not found in settings")
        if 'http_method' not in params_dict['settings']:
            raise Exception("http_method key not found in settings")
        if 'headers' not in params_dict['settings']:
            raise Exception("headers key not found in settings")

        self.api_type = params_dict['settings']['http_method']
        self.api_url = params_dict['settings']['url']
        self.api_header = params_dict['settings']['headers']

        if self.api_type not in {"GET", "POST"}:
            raise Exception("http type: {self.api_type} invalid")
        if self.api_url == '':
            raise Exception("http url empty")
        if not isinstance(self.api_header, dict):
            raise Exception(f"header:{self.api_header} type is not dict")

        for i, param_spec in enumerate(params_dict['inputs']):
            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_spec.setdefault('extra', {})
            # 防御性措施
            param_spec['extra'].setdefault('location', 'query')
            if param_spec['extra']['location'] not in {'query', 'body'}:
                raise Exception("input param: {param_spec} extra-location not found('query' or 'body')")

        for i, param_spec in enumerate(params_dict['outputs']):
            # param_spec 举例
            # {
            #     "name": "apiKeywords",
            #     "type": "string",
            #     "desc": "",
            #     "required": false,
            #     "value": {
            #         "type": "ref",
            #         "content": {
            #             "ref_node_id": "4daf0d1a33af497e9819fe515133eb5f",
            #             "ref_var_name": "keywords"
            #         }
            #     },
            #     "object_schema": null,
            #     "list_schema": null,
            # }

            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict = ASDiGraph.generate_node_param_spec(param_spec)
            self.output_params_spec |= param_one_dict

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    def __call__(self, *args, **kwargs):
        # 判断当前节点的运行状态
        self.running_status, is_running = self.dag_obj.confirm_current_node_running_status(self.node_id, kwargs)
        if not is_running:
            self.clear()
            return {}

        try:
            self.run(*args, **kwargs)
            self.running_status = WorkflowNodeStatus.SUCCESS
            return self.output_params
        except Exception as err:
            self.log_error(f'{traceback.format_exc()}')
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = f'{repr(err)}'
            return {}
        finally:
            self.clear()

    def run(self, *args, **kwargs):
        self.dag_obj.init_params_pool_with_running_node(self.node_id)

        # 1. 建立参数映射
        params_dict = self.opt_kwargs
        for i, param_spec in enumerate(params_dict['inputs']):
            # param_spec 举例
            # {
            #     "name": "apiKeywords",
            #     "type": "string",
            #     "desc": "",
            #     "required": false,
            #     "value": {
            #         "type": "ref",
            #         "content": {
            #             "ref_node_id": "4daf0d1a33af497e9819fe515133eb5f",
            #             "ref_var_name": "keywords"
            #         }
            #     },
            #     "object_schema": null,
            #     "list_schema": null,
            #     "extra": {
            #         "location": "query"
            #     }
            # }

            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict_for_query, param_one_dict_for_body \
                = self.dag_obj.generate_node_param_real_for_api_input(param_spec)

            if not isinstance(param_one_dict_for_query, dict):
                raise Exception("input param: {param_one_dict_for_query} type not dict")
            if not isinstance(param_one_dict_for_body, dict):
                raise Exception("input param: {param_one_dict_for_body} type not dict")

            self.input_params |= param_one_dict_for_query
            self.input_params |= param_one_dict_for_body
            self.input_params_for_query |= param_one_dict_for_query
            self.input_params_for_body |= param_one_dict_for_body

        # 2. 使用 api_request 函数进行 API 请求设置
        response = api_request(url=self.api_url, method=self.api_type, headers=self.api_header,
                               params=self.input_params_for_query, json=self.input_params_for_body)
        if response.status == service_status.ServiceExecStatus.ERROR:
            raise Exception(str(response.content))

        # 3. 拆包解析
        # 单个节点调试场景，不解析出参，直接返回调试的结果
        if len(self.output_params_spec) == 0:
            self.log_info(
                f"{self.var_name}, run success, input params: {self.input_params}, output params: {self.output_params}")
            self.output_params = response.content
            return self.output_params

        self.output_params = response.content
        # 检查返回值是否对应
        for k in self.output_params_spec:
            if k not in self.output_params:
                self.log_error(f"api response: {self.output_params}")
                raise Exception(f"user defined output parameter {k} not found in api response")

        self.dag_obj.update_params_pool_with_running_node(self.node_id, self.output_params)
        self.log_info(
            f"{self.var_name}, run success, input params: {self.input_params}, output params: {self.output_params}")
        return self.output_params

    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")


class LLMNode(WorkflowNode):
    """
    LLM Node.
    """

    node_type = WorkflowNodeType.LLM

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
            dag_obj: Optional[ASDiGraph] = None,
    ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        self.input_params = {}
        self.output_params = {}
        self.output_params_spec = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ''
        if dag_obj is None:
            self.dag_obj = None
        else:
            self.dag_obj = weakref.proxy(dag_obj)
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid

        # GET or POST
        self.api_url = LLM_URL
        self.model = ""
        self.api_header = {}
        self.input_params_for_query = {}
        self.input_params_for_body = {}
        self.is_user_defined_llm = False

    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs
        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")

        if 'model' not in params_dict['settings']:
            raise Exception("model not found in settings")
        if 'headers' not in params_dict['settings']:
            raise Exception("headers key not found in settings")

        # 私有云适配用户自定义model url
        model_endpoint_url = params_dict['settings'].get('model_endpoint_url', None)
        if model_endpoint_url:
            self.api_url = model_endpoint_url
            self.is_user_defined_llm = True

        # 调用内部函数，将LLM Model请求参数组合为大模型可以识别的参数
        params_dict = self.convert_llm_request(params_dict)
        self.api_header = params_dict['settings']['headers']
        if not isinstance(self.api_header, dict):
            raise Exception(f"header:{self.api_header} type is not dict")

        for i, param_spec in enumerate(params_dict['inputs']):
            param_spec.setdefault('extra', {})
            param_spec['extra'].setdefault('location', '')
            if param_spec['extra'].get('location', '') not in {'query', 'body'}:
                raise Exception("input param: {param_spec} extra-location not found('query' or 'body')")

            # python关键字，不支持作为变量名
            if param_spec['name'] in {'input', 'return'}:
                raise Exception("input param: {param_spec} name not support")

        for i, param_spec in enumerate(params_dict['outputs']):
            param_one_dict = ASDiGraph.generate_node_param_spec(param_spec)
            self.output_params_spec |= param_one_dict

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    def __call__(self, *args, **kwargs):
        # 判断当前节点的运行状态
        self.running_status, is_running = self.dag_obj.confirm_current_node_running_status(self.node_id, kwargs)
        if not is_running:
            self.clear()
            return {}

        try:
            self.run(*args, **kwargs)
            self.running_status = WorkflowNodeStatus.SUCCESS
            return self.output_params
        except Exception as err:
            self.log_error(f'{traceback.format_exc()}')
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = f'{repr(err)}'
            return {}
        finally:
            self.clear()

    def run(self, *args, **kwargs):
        self.dag_obj.init_params_pool_with_running_node(self.node_id)

        # 1. 建立参数映射
        params_dict = self.opt_kwargs

        for i, param_spec in enumerate(params_dict['inputs']):
            param_one_dict_for_query, param_one_dict_for_body \
                = self.dag_obj.generate_node_param_real_for_api_input(param_spec)

            if not isinstance(param_one_dict_for_query, dict):
                raise Exception("input param: {param_one_dict_for_query} type not dict")
            if not isinstance(param_one_dict_for_body, dict):
                raise Exception("input param: {param_one_dict_for_body} type not dict")

            self.input_params |= param_one_dict_for_query
            self.input_params |= param_one_dict_for_body
            self.input_params_for_query |= param_one_dict_for_query
            self.input_params_for_body |= param_one_dict_for_body

        # 大模型参数校验
        temperature = self.input_params_for_body.get('temperature', None)
        if not (0 <= temperature <= 1):
            raise Exception("温度参数错误，应该在[0,1]之间")
        top_p = self.input_params_for_body.get('top_p', None)
        if not (0 <= top_p <= 1):
            raise Exception("多样性参数错误，应该在[0,1]之间")
        presence_penalty = self.input_params_for_body.get('presence_penalty', None)
        if not (1 <= presence_penalty <= 1.3):
            raise Exception("重复惩罚参数错误，应该在[1,1.3]之间")
        messages = self.input_params_for_body.get('messages', None)
        if not messages:
            raise Exception("大模型节点提示词为空,请填写提示词")
        if 'headers' not in params_dict['settings']:
            raise Exception("headers key not found in settings")
        # prompt提示词拼接
        self.generate_prompt(*args, **kwargs)

        model_info_url = f"http://bff-service:6668/callback/v1/model/{self.model}"
        loghandler.logger.info(f"API MODEL Request - URL: {model_info_url}")
        response = api_request(url=model_info_url, method='GET')
        loghandler.logger.info(f"API MODEL Response: {response}")
        if response.status == service_status.ServiceExecStatus.ERROR:
            lid = self.get_log_id(response)
            self.log_error(f"api response: {str(response.content)}, log_id: {lid}")
            raise Exception(f"{self.model} get model info  error")
        model_response = self.convert_model_response(response.content)
        loghandler.logger.info(f"API MODEL Response: {model_response}")


        # 只保留需要的键值对
        self.input_params_for_body = {
            'model': model_response['model'],
            'messages': self.input_params_for_body['messages'],
            'temperature': self.input_params_for_body['temperature'],
            'top_p': self.input_params_for_body['top_p'],
            'presence_penalty': self.input_params_for_body['presence_penalty']
        }
        # 获取LLM的静态Token
        sys_params = self.dag_obj.generate_setting_param_real()

        # static_token = ASDiGraph.get_static_auth_token(sys_params)

        # if static_token:
        #     self.api_header["Authorization"] = f"Bearer {static_token}"
        #     self.api_header["Content-Type"] = "application/json"
        # else:
        #     self.log_error(f"Token失效或未提供Token")
        #     raise Exception(f"Token失效或未提供Token, 检查参数是否正确")

        # 2. 使用 api_request 函数进行 API 请求

        self.api_header["Content-Type"] = "application/json"
        # 修改 url
        self.api_url = f"http://bff-service:6668/callback/v1/model/{self.model}/chat/completions"

        loghandler.logger.info(f"API Request - URL: {self.api_url}")
        loghandler.logger.info(f"API Request - Headers: {self.api_header}")
        loghandler.logger.info(f"API Request - Body: {self.input_params_for_body}")
        response = api_request_for_big_model(url=self.api_url, method='POST', headers=self.api_header,
                                             data=self.input_params_for_body)

        loghandler.logger.info(f"API Response: {response}")
        if response.status == service_status.ServiceExecStatus.ERROR:
            lid = self.get_log_id(response)
            self.log_error(f"api response: {str(response.content)}, log_id: {lid}")
            raise Exception(f"{self.model} is not available")
        if response.content is None or response.content['choices'] is None:
            lid = self.get_log_id(response)
            self.log_error(f"api response: {str(response.content)}, log_id: {lid}")
            raise Exception("Can not get response from selected model. Please check the parameters")
        # 3. 拆包解析
        self.output_params = LLMNode.convert_llm_response(response.content)
        # 检查返回值是否对应
        for k in self.output_params_spec:
            if k not in self.output_params:
                self.log_error(f"api response: {self.output_params}")
                raise Exception(f"user defined output parameter {k} not found in api response")

        log_id = self.get_log_id(response)

        self.dag_obj.update_params_pool_with_running_node(self.node_id, self.output_params)
        self.log_info(
            f"{self.var_name}, run success, input params: {self.input_params}, output params: {self.output_params}, log_id: {log_id}")
        return self.output_params

    def get_log_id(self, response) -> str:
        if response and hasattr(response, 'content') and \
                isinstance(response.content, dict):
            return response.content.get("id", "")
        return ""

    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None

    def generate_prompt(self, *args, **kwargs):
        messages = self.input_params_for_body["messages"]

        try:
            # 使用正则表达式严格匹配 {{param}}，确保 {{ 和 }} 成对出现
            import re
            pattern = re.compile(r'\{\{([^{}]+)\}\}')  # 仅匹配 {{xxx}}，且中间不能包含 { 或 }

            def replace_match(match):
                key = match.group(1).strip()  # 提取变量名，并去除两端空格
                if key in self.input_params_for_body:
                    return str(self.input_params_for_body[key])
                else:
                    raise KeyError(f"Key '{{{{{key}}}}}' not found in input parameters")

            # 只替换成对的 {{xxx}}，其他 {x} 或 {{x}、{x}} 均不处理
            updated_messages = pattern.sub(replace_match, messages)

            # 构造模型可识别的消息格式
            self.input_params_for_body["messages"] = [{'role': 'user', 'content': updated_messages}]

        except KeyError as e:
            error_message = f"Missing key for formatting: {e}"
            raise ValueError(error_message)

        return self.input_params_for_body

    def convert_llm_request(self, origin_params_dict: dict) -> dict:

        # 将name为Input替换为LLM识别的messages
        if 'inputs' in origin_params_dict:
            for item in origin_params_dict['inputs']:
                if item['name'] == 'input':
                    item['name'] = 'messages'
        else:
            raise KeyError("Missing 'inputs' in origin_params_dict")

        # 提取model名称作为大模型请求的入参
        if 'settings' in origin_params_dict and 'model' in origin_params_dict['settings']:
            self.model = origin_params_dict['settings']['model']
        else:
            raise KeyError("Missing 'settings' or 'model' in origin_params_dict")

        return origin_params_dict

    @staticmethod
    def convert_llm_response(origin_params_dict: dict) -> dict:
        content = origin_params_dict['choices'][0]['message']['content']
        updated_params_dict = {'content': content}
        return updated_params_dict

    @staticmethod
    def convert_model_response(model_result_dict: dict) -> dict:
        content = model_result_dict['data']['model']
        model_info = {'model': content}
        return model_info

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def peek_llm_server_firstly(self):
        # check openai llm whether ping success

        check_header = {}
        check_data = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": "hi"
                }
            ],
            "stream": False
        }

        # 获取LLM的静态Token
        sys_params = self.dag_obj.generate_setting_param_real()
        # static_token = ASDiGraph.get_static_auth_token(sys_params)
        # if static_token:
        #     check_header["Authorization"] = f"Bearer {static_token}"
        #     check_header["Content-Type"] = "application/json"
        # else:
        #     self.log_error(f"Token失效或未提供Token")
        #     raise Exception(f"Token失效或未提供Token, 检查参数是否正确")

        # ping
        self.log_info(f"ping llm server...")
        response = api_request_for_big_model(url=self.api_url, method='POST', headers=check_header,
                                             data=check_data)

        if response.status == service_status.ServiceExecStatus.ERROR:
            lid = self.get_log_id(response)
            self.log_error(f"ping failed, status code error, api response: {str(response.content)}, log_id: {lid}")
            raise Exception(f"无法访问大模型服务，请检查参数是否正确: {self.api_url}, {self.model}")
        if response.content is None or response.content['choices'] is None:
            lid = self.get_log_id(response)
            self.log_error(f"ping failed, not openai format, api response: {str(response.content)}, log_id: {lid}")
            raise Exception("大模型服务不是openai标准，返回值不符合预期。只支持openai兼容的标准LLM模型")

        self.log_info(f"ping llm server success")
        return


class LLMStreamingNode(WorkflowNode):
    node_type = WorkflowNodeType.LLMStreaming

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
            dag_obj: Optional[ASDiGraph] = None,
    ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        self.input_params = {}
        self.output_params = {}
        self.output_params_spec = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ''
        if dag_obj is None:
            self.dag_obj = None
        else:
            self.dag_obj = weakref.proxy(dag_obj)
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid
            self.workflow_id = self.dag_obj.workflow_id

        # GET or POST
        self.api_url = LLM_URL
        self.model = ""
        self.api_header = {}
        self.input_params_for_query = {}
        self.input_params_for_body = {}
        # 添加线程锁确保状态安全
        self._status_lock = threading.Lock()
        self._stream_active = threading.Event()  # 流状态控制
        self.stream_queue = f"llm_stream:{self.dag_id}:{self.node_id}"  # 生成唯一队列名称
        self.is_user_defined_llm = False

    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs
        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")

        if 'model' not in params_dict['settings']:
            raise Exception("model not found in settings")
        if 'headers' not in params_dict['settings']:
            raise Exception("headers key not found in settings")

        # 私有云适配用户自定义model url
        model_endpoint_url = params_dict['settings'].get('model_endpoint_url', None)
        if model_endpoint_url:
            self.api_url = model_endpoint_url
            self.is_user_defined_llm = True

        # 调用内部函数，将LLM Model请求参数组合为大模型可以识别的参数
        params_dict = self.convert_llm_request(params_dict)
        self.api_header = params_dict['settings']['headers']
        if not isinstance(self.api_header, dict):
            raise Exception(f"header:{self.api_header} type is not dict")

        for i, param_spec in enumerate(params_dict['inputs']):
            param_spec.setdefault('extra', {})
            param_spec['extra'].setdefault('location', '')
            if param_spec['extra'].get('location', '') not in {'query', 'body'}:
                raise Exception("input param: {param_spec} extra-location not found('query' or 'body')")

            # python关键字，不支持作为变量名
            if param_spec['name'] in {'input', 'return'}:
                raise Exception("input param: {param_spec} name not support")

        for i, param_spec in enumerate(params_dict['outputs']):
            param_one_dict = ASDiGraph.generate_node_param_spec(param_spec)
            self.output_params_spec |= param_one_dict

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    def __call__(self, *args, **kwargs):
        # 判断当前节点的运行状态
        self.running_status, is_running = self.dag_obj.confirm_current_node_running_status(self.node_id, kwargs)
        if not is_running:
            self.clear()
            return {}

        # 将当前流式节点的node_id添加到队列中,后续结束节点
        self.dag_obj.predecessor_node_id.append(self.node_id)

        with self._status_lock:

            try:
                # 执行同步预处理
                self.dag_obj.init_params_pool_with_running_node(self.node_id)

                # 发送API请求获取流式响应
                # response_stream = self.run()

                if self.is_user_defined_llm:
                    self.peek_llm_server_firstly()
                self.log_info(f"LLM节点启动异步处理")
                # 启动异步处理线程
                self._stream_active.set()

                self.running_status = WorkflowNodeStatus.RUNNING
                threading.Thread(
                    target=self._async_stream_processor,
                    daemon=True
                ).start()

                self.running_status = WorkflowNodeStatus.SUCCESS
                return {
                    "stream_queue": self.stream_queue,
                    "status": "PROCESSING"
                }
            except Exception as err:
                self.log_error(f'dag: {self.dag_id}, {traceback.format_exc()}')
                self.running_status = WorkflowNodeStatus.FAILED
                self.running_message = f'{repr(err)}'
                return {"status": "ERROR", "message": str(err)}

    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None

    def _async_stream_processor(self):
        """异步处理流式响应"""
        self.dag_obj.init_params_pool_with_running_node(self.node_id)

        # 1. 建立参数映射
        params_dict = self.opt_kwargs

        for i, param_spec in enumerate(params_dict['inputs']):
            param_one_dict_for_query, param_one_dict_for_body \
                = self.dag_obj.generate_node_param_real_for_api_input(param_spec)

            if not isinstance(param_one_dict_for_query, dict):
                raise Exception("input param: {param_one_dict_for_query} type not dict")
            if not isinstance(param_one_dict_for_body, dict):
                raise Exception("input param: {param_one_dict_for_body} type not dict")

            self.input_params |= param_one_dict_for_query
            self.input_params |= param_one_dict_for_body
            self.input_params_for_query |= param_one_dict_for_query
            self.input_params_for_body |= param_one_dict_for_body

        # 大模型参数校验
        temperature = self.input_params_for_body.get('temperature', None)
        if not (0 <= temperature <= 1):
            raise Exception("温度参数错误，应该在[0,1]之间")
        top_p = self.input_params_for_body.get('top_p', None)
        if not (0 <= top_p <= 1):
            raise Exception("多样性参数错误，应该在[0,1]之间")
        presence_penalty = self.input_params_for_body.get('presence_penalty', None)
        if not (1 <= presence_penalty <= 1.3):
            raise Exception("重复惩罚参数错误，应该在[1,1.3]之间")
        messages = self.input_params_for_body.get('messages', None)
        if not messages:
            raise Exception("大模型节点提示词为空,请填写提示词")
        if 'headers' not in params_dict['settings']:
            raise Exception("headers key not found in settings")
        # prompt提示词拼接
        self.generate_prompt()

        # 只保留需要的键值对
        self.input_params_for_body = {
            'model': self.model,
            'stream': True,
            'messages': self.input_params_for_body['messages'],
            'temperature': self.input_params_for_body['temperature'],
            'top_p': self.input_params_for_body['top_p'],
            'presence_penalty': self.input_params_for_body['presence_penalty']
        }
        # 获取LLM的静态Token
        sys_params = self.dag_obj.generate_setting_param_real()

        # static_token = ASDiGraph.get_static_auth_token(sys_params)

        # if static_token:
        #     self.api_header["Authorization"] = f"Bearer {static_token}"
        #     self.api_header["Content-Type"] = "application/json"
        # else:
        #     self.log_error(f"Token失效或未提供Token")
        #     raise Exception(f"Token失效或未提供Token, 检查参数是否正确")

        # 特殊处理，更新一个空的content
        self.output_params = {'content': ""}
        self.dag_obj.update_params_pool_with_running_node(self.node_id, self.output_params)

        self.log_info(f"入参{self.input_params_for_body}")
        self.log_info(f"Header{self.api_header}")

        # 2. 流式响应处理
        response_stream = api_request_for_big_model_stream(url=self.api_url, method='POST', headers=self.api_header,
                                                           data=self.input_params_for_body)

        try:
            aggregated_response = ""

            self.log_info(f"LLM节点启动异步处理:{response_stream}")

            for line in response_stream:
                self.log_info(f"LLM节点启动异步处理内容:{line}")
                if not self._stream_active.is_set():
                    break
                # 处理有效数据
                if line:
                    data = json.loads(line)

                    # 2. 安全地提取内容
                    if data.get("choices"):
                        first_choice = data["choices"][0]
                        delta = first_choice.get("delta", {})
                        content = delta.get("content", "")
                        if content is not None:
                            aggregated_response += str(content)  # 显式转换为字符串
                    self._send_message({}, str(line))

            # 发送结束标记
            self._send_message({}, 'data: [DONE]')

            # 更新输出参数和状态
            output_params = {"content": aggregated_response}
            self._update_output_params(output_params)

            self.log_info(
                f"流式处理完成 - "
                f"节点ID: {self.node_id}, "
                f"总内容长度: {len(aggregated_response)}字符, "
                f"输出内容为: {output_params}"
            )
            self.running_status = WorkflowNodeStatus.SUCCESS

        except Exception as e:
            # 发送错误信息
            exausted_err = traceback.format_exc()
            self._send_message({
                "event_type": "error",
                "message": str(e),
                "trace": exausted_err
            }, '')
            self._send_message({}, 'data: [DONE]')
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = str(e)
            self.log_error(f"流处理异常: {exausted_err}")
        finally:
            self.log_info(f"{self.var_name} 流式处理结束")
            self.clear()
            self._stream_active.clear()

    def _send_message(self, data: dict, data_str: str):
        """通过单例队列发送消息"""
        if not self._stream_active.is_set():
            self.log_info("流控制失效")
            return

        try:
            if data:
                redis_queue.enqueue(self.stream_queue, json.dumps(data, ensure_ascii=False))
            else:
                redis_queue.enqueue(self.stream_queue, data_str)
        except Exception as e:
            self.log_error(f"消息入队失败: {str(e)}")

    def _update_output_params(self, output_params):
        self.output_params = output_params
        self.dag_obj.update_params_pool_with_running_node(self.node_id, output_params)

    def _cleanup_queue(self):
        """安全清理队列"""
        try:
            # 延迟5秒清理确保消息被消费
            time.sleep(5)
            redis_queue.delete_queue(self.stream_queue, dag_id=self.dag_id)
        except Exception as e:
            self.log_error(f"[WARN] 队列清理失败: {str(e)}")

    def convert_llm_request(self, origin_params_dict: dict) -> dict:

        # 将name为Input替换为LLM识别的messages
        if 'inputs' in origin_params_dict:
            for item in origin_params_dict['inputs']:
                if item['name'] == 'input':
                    item['name'] = 'messages'
        else:
            raise KeyError("Missing 'inputs' in origin_params_dict")

        # 提取model名称作为大模型请求的入参
        if 'settings' in origin_params_dict and 'model' in origin_params_dict['settings']:
            self.model = origin_params_dict['settings']['model']
        else:
            raise KeyError("Missing 'settings' or 'model' in origin_params_dict")

        return origin_params_dict

    def generate_prompt(self):
        messages = self.input_params_for_body["messages"]

        try:
            # 替换 {{param}} 为 {param}，使其能被 .format() 识别
            formatted_message = messages.replace("{{", "{").replace("}}", "}")

            # 使用 .format() 进行变量替换
            updated_messages = formatted_message.format(**self.input_params_for_body)

            # 构造模型可识别的消息格式
            self.input_params_for_body["messages"] = [{'role': 'user', 'content': updated_messages}]

        except KeyError as e:
            error_message = f"Missing key for formatting: {e}"
            raise ValueError(error_message)

        return self.input_params_for_body

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def peek_llm_server_firstly(self):
        # check openai llm whether ping success

        check_header = {}
        check_data = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": "hi"
                }
            ],
            "stream": False
        }

        # 获取LLM的静态Token
        sys_params = self.dag_obj.generate_setting_param_real()
        # static_token = ASDiGraph.get_static_auth_token(sys_params)
        # if static_token:
        #     check_header["Authorization"] = f"Bearer {static_token}"
        #     check_header["Content-Type"] = "application/json"
        # else:
        #     self.log_error(f"Token失效或未提供Token")
        #     raise Exception(f"Token失效或未提供Token, 检查参数是否正确")

        # ping
        self.log_info(f"ping llm server...")
        response = api_request_for_big_model(url=self.api_url, method='POST', headers=check_header,
                                             data=check_data)

        if response.status == service_status.ServiceExecStatus.ERROR:
            lid = self.get_log_id(response)
            self.log_error(f"ping failed, status code error, api response: {str(response.content)}, log_id: {lid}")
            raise Exception(f"无法访问大模型服务，请检查参数是否正确: {self.api_url}, {self.model}")
        if response.content is None or response.content['choices'] is None:
            lid = self.get_log_id(response)
            self.log_error(f"ping failed, not openai format, api response: {str(response.content)}, log_id: {lid}")
            raise Exception("大模型服务不是openai标准，返回值不符合预期。只支持openai兼容的标准LLM模型")

        self.log_info(f"ping llm server success")
        return


class SwitchNode(WorkflowNode):
    """
    A node representing a switch-case structure within a workflow.
    SwitchPipelineNode routes the execution to different node nodes
    based on the evaluation of a specified key or condition.
    """

    node_type = WorkflowNodeType.SWITCH

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
            dag_obj: Optional[ASDiGraph] = None,
    ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        # opt_kwargs 包含用户定义的节点输入变量
        self.input_params = {}
        self.output_params = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ""
        if dag_obj is None:
            self.dag_obj = None
        else:
            self.dag_obj = weakref.proxy(dag_obj)
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid

    def __call__(self, *args, **kwargs):
        # 判断当前节点的运行状态
        self.running_status, is_running = self.dag_obj.confirm_current_node_running_status(self.node_id, kwargs)
        if not is_running:
            self.clear()
            return {}

        try:
            self.run(*args, **kwargs)
            self.running_status = WorkflowNodeStatus.SUCCESS
            self.running_message = f'pass to branch {self.dag_obj.selected_branch}'
            return self.output_params
        except Exception as err:
            self.log_error(f'dag: {self.dag_id}, {traceback.format_exc()}')
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = f'{repr(err)}'
            return {}
        finally:
            self.clear()

    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs

        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")

        for item in params_dict["inputs"]:
            if "logic" not in item:
                raise Exception("logic not found in inputs")
            elif "target_node_id" not in item:
                raise Exception("target node not found in inputs")
            elif "conditions" not in item:
                raise Exception("conditions not found in inputs")

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    def run(self, *args, **kwargs):
        self.dag_obj.init_params_pool_with_running_node(self.node_id)
        # 1. 建立参数映射
        params_dict = self.opt_kwargs
        for branch, param_spec in enumerate(params_dict['inputs']):
            param_one_dict = self.dag_obj.generate_node_param_real_for_switch_input(param_spec)
            # 2. 运行SwitchNode转换代码，将json转换为对应判断
            self.dag_obj.generate_and_execute_condition_python_code(self.node_id, param_one_dict, branch)
        return self.output_params

    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")


class RAGNode(WorkflowNode):
    """
    RAG Node.
    """

    node_type = WorkflowNodeType.RAG

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
            dag_obj: Optional[ASDiGraph] = None,
    ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        self.input_params = {}
        self.output_params = {}
        self.output_params_spec = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ''
        if dag_obj is None:
            self.dag_obj = None
        else:
            self.dag_obj = weakref.proxy(dag_obj)
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid

        # GET or POST
        self.api_url = ""
        self.userId = ""
        self.rerank_model_id = ""
        self.knowledgeBase = []
        self.api_header = {}
        self.input_params_for_query = {}
        self.input_params_for_body = {}

    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs
        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")

        # 调用内部函数，将请求参数组合为RAG可以识别的参数
        params_dict = RAGNode.convert_rag_request(params_dict)
        self.api_header = params_dict['settings']['headers']
        self.api_url = params_dict['settings']['url']
        self.knowledgeBase = params_dict['settings']['knowledgeBase']
        self.userId = params_dict['settings']['userId']
        self.rerank_model_id = params_dict['settings']['model']
        if not isinstance(self.api_header, dict):
            raise Exception(f"header:{self.api_header} type is not dict")

        for i, param_spec in enumerate(params_dict['inputs']):
            param_spec.setdefault('extra', {})
            param_spec['extra'].setdefault('location', '')
            if param_spec['extra'].get('location', '') not in {'query', 'body'}:
                raise Exception("input param: {param_spec} extra-location not found('query' or 'body')")

        for i, param_spec in enumerate(params_dict['outputs']):
            param_one_dict = ASDiGraph.generate_node_param_spec(param_spec)
            self.output_params_spec |= param_one_dict

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    def __call__(self, *args, **kwargs):
        # 判断当前节点的运行状态
        self.running_status, is_running = self.dag_obj.confirm_current_node_running_status(self.node_id, kwargs)
        if not is_running:
            return {}

        try:
            self.run(*args, **kwargs)
            self.running_status = WorkflowNodeStatus.SUCCESS
            return self.output_params
        except Exception as err:
            self.log_error(f'{traceback.format_exc()}')
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = f'{repr(err)}'
            return {}
        finally:
            self.clear()

    def run(self, *args, **kwargs):
        self.dag_obj.init_params_pool_with_running_node(self.node_id)

        # 1. 建立参数映射
        params_dict = self.opt_kwargs

        for i, param_spec in enumerate(params_dict['inputs']):
            param_one_dict_for_query, param_one_dict_for_body \
                = self.dag_obj.generate_node_param_real_for_api_input(param_spec)

            if not isinstance(param_one_dict_for_query, dict):
                raise Exception("input param: {param_one_dict_for_query} type not dict")
            if not isinstance(param_one_dict_for_body, dict):
                raise Exception("input param: {param_one_dict_for_body} type not dict")

            self.input_params |= param_one_dict_for_query
            self.input_params |= param_one_dict_for_body
            self.input_params_for_query |= param_one_dict_for_query
            self.input_params_for_body |= param_one_dict_for_body

        # RAG参数校验
        threshold = self.input_params_for_body.get('threshold', None)
        if not (0 <= threshold <= 1):
            raise Exception("过滤阈值参数错误，应该在[0,1]之间")
        top_k = self.input_params_for_body.get('top_k', None)
        if not (0 <= top_k <= 10):
            raise Exception("选取知识条数参数错误，应该在[0,10]之间")
        question = self.input_params_for_body.get('question', None)
        if not question:
            raise Exception("RAG节点问题为空,请填写问题")
        if 'headers' not in params_dict['settings']:
            raise Exception("headers key not found in settings")
        if len(self.knowledgeBase) == 0:
            raise Exception('知识库参数为空，至少选择一个知识库')
        # 2. 使用 api_request 函数进行 API 请求设置
        self.input_params_for_body['knowledgeBase'] = self.knowledgeBase
        self.input_params_for_body['userId'] = self.userId
        self.input_params_for_body['rerank_model_id'] = self.rerank_model_id

        # 适配知识库接口
        if 'top_k' in self.input_params_for_body:
            self.input_params_for_body['topK'] = self.input_params_for_body.pop('top_k')

        response = api_request_for_big_model(url=self.api_url, method='POST', headers=self.api_header,
                                             data=self.input_params_for_body)
        self.log_info(f'RAG service response {response.content}')
        if response.status == service_status.ServiceExecStatus.ERROR:
            raise Exception(str(response.content))
        if response.content is None:
            raise Exception("Can not get response from RAG service")
        if len(response.content['data']['searchList']) == 0 and response.content['code'] != 0:
            raise Exception(f"{response.content['message']}")
        # 3. 拆包解析
        self.output_params = RAGNode.convert_rag_response(response.content)
        # 检查返回值是否对应
        for k in self.output_params_spec:
            if k not in self.output_params:
                self.log_error(f"api response: {self.output_params}")
                raise Exception(f"user defined output parameter {k} not found in api response")

        self.dag_obj.update_params_pool_with_running_node(self.node_id, self.output_params)
        self.log_info(
            f"{self.var_name}, run success, input params: {self.input_params}, output params: {self.output_params}")
        return self.output_params

    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None

    @staticmethod
    def convert_rag_request(origin_params_dict: dict) -> dict:
        # 将name为query替换为RAG识别的question
        for item in origin_params_dict['inputs']:
            if item['name'] == 'query':
                item['name'] = 'question'
        # 添加知识库url地址
        origin_params_dict['settings']['url'] = RAG_URL
        return origin_params_dict

    @staticmethod
    def convert_rag_response(origin_params_dict: dict) -> dict:
        prompt = origin_params_dict['data']['prompt']
        content = origin_params_dict['data']['searchList']
        updated_params_dict = {'prompt': prompt, 'content': content}
        return updated_params_dict

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")


# 新增GUIAgent节点
# api请求的所需参数从setting中获取
class GUIAgentNode(WorkflowNode):
    """
    GUIAgent Node.
    """

    node_type = WorkflowNodeType.GUIAGENT

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
            dag_obj: Optional[ASDiGraph] = None,
    ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        self.input_params = {}
        self.output_params = {}
        self.output_params_spec = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ''
        if dag_obj is None:
            self.dag_obj = None
        else:
            self.dag_obj = weakref.proxy(dag_obj)
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid

        # GUI请求参数初始化
        self.api_url = GUI_URL
        self.api_header = {}
        self.input_params_for_query = {}
        self.input_params_for_body = {}

    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs

        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")

        if 'headers' not in params_dict['settings']:
            raise Exception("headers key not found in settings")

        self.api_header = params_dict['settings']['headers']

        if not isinstance(self.api_header, dict):
            raise Exception(f"header:{self.api_header} type is not dict")

        for i, param_spec in enumerate(params_dict['inputs']):
            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_spec.setdefault('extra', {})
            # 防御性措施
            param_spec['extra'].setdefault('location', 'query')
            if param_spec['extra']['location'] not in {'query', 'body'}:
                raise Exception("input param: {param_spec} extra-location not found('query' or 'body')")

        for i, param_spec in enumerate(params_dict['outputs']):

            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict = ASDiGraph.generate_node_param_spec(param_spec)
            self.output_params_spec |= param_one_dict

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    def __call__(self, *args, **kwargs):
        # 判断当前节点的运行状态
        self.running_status, is_running = self.dag_obj.confirm_current_node_running_status(self.node_id, kwargs)
        if not is_running:
            self.clear()
            return {}

        try:
            self.run(*args, **kwargs)
            self.running_status = WorkflowNodeStatus.SUCCESS
            return self.output_params
        except Exception as err:
            exausted_err = traceback.format_exc()
            self.log_error(f'{exausted_err}')
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = f'{repr(err)}, trace: {exausted_err}'
            return {}
        finally:
            self.clear()

    def run(self, *args, **kwargs):
        self.dag_obj.init_params_pool_with_running_node(self.node_id)

        # 1. 建立参数映射
        params_dict = self.opt_kwargs
        for i, param_spec in enumerate(params_dict['inputs']):
            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict_for_query, param_one_dict_for_body \
                = self.dag_obj.generate_node_param_real_for_api_input(param_spec)

            if not isinstance(param_one_dict_for_query, dict):
                raise Exception("input param: {param_one_dict_for_query} type not dict")
            if not isinstance(param_one_dict_for_body, dict):
                raise Exception("input param: {param_one_dict_for_body} type not dict")

            self.input_params |= param_one_dict_for_query
            self.input_params |= param_one_dict_for_body
            self.input_params_for_query |= param_one_dict_for_query
            self.input_params_for_body |= param_one_dict_for_body

        # GUI节点参数校验
        platform = self.input_params_for_body.get('platform', None)
        if platform not in ["Mobile", "WIN", "MAC"]:
            raise Exception("平台参数错误，应该为 Mobile、WIN 或 MAC")
        current_screenshot_width = self.input_params_for_body.get('current_screenshot_width', None)
        if not current_screenshot_width:
            raise Exception("current_screenshot_width参数为空")
        current_screenshot_height = self.input_params_for_body.get('current_screenshot_height', None)
        if not current_screenshot_height:
            raise Exception("current_screenshot_height参数为空")

        # 参数固定转换为Integer
        try:
            current_screenshot_width = int(current_screenshot_width)
            self.input_params_for_body["current_screenshot_width"] = current_screenshot_width  # 更新为 int 类型
        except (ValueError, TypeError) as e:
            raise Exception(f"current_screenshot_width 需要为整数类型")

        try:
            current_screenshot_height = int(current_screenshot_height)
            self.input_params_for_body["current_screenshot_height"] = current_screenshot_height  # 更新为 int 类型
        except (ValueError, TypeError) as e:
            raise Exception(f"current_screenshot_height 需要为整数类型")

        task = self.input_params_for_body.get('task', None)
        if not task:
            raise Exception("task参数为空")

        if 'headers' not in params_dict['settings']:
            raise Exception("headers key not found in settings")

        # 获取静态Token
        sys_params = self.dag_obj.generate_setting_param_real()

        # static_token = ASDiGraph.get_static_auth_token(sys_params)

        # if static_token:
        #     self.api_header["Authorization"] = f"Bearer {static_token}"
        # else:
        #     self.log_error(f"Token失效或未提供Token")
        #     raise Exception(f"Token失效或未提供Token, 检查参数是否正确")

        # 2. 使用 api_request 函数进行 API 请求设置
        response = api_request(url=self.api_url, method="POST", headers=self.api_header,
                               params=self.input_params_for_query, json=self.input_params_for_body)
        if response.status == service_status.ServiceExecStatus.ERROR:
            raise Exception(str(response.content))

        # 3. 拆包解析
        self.output_params = response.content
        # 检查返回值是否对应
        for k in self.output_params_spec:
            if k not in self.output_params:
                self.log_error(f"api response: {self.output_params}")
                raise Exception(f"user defined output parameter {k} not found in api response")

        self.dag_obj.update_params_pool_with_running_node(self.node_id, self.output_params)
        self.log_info(
            f"{self.var_name}, run success, algo: {self.input_params.get('algo')}, "
            f"platform: {self.input_params.get('platform')}, "
            f"task: {self.input_params.get('task')}, history: {self.input_params.get('history')}, "
            f"output params: {self.output_params}")
        return self.output_params

    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")


# 新增文件生成节点
class FileGenerateNode(WorkflowNode):
    """
    FileGenerate Node.
    """

    node_type = WorkflowNodeType.FileGenerate

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
            dag_obj: Optional[ASDiGraph] = None,
    ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        self.input_params = {}
        self.output_params = {}
        self.output_params_spec = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ''
        if dag_obj is None:
            self.dag_obj = None
        else:
            self.dag_obj = weakref.proxy(dag_obj)
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid
            self.workflow_id = self.dag_obj.workflow_id

        # GUI请求参数初始化
        self.api_url = FILE_GENERATE_URL
        self.api_header = {}
        self.input_params_for_query = {}
        self.input_params_for_body = {}

    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs

        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")

        if 'headers' not in params_dict['settings']:
            raise Exception("headers key not found in settings")

        self.api_header = params_dict['settings']['headers']

        if not isinstance(self.api_header, dict):
            raise Exception(f"header:{self.api_header} type is not dict")

        # 调用内部函数，将LLM Model请求参数组合为大模型可以识别的参数
        params_dict = self.convert_file_gen_request(params_dict)

        for i, param_spec in enumerate(params_dict['inputs']):
            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_spec.setdefault('extra', {})
            # 防御性措施
            param_spec['extra'].setdefault('location', 'query')
            if param_spec['extra']['location'] not in {'query', 'body'}:
                raise Exception("input param: {param_spec} extra-location not found('query' or 'body')")

        for i, param_spec in enumerate(params_dict['outputs']):

            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict = ASDiGraph.generate_node_param_spec(param_spec)
            self.output_params_spec |= param_one_dict

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    def __call__(self, *args, **kwargs):
        # 判断当前节点的运行状态
        self.running_status, is_running = self.dag_obj.confirm_current_node_running_status(self.node_id, kwargs)
        if not is_running:
            self.clear()
            return {}

        try:
            self.run(*args, **kwargs)
            self.running_status = WorkflowNodeStatus.SUCCESS
            return self.output_params
        except Exception as err:
            exhausted_err = traceback.format_exc()
            self.log_error(f'{exhausted_err}')
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = f'{repr(err)}, trace: {exhausted_err}'
            return {}
        finally:
            self.clear()

    def run(self, *args, **kwargs):
        self.dag_obj.init_params_pool_with_running_node(self.node_id)

        # 1. 建立参数映射
        params_dict = self.opt_kwargs
        for i, param_spec in enumerate(params_dict['inputs']):
            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict_for_query, param_one_dict_for_body \
                = self.dag_obj.generate_node_param_real_for_api_input(param_spec)

            if not isinstance(param_one_dict_for_query, dict):
                raise Exception("input param: {param_one_dict_for_query} type not dict")
            if not isinstance(param_one_dict_for_body, dict):
                raise Exception("input param: {param_one_dict_for_body} type not dict")

            self.input_params |= param_one_dict_for_query
            self.input_params |= param_one_dict_for_body
            self.input_params_for_query |= param_one_dict_for_query
            self.input_params_for_body |= param_one_dict_for_body

        # 节点参数校验
        to_format = self.input_params_for_body.get('to_format', None)
        if to_format not in ["txt", "pdf", "docx"]:
            raise Exception("文件格式，应该为 txt、pdf 或 docx")
        formatted_markdown = self.input_params_for_body.get('formatted_markdown', None)
        if not formatted_markdown:
            raise Exception("文本内容为空")
        if not isinstance(formatted_markdown, str):
            try:
                formatted_markdown = str(formatted_markdown)
                self.input_params_for_body['formatted_markdown'] = formatted_markdown  # 更新字典中的值
            except Exception as e:
                raise Exception(f"无法将 formatted_markdown 转换为字符串: {str(e)}")
        title = self.input_params_for_body.get('title', None)
        if not title:
            raise Exception("文件名称为空")
        if not isinstance(title, str):
            try:
                title = str(title)
                self.input_params_for_body['title'] = title  # 更新字典中的值
            except Exception as e:
                raise Exception(f"无法将 title 转换为字符串: {str(e)}")

        if 'headers' not in params_dict['settings']:
            raise Exception("headers key not found in settings")

        # 2. 使用 api_request 函数进行 API 请求设置
        response = api_request(url=self.api_url, method="POST", headers=self.api_header,
                               params=self.input_params_for_query, data=self.input_params_for_body)
        if response.status == service_status.ServiceExecStatus.ERROR:
            self.log_info(f"workflow: {self.workflow_id}, 文件上传失败:{str(response.content)}")
            raise Exception(f"文件上传失败: {str(response.content)}")

        # 3. 拆包解析
        self.output_params = self.convert_file_gen_response(response.content)
        # 检查返回值是否对应
        for k in self.output_params_spec:
            if k not in self.output_params:
                self.log_error(f"file parse response: {self.output_params}")
                raise Exception(f"user defined output parameter {k} not found")

        self.dag_obj.update_params_pool_with_running_node(self.node_id, self.output_params)
        self.log_info(
            f"workflow: {self.workflow_id}, dag: {self.dag_id}, {self.var_name}, run success, "
            f"input params: {self.input_params}, file_url: "
            f"{self.output_params.get('file_url')}")
        return self.output_params

    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None

    @staticmethod
    def convert_file_gen_request(origin_params_dict: dict) -> dict:

        # 转换前端输入Key值为后端可识别Key值
        if 'inputs' in origin_params_dict:
            for item in origin_params_dict['inputs']:
                if item['name'] == 'format':
                    item['name'] = 'to_format'
                elif item['name'] == 'text':
                    item['name'] = 'formatted_markdown'
        else:
            raise KeyError("Missing 'inputs' in origin_params_dict")

        return origin_params_dict

    @staticmethod
    def convert_file_gen_response(origin_params_dict: dict) -> dict:
        content = origin_params_dict['download_link']
        updated_params_dict = {'file_url': content}
        return updated_params_dict

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")


# 新增文件生成节点
class FileParseNode(WorkflowNode):
    """
    FileParse Node.
    """

    node_type = WorkflowNodeType.FileParse

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
            dag_obj: Optional[ASDiGraph] = None,
    ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        self.input_params = {}
        self.output_params = {}
        self.output_params_spec = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ''
        self.dag_obj = dag_obj
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid
            self.workflow_id = self.dag_obj.workflow_id

        # GUI请求参数初始化
        self.api_url = FILE_PARSE_URL
        self.api_header = {}
        self.input_params_for_query = {}
        self.input_params_for_body = {}

    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs

        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")

        if 'headers' not in params_dict['settings']:
            raise Exception("headers key not found in settings")

        self.api_header = params_dict['settings']['headers']

        if not isinstance(self.api_header, dict):
            raise Exception(f"header:{self.api_header} type is not dict")

        # 调用内部函数，将LLM Model请求参数组合为大模型可以识别的参数
        params_dict = self.convert_file_parse_request(params_dict)

        for i, param_spec in enumerate(params_dict['inputs']):
            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_spec.setdefault('extra', {})
            # 防御性措施
            param_spec['extra'].setdefault('location', 'query')
            if param_spec['extra']['location'] not in {'query', 'body'}:
                raise Exception("input param: {param_spec} extra-location not found('query' or 'body')")

        for i, param_spec in enumerate(params_dict['outputs']):

            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict = ASDiGraph.generate_node_param_spec(param_spec)
            self.output_params_spec |= param_one_dict

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    def __call__(self, *args, **kwargs):
        # 判断当前节点的运行状态
        self.running_status, is_running = self.dag_obj.confirm_current_node_running_status(self.node_id, kwargs)
        if not is_running:
            self.clear()
            return {}

        try:
            self.run(*args, **kwargs)
            self.running_status = WorkflowNodeStatus.SUCCESS
            return self.output_params
        except Exception as err:
            exhausted_err = traceback.format_exc()
            self.log_error(f'{exhausted_err}')
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = f'{repr(err)}, trace: {exhausted_err}'
            return {}
        finally:
            self.clear()

    def run(self, *args, **kwargs):
        self.dag_obj.init_params_pool_with_running_node(self.node_id)

        # 1. 建立参数映射
        params_dict = self.opt_kwargs
        for i, param_spec in enumerate(params_dict['inputs']):
            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict_for_query, param_one_dict_for_body \
                = self.dag_obj.generate_node_param_real_for_api_input(param_spec)

            if not isinstance(param_one_dict_for_query, dict):
                raise Exception("input param: {param_one_dict_for_query} type not dict")
            if not isinstance(param_one_dict_for_body, dict):
                raise Exception("input param: {param_one_dict_for_body} type not dict")

            self.input_params |= param_one_dict_for_query
            self.input_params |= param_one_dict_for_body
            self.input_params_for_query |= param_one_dict_for_query
            self.input_params_for_body |= param_one_dict_for_body

        # 节点参数校验
        url = self.input_params_for_body.get('upload_file_url', None)
        if not url:
            raise Exception("文件链接为空")

        self.input_params_for_body["upload_file_url"] = [url]

        if 'headers' not in params_dict['settings']:
            raise Exception("headers key not found in settings")

        # 2. 使用 api_request 函数进行 API 请求设置
        response = api_request(url=self.api_url, method="POST", headers=self.api_header,
                               params=self.input_params_for_query, json=self.input_params_for_body)
        if response.status == service_status.ServiceExecStatus.ERROR:
            self.log_error(f"workflow: {self.workflow_id}, 文件解析服务错误: {str(response.content)}")
            raise Exception(f"文件解析服务错误: {str(response.content)}")
        # if response.content['code'] >= 400:
        #     self.log_error(f"workflow: {self.workflow_id}, 文件解析服务错误: {str(response.content)}")
        #     raise Exception(f"文件解析服务错误: {str(response.content)}")
        # 3. 拆包解析
        self.log_info(f"workflow: {self.workflow_id}, 文件解析输出为:{str(response.content)}")
        self.output_params = self.convert_file_parse_response(response.content)

        # 检查返回值是否对应
        for k in self.output_params_spec:
            if k not in self.output_params:
                self.log_error(f"file parse response: {self.output_params}")
                raise Exception(f"user defined output parameter {k} not found")

        self.dag_obj.update_params_pool_with_running_node(self.node_id, self.output_params)
        self.log_info(
            f"workflow: {self.workflow_id}, dag: {self.dag_id}, {self.var_name}, run success, input params: {self.input_params}, text: "
            f"{self.output_params.get('text')}")
        return self.output_params

    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None

    @staticmethod
    def convert_file_parse_request(origin_params_dict: dict) -> dict:

        # 转换前端输入Key值为后端可识别Key值
        if 'inputs' in origin_params_dict:
            for item in origin_params_dict['inputs']:
                if item['name'] == 'file_url':
                    item['name'] = 'upload_file_url'
        else:
            raise KeyError("Missing 'inputs' in origin_params_dict")

        return origin_params_dict

    @staticmethod
    def convert_file_parse_response(origin_params_dict: dict) -> dict:
        content = json.dumps(origin_params_dict, ensure_ascii=False)
        updated_params_dict = {'text': content}
        return updated_params_dict

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")


class MCPClientNode(WorkflowNode):
    """MCP Client节点实现SSE工具调用"""
    node_type = WorkflowNodeType.MCPClient

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
            dag_obj: Optional[ASDiGraph] = None,
    ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        self.input_params = {}
        self.output_params = {}
        self.output_params_spec = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ''
        if dag_obj is None:
            self.dag_obj = None
        else:
            self.dag_obj = weakref.proxy(dag_obj)
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid

        # MCP 配置信息
        self.mcp_server_url = ""  # 存储解析后的服务器配置
        self.selected_tool = ""  # 用户选择的工具

    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs

        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")

        # 解析MCP服务器配置
        if 'mcp_server_url' not in params_dict['settings']:
            raise Exception("mcp_servers not found in settings")
        if 'mcp_tool_name' not in params_dict['settings']:
            raise Exception("selected tool name not found in settings")

        self.mcp_server_url = params_dict['settings']['mcp_server_url']
        self.selected_tool = params_dict['settings']['mcp_tool_name']

        for i, param_spec in enumerate(params_dict['inputs']):
            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_spec.setdefault('extra', {})
            # 防御性措施
            param_spec['extra'].setdefault('location', 'body')
            if param_spec['extra']['location'] not in {'query', 'body'}:
                raise Exception("input param: {param_spec} extra-location not found('query' or 'body')")

        for i, param_spec in enumerate(params_dict['outputs']):
            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict = ASDiGraph.generate_node_param_spec(param_spec)
            self.output_params_spec |= param_one_dict

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    def __call__(self, *args, **kwargs):
        # 判断当前节点的运行状态
        self.running_status, is_running = self.dag_obj.confirm_current_node_running_status(self.node_id, kwargs)
        if not is_running:
            self.clear()
            return {}
        try:
            self.run(*args, **kwargs)
            self.running_status = WorkflowNodeStatus.SUCCESS
            return self.output_params
        except Exception as err:
            self.log_error({traceback.format_exc()})
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = f'{repr(err)}'
            return {}
        finally:
            self.clear()

    def run(self, *args, **kwargs):
        self.dag_obj.init_params_pool_with_running_node(self.node_id)

        # 1.建立参数映射，把param_spec转换为，{'key0':'value','key1':'value'}的格式
        params_dict = self.opt_kwargs
        for i, param_spec in enumerate(params_dict['inputs']):
            if param_spec.get('name', '') == '':
                continue

            param_one_dict = self.dag_obj.generate_node_param_real_for_mcp_input(param_spec)

            if not isinstance(param_one_dict, dict):
                raise Exception("input param:{param_one_dict} type is not dict")

            self.input_params |= param_one_dict

        # 3. 拆包解析
        # 运行异步逻辑
        self.output_params = asyncio.run(self._execute_tool_async())
        # 检查返回值是否对应(mcp非标准返回时触发)isError和content
        for k in self.output_params_spec:
            if k not in self.output_params:
                self.log_error(f"mcp tool response: {self.output_params}")
                raise Exception(f"user defined output parameter {k} not found in mcp response")
        self.dag_obj.update_params_pool_with_running_node(self.node_id, self.output_params)
        self.log_info(
            f"{self.var_name}, run success, input params: {self.input_params}, output params: {self.output_params}")
        return self.output_params

    async def _execute_tool_async(self) -> dict:
        """异步执行工具的核心逻辑"""
        try:
            mcp_client = MCPClient()
            await mcp_client.connect_server(self.mcp_server_url)
            result = await mcp_client.execute_tool(
                tool_name=self.selected_tool,
                arguments=self.input_params
            )

        except Exception as err:
            self.log_error(f"Failed to execute tool: {str(err)}")
            raise RuntimeError(f"Tool execution failed: {err}")

        finally:
            # 尝试断开连接并清理资源
            await mcp_client.disconnect()

        if result.get('isError'):
            error_msg = result.get('message', 'Unknown tool error')
            self.log_error(f"Failed to execute tool or MCP tool error: {error_msg}")

        return result

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None


# 意图识别节点
class IntentionNode(WorkflowNode):
    """
    Intention Node.
    """
    node_type = WorkflowNodeType.Intention

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
            dag_obj: Optional[ASDiGraph] = None,
    ) -> None:
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        # opt_kwargs 包含用户定义的节点输入变量
        self.input_params_for_body = {}
        self.input_params = {}
        self.output_params = {}
        self.output_params_spec = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ""
        self.dag_obj = dag_obj
        self.dag_id = ""
        if self.dag_obj:
            self.dag_id = self.dag_obj.uuid
            self.workflow_id = self.dag_obj.workflow_id
        # 大模型信息
        self.model = ""
        self.model_name = ""
        self.api_header = {}
        self.api_url = LLM_URL

    def __call__(self, *args, **kwargs):
        # 判断当前节点的运行状态
        self.running_status, is_running = self.dag_obj.confirm_current_node_running_status(self.node_id, kwargs)
        if not is_running:
            self.clear()
            return {}

        try:
            self.run(*args, **kwargs)
            self.running_status = WorkflowNodeStatus.SUCCESS
            return self.output_params
        except Exception as err:
            self.log_info(f'{traceback.format_exc()}')
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = f'{repr(err)}'
            return {}
        finally:
            self.clear()

    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs

        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")

        for i, param_spec in enumerate(params_dict['outputs']):
            param_one_dict = ASDiGraph.generate_node_param_spec(param_spec)
            self.output_params_spec |= param_one_dict

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    def run(self, *args, **kwargs):
        self.dag_obj.init_params_pool_with_running_node(self.node_id)

        # 1. 建立输入参数映射
        params_dict = self.opt_kwargs

        self.verify_settings_info(params_dict['settings'])

        for i, param_spec in enumerate(params_dict['inputs']):
            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict = self.dag_obj.generate_node_param_real(param_spec)
            self.input_params |= param_one_dict

        # 2. 获取意图识别参数与意图模式(极速模式，精确模式)
        model_settings = params_dict['settings'].get("model", {})
        intentions = params_dict['settings'].get("intentions", [])
        lite_mode = params_dict['settings'].get("lite_mode", True)

        self.model = model_settings["model_name"]
        model_info_url = f"http://bff-service:6668/callback/v1/model/{self.model}"
        loghandler.logger.info(f"API MODEL Request - URL: {model_info_url}")
        response = api_request(url=model_info_url, method='GET')
        loghandler.logger.info(f"API MODEL Response: {response}")
        if response.status == service_status.ServiceExecStatus.ERROR:
            lid = self.get_log_id(response)
            self.log_error(f"api response: {str(response.content)}, log_id: {lid}")
            raise Exception(f"{self.model} get model info  error")
        model_response = self.convert_model_response(response.content)
        self.model_name = model_response['model']
        loghandler.logger.info(f"API MODEL Response: {model_response}")


        # 3. 不同模式意图处理
        self.understand_intention_mode(self.input_params, model_settings, intentions, lite_mode)

        return self.output_params

    def understand_intention_mode(self, input_params: dict, model_settings: dict, intentions: list,
                                  lite_mode: bool) -> dict:

        # 意图请求获取
        query = input_params.get("query", "")

        # prompt提示词拼接
        self.generate_intention_prompt(query, intentions, model_settings)
        messages = self.generate_intention_prompt(query, intentions, model_settings)

        # 新增需要的键值对
        self.input_params_for_body = {
            'model': self.model_name,
            'temperature': model_settings['temperature'],
            'top_p': model_settings['top_p'],
            'messages': messages
        }

        # 获取LLM的静态Token
        sys_params = self.dag_obj.generate_setting_param_real()

        # static_token = ASDiGraph.get_static_auth_token(sys_params)

        # if static_token:
        #     self.api_header["Authorization"] = f"Bearer {static_token}"
        #     self.api_header["Content-Type"] = "application/json"
        # else:
        #     self.log_error(f"Token失效或未提供Token")
        #     raise Exception(f"Token失效或未提供Token, 检查参数是否正确")

        # 2. 使用 api_request 函数进行 API 请求设置
        self.api_header["Content-Type"] = "application/json"
        self.api_url = f"http://bff-service:6668/callback/v1/model/{self.model}/chat/completions"

        response = api_request_for_big_model(url=self.api_url, method='POST', headers=self.api_header,
                                             data=self.input_params_for_body)

        if response.status == service_status.ServiceExecStatus.ERROR:
            self.log_error(f"意图识别回答错误: {str(response.content)}")
            raise Exception(f"{model_settings['model_name']} is not available")

        try:
            content = response.content['choices'][0]['message']['content']
            if not content:  # 检查 content 是否为空字符串/None
                error_msg = "大模型返回内容为空，请检查参数配置"
                self.log_error(error_msg)
                raise Exception(error_msg)
        except (KeyError, IndexError, TypeError) as e:
            error_msg = f"大模型响应异常，请检查参数配置，错误详情: {str(e)}"
            self.log_error(error_msg)
            raise Exception(error_msg)

        # 3. 拆包解析, 输出意图识别结果
        if lite_mode:
            self.output_params = self.get_intention_result_lite_mode(response.content, intentions)
        else:
            self.output_params = self.get_intention_result_precision_mode(response.content, intentions)
        self.log_info(f"意图识别出参: {self.output_params}")
        # 检查返回值是否对应
        for k in self.output_params_spec:
            if k not in self.output_params:
                self.log_error(f"意图识别变量缺失: {self.output_params}")
                raise Exception(f"意图识别变量缺失: {k}")

        self.dag_obj.update_params_pool_with_running_node(self.node_id, self.output_params)
        self.log_info(
            f"{self.var_name}, run success, "
            f"input params: {self.input_params_for_body}, "
            f"output intention result: {self.output_params}")
        return self.output_params

    def generate_intention_prompt(self, query: str, intentions: list, model_settings: dict):
        # 用户自定义prompt
        user_defined_prompt = model_settings["system"]
        # 拼接输入大模型的prompt
        try:
            if not isinstance(query, str):
                raise ValueError("query must be a string")
            if not isinstance(intentions, list):
                raise ValueError("intentions must be a list")

            # 拼接意图描述
            intention_descriptions = []
            for intent in intentions:
                if not isinstance(intent, dict):
                    continue

                name = intent.get('name', '')
                desc = intent.get('desc', '')
                sentences = intent.get('sentences', [])

                # 构建每个意图的完整描述（包含例句）
                intention_entry = [
                    f"- {name}: {desc}",
                    "  意图例句:"
                ]

                # 添加例句（最多显示10个，避免prompt过长）
                for i, sentence in enumerate(sentences[:10]):
                    intention_entry.append(f"    {i + 1}. {sentence}")

                intention_descriptions.append("\n".join(intention_entry))
                intention_list = '\n'.join(intention_descriptions)  # 提前处理换行
            full_prompt = f"""
你是一个专业的意图识别助手，需要准确分析用户输入并匹配到最合适的意图类别。请严格遵循以下优先级规则:

1. 首先检查用户输入是否为空:
- 如果query为空值，直接返回"其他意图"

2. 检查用户自定义意图描述:{user_defined_prompt}
- 如果用户输入符合自定义描述中的任何规则，必须优先使用该自定义意图,不要自行扩展类别
- 仅简要说明匹配到自定义意图的原因，不要提及规则或优先级
- 匹配则输出：意图Y [匹配特征] 
- 示例：意图5 物流查询

3. 只有当用户输入不符合任何自定义规则时，才参考以下可用意图列表:
- 准确分析用户输入并分析意图例句或描述是否匹配
- 仅简要说明匹配到自定义意图的原因，不要提及规则或优先级
- 匹配则输出：意图Y [匹配特征]
- 示例：意图5 物流查询
- 仅返回匹配结果, 匹配则输出：意图Y [匹配特征], 不要提及规则或优先级
- 匹配示例：意图5 物流查询

3. 只有当用户输入不符合任何自定义规则时，才参考以下可用意图列表:
- 准确分析用户输入并分析意图例句或描述是否匹配
- 仅返回匹配结果, 匹配则输出：意图Y [匹配特征], 不要提及规则或优先级
- 匹配示例：意图5 物流查询

4. 仅当用户意图既不匹配自定义规则也不在预定义列表中时，仅返回'其他意图'，匹配示例：其他意图

5. 匹配原则:
- 绝对优先使用用户自定义规则
- 避免主观猜测，优先匹配字面表达
- 每个判断必须提供明确证据

可用意图列表:
{intention_list}

**重要：仅输出匹配结果和简要理由，不要解释规则、优先级或匹配过程。**

"""

            user_prompt = f""" 用户输入为:{query} """

            self.log_info(f"意图识别prompt: {full_prompt}, 用户输入为: {user_prompt}")
            # 构造模型可识别的消息格式, 系统提示词
            messages = [{'role': 'system', 'content': full_prompt}, {'role': 'user', 'content': user_prompt}]

            return messages

        except KeyError as e:
            error_message = f"Missing key for formatting: {e}"
            raise ValueError(error_message)

    def get_intention_result_lite_mode(self, response: dict, intentions: list) -> dict:
        # 默认意图结果
        default_result = {
            "classification": "其他意图",
            "classificationID": -1
        }

        # 获取大模型返回内容
        content = response['choices'][0]['message']['content']
        self.log_info(f"意图识别大模型返回内容: {content}")

        # 预处理意图列表，过滤无效条目
        valid_intentions = [i for i in intentions if isinstance(i, dict)]

        # 查找匹配的意图
        matched_intention = None

        # 检查是否存在<think>标签
        think_end = content.find('</think>')
        if think_end != -1:
            # 只使用</think>之后的内容进行匹配
            match_content = content[think_end + len('</think>'):]
        else:
            # 使用全部内容进行匹配
            match_content = content

        for index, intention in enumerate(valid_intentions):
            self.log_info(f"index: {index} 意图内容: {intention}")

            # 检查意图名称或描述是否匹配（区分大小写）
            intention_name = intention.get('name')

            if intention_name and intention_name in match_content:
                matched_intention = intention

                classification_id = -1 if intention_name == "其他意图" else index

                default_result = {
                    "classification": intention_name,
                    "classificationID": classification_id
                }
                break

        # 获取目标节点ID
        target_node_id = None
        if matched_intention:
            target_node_id = matched_intention.get('target_node_id')
        else:
            # 查找"其他意图"对应的target_node_id
            other_intention = next((i for i in valid_intentions if i.get('name') == '其他意图'), None)
            if other_intention:
                target_node_id = other_intention.get('target_node_id')

        # 更新条件池参数
        for intention in valid_intentions:
            current_target = intention.get('target_node_id')
            if current_target:
                self.dag_obj.update_conditions_pool_with_running_node(
                    self.node_id,
                    current_target,
                    current_target == target_node_id  # 匹配则为True，否则False
                )

        return default_result

    def get_intention_result_precision_mode(self, response: dict, intentions: list) -> dict:
        # 默认意图结果
        default_result = {
            "classification": "其他意图",
            "classificationID": -1,
            "thought": ""
        }

        # 获取大模型返回内容
        content = response['choices'][0]['message']['content']
        self.log_info(f"意图识别大模型返回内容: {content}")

        # 预处理意图列表，过滤无效条目
        valid_intentions = [i for i in intentions if isinstance(i, dict)]

        # 查找匹配的意图
        matched_intention = None

        # 检查是否存在<think>标签
        think_end = content.find('</think>')
        if think_end != -1:
            # 只使用</think>之后的内容进行匹配
            match_content = content[think_end + len('</think>'):]
        else:
            # 使用全部内容进行匹配
            match_content = content

        for index, intention in enumerate(valid_intentions):
            self.log_info(f"index: {index} 意图内容: {intention}")

            # 检查意图名称或描述是否匹配（区分大小写）
            intention_name = intention.get('name')

            if intention_name and intention_name in match_content:
                matched_intention = intention

                classification_id = -1 if intention_name == "其他意图" else index

                default_result = {
                    "classification": intention_name,
                    "classificationID": classification_id,
                    "thought": content  # 包含完整原始内容
                }
                break

        # 获取匹配意图的target_node_id
        target_node_id = None
        if matched_intention:
            target_node_id = matched_intention.get('target_node_id')
        else:
            # 查找"其他意图"对应的target_node_id
            other_intention = next((i for i in valid_intentions if i.get('name') == '其他意图'), None)
            if other_intention:
                target_node_id = other_intention.get('target_node_id')

        # 更新条件池参数
        for intention in valid_intentions:
            current_target = intention.get('target_node_id')
            if current_target:
                self.dag_obj.update_conditions_pool_with_running_node(
                    self.node_id,
                    current_target,
                    current_target == target_node_id  # 匹配则为True，否则False
                )

        return default_result

    @staticmethod
    def convert_model_response(model_result_dict: dict) -> dict:
        content = model_result_dict['data']['model']
        model_info = {'model': content}
        return model_info

    @staticmethod
    def verify_settings_info(settings: dict) -> None:
        """
        从settings中提取并验证model配置和intentions列表
        """
        if not isinstance(settings, dict):
            raise Exception("Input settings must be a dictionary")

        # 初始化结果结构
        model_settings = settings.get("model", {})
        intentions = settings.get("intentions", [])

        # 验证model配置
        if not isinstance(model_settings, dict):
            raise Exception("'model' field must be a dictionary")

        temperature = float(model_settings.get("temperature", 0.0))
        top_p = float(model_settings.get("top_p", 0.0))

        if not 0 <= temperature <= 1:
            raise Exception("温度参数错误，应该在[0,1]之间")
        if not 0 <= top_p <= 1:
            raise Exception("多样性参数错误，应该在[0,1]之间")

        # 用于检查名称重复的集合
        intention_names = set()

        # 验证intentions列表
        if not isinstance(intentions, list):
            raise Exception("'intentions' field must be a list")

        for intent in intentions:
            if not isinstance(intent, dict):
                raise Exception("'intention' field must be a dictionary")

            if not intent.get("name"):
                raise Exception("意图名称为空")

            # 检查名称是否重复
            intent_name = intent.get("name")
            if intent_name in intention_names:
                raise Exception(f"意图名称重复: '{intent_name}'")
            intention_names.add(intent_name)

            if intent.get("name") != "其他意图" and not intent.get("desc"):
                raise Exception("意图描述为空")

            if not intent.get("target_node_id"):
                raise Exception("存在未连接分支")

        return None

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, "
                               f"workflow: {self.workflow_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, "
                                f"workflow: {self.workflow_id}, {message}")

    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None


class AggregationNode(WorkflowNode):
    """
    RAG Node.
    """

    node_type = WorkflowNodeType.Aggregation

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
            dag_obj: Optional[ASDiGraph] = None,
    ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        self.input_params = {}
        self.output_params = {}
        self.output_params_spec = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ''
        if dag_obj is None:
            self.dag_obj = None
        else:
            self.dag_obj = weakref.proxy(dag_obj)
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid

        # GET or POST
        self.api_header = {}
        self.input_params_for_query = {}
        self.input_params_for_body = {}

    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs
        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")

        # 校验inputs中每个group的variables类型是否一致
        for group in params_dict['inputs']:
            if not isinstance(group, dict):
                raise Exception(f"input group:{group} is not a dict")

            if 'variables' not in group:
                raise Exception(f"variables key not found in group: {group}")

            variables = group['variables']
            if not isinstance(variables, list):
                raise Exception(f"variables:{variables} in group {group['name']} is not a list")

            if not variables:
                continue

            # 检查组变量的类型作为基准, 相同组内变量类型必须保持一致
            group_type = group['type']

            for var in variables:
                if var['type'] != group_type:
                    raise Exception(f"Variable type mismatch in group {group['name']}: "
                                    f"expected {group_type}, got {var['type']}")

        for i, param_spec in enumerate(params_dict['outputs']):
            param_one_dict = ASDiGraph.generate_node_param_spec(param_spec)
            self.output_params_spec |= param_one_dict

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }

    def __call__(self, *args, **kwargs):
        # 判断当前节点的运行状态
        self.running_status, is_running = self.dag_obj.confirm_current_node_running_status(self.node_id, kwargs)
        if not is_running:
            return {}

        try:
            self.run(*args, **kwargs)
            self.running_status = WorkflowNodeStatus.SUCCESS
            return self.output_params
        except Exception as err:
            self.log_error(f'{traceback.format_exc()}')
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = f'{repr(err)}'
            return {}
        finally:
            self.clear()

    def run(self, *args, **kwargs):
        self.dag_obj.init_params_pool_with_running_node(self.node_id)

        # 1. 建立参数映射
        params_dict = self.opt_kwargs

        for param_spec in params_dict['inputs']:
            param_one_dict = self.dag_obj.generate_node_param_real_for_aggregation_input(param_spec)

            # 2. 检查分组中是否有非空变量
            group_name = param_spec['name']
            self.input_params |= {group_name: param_one_dict}  # 合并到输入参数池
            first_non_empty_value = None

            for var_spec in param_spec['variables']:
                var_name = var_spec['name']
                current_value = param_one_dict.get(var_name)

                if not self.is_value_empty(current_value):  # 找到第一个非空值
                    first_non_empty_value = current_value
                    break

            # 3. 更新输出参数
            self.output_params[group_name] = first_non_empty_value  # 可能为None

            # 全部为空值时打印日志
            if first_non_empty_value is None:
                self.log_info(f"Group {group_name} all values are empty")

        # 检查返回值是否对应
        for k in self.output_params_spec:
            if k not in self.output_params:
                self.log_error(f"api response: {self.output_params}")
                raise Exception(f"user defined output parameter {k} not found in aggregation node")

        self.dag_obj.update_params_pool_with_running_node(self.node_id, self.output_params)
        self.log_info(
            f"{self.var_name}, run success, input params: {self.input_params}, output params: {self.output_params}")
        return self.output_params

    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None

    @staticmethod
    def is_value_empty(value):
        """判断值是否为空（支持所有类型）"""
        if value is None:
            return True
        if isinstance(value, (str, bytes)) and not value:
            return True
        if isinstance(value, (list, tuple, set)) and not value:
            return True
        if isinstance(value, dict) and not value:
            return True
        return False

    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")


class TemplateTransformNode(WorkflowNode):
    """
    Template Transform Node.
    """

    node_type = WorkflowNodeType.TemplateTransform

    def __init__(
            self,
            node_id: str,
            opt_kwargs: dict,
            source_kwargs: dict,
            dep_opts: list,
            dag_obj: Optional[ASDiGraph] = None,
    ) -> None:
        # 注意，在__init__方法中，不要调用self.log_info()，因为成员变量还未初始化，调用时有可能会有异常
        super().__init__(node_id, opt_kwargs, source_kwargs, dep_opts)
        # opt_kwargs 包含用户定义的节点输入变量
        self.input_params = {}
        self.output_params = {}
        self.output_params_spec = {}
        # init --> running -> success/failed:xxx
        self.running_status = WorkflowNodeStatus.INIT
        self.running_message = ''
        if dag_obj is None:
            self.dag_obj = None
        else:
            self.dag_obj = weakref.proxy(dag_obj)
        self.dag_id = ""
        if self.dag_obj is not None:
            self.dag_id = self.dag_obj.uuid
        
        self.jinja2_code = ""
    
    def compile(self) -> dict:
        # 检查参数格式是否正确
        self.log_info(f"{self.var_name}, compile param: {self.opt_kwargs}")
        params_dict = self.opt_kwargs

        # 检查参数格式是否正确
        if 'inputs' not in params_dict:
            raise Exception("inputs key not found")
        if 'outputs' not in params_dict:
            raise Exception("outputs key not found")
        if 'settings' not in params_dict:
            raise Exception("settings key not found")

        if not isinstance(params_dict['inputs'], list):
            raise Exception(f"inputs:{params_dict['inputs']} type is not list")
        if not isinstance(params_dict['outputs'], list):
            raise Exception(f"outputs:{params_dict['outputs']} type is not list")
        if not isinstance(params_dict['settings'], dict):
            raise Exception(f"settings:{params_dict['settings']} type is not dict")
        
        base64_jinja2_code = params_dict['settings'].get('code', None)
        if not base64_jinja2_code:
            raise Exception("jinja2 code none")

        isBase64 = False
        try:
            base64.b64encode(base64.b64decode(base64_jinja2_code)) == base64_jinja2_code
            isBase64 = True
        except Exception:
            isBase64 = False
            raise Exception("jinja2 code str not base64")
        
        self.jinja2_code = base64.b64decode(base64_jinja2_code).decode('utf-8')
        if self.jinja2_code == "":
            raise Exception("jinja2 code empty")
        self.log_info(f"{self.var_name}, jinja2 code: {self.jinja2_code}")

        return {
            "imports": "",
            "inits": "",
            "execs": "",
        }
    
    def __call__(self, *args, **kwargs):
        # 判断当前节点的运行状态
        self.running_status, is_running = self.dag_obj.confirm_current_node_running_status(self.node_id, kwargs)
        if not is_running:
            self.clear()
            return {}
        try:
            self.run(*args, **kwargs)
            self.running_status = WorkflowNodeStatus.SUCCESS
            return self.output_params
        except Exception as err:
            exhausted_err = traceback.format_exc()
            self.log_error(f'{exhausted_err}')
            self.running_status = WorkflowNodeStatus.FAILED
            self.running_message = f'{repr(err)}, trace: {exhausted_err}'
            return {}
        finally:
            self.clear()
    
    def run(self, *args, **kwargs):
        self.dag_obj.init_params_pool_with_running_node(self.node_id)

        # 1. 建立参数映射
        params_dict = self.opt_kwargs
        for i, param_spec in enumerate(params_dict['inputs']):
            # param_spec 举例
            # {
            #     "name": "apiKeywords",
            #     "type": "string",
            #     "desc": "",
            #     "required": false,
            #     "value": {
            #         "type": "ref",
            #         "content": {
            #             "ref_node_id": "4daf0d1a33af497e9819fe515133eb5f",
            #             "ref_var_name": "keywords"
            #         }
            #     },
            #     "object_schema": null,
            #     "list_schema": null,
            # }

            # 防御性措施
            if param_spec.get('name', '') == '':
                continue

            param_one_dict = self.dag_obj.generate_node_param_real(param_spec)
            self.input_params |= param_one_dict
        self.log_info(f"{self.var_name}, input params: {self.input_params}")
        
        try:
            result = jinja2.Template(self.jinja2_code).render(self.input_params)
            self.output_params = {'output': result}
        except Exception as e:
            raise Exception(f"无法将渲染Jinja2模板: {str(e)}")

        self.dag_obj.update_params_pool_with_running_node(self.node_id, self.output_params)
        self.log_info(f"{self.var_name}, run success, input params: {self.input_params}, output params: {self.output_params}")
        return self.output_params
    
    def clear(self):
        """
        避免循环引用风险.
        """
        self.dag_obj = None
        self.dep_opts = None
        self.dep_vars = None
    
    def log_info(self, message: str):
        """
        记录日志
        :param message: 日志消息
        """
        loghandler.logger.info(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")

    def log_error(self, message: str):
        """
        记录错误日志
        :param message: 错误消息
        """
        loghandler.logger.error(f"{self.node_type}, node: {self.node_id}, dag: {self.dag_id}, {message}")


NODE_NAME_MAPPING = {
    # 自定义节点
    "StartNode": StartNode,
    "EndNode": EndNode,
    "PythonNode": PythonServiceUserTypingNode,
    "ApiNode": ApiNode,
    "LLMNode": LLMNode,
    "SwitchNode": SwitchNode,
    "RAGNode": RAGNode,
    "GUIAgentNode": GUIAgentNode,
    "LLMStreamingNode": LLMStreamingNode,
    "EndStreamingNode": EndStreamingNode,
    "FileGenerateNode": FileGenerateNode,
    "FileParseNode": FileParseNode,
    "MCPClientNode": MCPClientNode,
    "IntentionNode": IntentionNode,
    "AggregationNode": AggregationNode,
    "TemplateTransformNode": TemplateTransformNode
}


def get_all_agents(
        node: WorkflowNode,
        seen_agents: Optional[set] = None,
        return_var: bool = False,
) -> List:
    """
    Retrieve all unique agent objects from a pipeline.

    Recursively traverses the pipeline to collect all distinct agent-based
    participants. Prevents duplication by tracking already seen agents.

    Args:
        node (WorkflowNode): The WorkflowNode from which to extract agents.
        seen_agents (set, optional): A set of agents that have already been
            seen to avoid duplication. Defaults to None.

    Returns:
        list: A list of unique agent objects found in the pipeline.
    """
    if seen_agents is None:
        seen_agents = set()

    all_agents = []

    for participant in node.pipeline.participants:
        if participant.node_type == WorkflowNodeType.AGENT:
            if participant not in seen_agents:
                if return_var:
                    all_agents.append(participant.var_name)
                else:
                    all_agents.append(participant.pipeline)
                seen_agents.add(participant.pipeline)
        elif participant.node_type == WorkflowNodeType.PIPELINE:
            nested_agents = get_all_agents(
                participant,
                seen_agents,
                return_var=return_var,
            )
            all_agents.extend(nested_agents)
        else:
            raise TypeError(type(participant))

    return all_agents
