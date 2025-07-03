import time
import uuid
import json
import re

from pypinyin import lazy_pinyin
from loguru import logger

# 导入redis_queue，优先使用绝对导入，失败时使用相对导入
try:
    from config import redis_queue
except ImportError:
    from .config import redis_queue


class WorkflowStatus:  # type: ignore[name-defined]
    WORKFLOW_PUBLISHED = "published",  # 已发布状态
    WORKFLOW_DRAFT = "draft"  # 未发布状态


class WorkflowType:  # type: ignore[name-defined]
    WORKFLOW_EXAMPLE = 1,  # 插件样例


class URLPath:
    GUI_PATH_PREFIX = "run_for_user_defined"  # GUI路径
    STREAM_PATH_PREFIX = "run_for_stream"  # 流式路径


def workflow_format_convert(origin_dict: dict) -> dict:
    converted_dict = {}
    nodes = origin_dict.get("nodes", [])
    edges = origin_dict.get("edges", [])

    for node in nodes:
        if len(node) == 0:
            raise Exception(f"异常: 前端透传了空节点，请确认")

        node_id = node["id"]
        node_data = {
            "data": {
                "args": node["data"]
            },
            "inputs": {
                "input_1": {
                    "connections": []
                }
            },
            "outputs": {
                "output_1": {
                    "connections": []
                }
            },
            "name": node["type"]
        }
        converted_dict.setdefault(node_id, node_data)

        for edge in edges:
            if edge["source_node_id"] == node_id:
                converted_dict[node_id]["outputs"]["output_1"]["connections"].append(
                    {'node': edge["target_node_id"], 'output': "input_1"}
                )
            elif edge["target_node_id"] == node_id:
                converted_dict[node_id]["inputs"]["input_1"]["connections"].append(
                    {'node': edge["source_node_id"], 'input': "output_1"}
                )

    return converted_dict


def node_format_convert(node_dict: dict) -> dict:
    converted_dict = {}
    node_id = node_dict["id"]
    node_data = {
        "data": {
            "args": node_dict["data"]
        },
        "inputs": {
            "input_1": {
                "connections": []
            }
        },
        "outputs": {
            "output_1": {
                "connections": []
            }
        },
        "name": node_dict["type"]
    }
    converted_dict.setdefault(node_id, node_data)
    return converted_dict


def get_workflow_running_result(nodes_result: list, execute_id: str, execute_status: str, execute_cost: str) -> dict:
    execute_result = {
        "execute_id": execute_id,
        "execute_status": execute_status,
        "execute_cost": execute_cost,
        "node_result": nodes_result
    }
    return execute_result


def standardize_single_node_format(data: dict) -> dict:
    for value in data.values():
        for field in ['inputs', 'outputs']:
            # 如果字段是一个字典，且'connections'键存在于字典中
            if 'input_1' in value[field]:
                # 将'connections'字典设置为[]
                value[field]['input_1']['connections'] = []
            elif 'output_1' in value[field]:
                value[field]['output_1']['connections'] = []
    return data


def plugin_desc_config_generator(data: dict) -> dict:
    dag_name = data['pluginName']
    # identifier = data['identifier']
    # if identifier == '':
    #     raise Exception("userID not found")
    org_id = data['org_id']
    if org_id == '':
        raise Exception("org_id not found")
    user_id = data['user_id']
    if user_id == '':
        raise Exception("user_id not found")
    service_url = data['serviceURL']

    dag_en_name = data['pluginENName']
    if dag_en_name == '':
        raise Exception("plugin english name not found")
    dag_desc = data['pluginDesc']
    dag_spec_dict = data['pluginSpec']
    if not isinstance(dag_spec_dict, dict) or "nodes" not in dag_spec_dict:
        raise ValueError("Invalid workflow schema format")

    openapi_schema_dict = {"openapi": "3.0.0", "info": {
        "title": f"{dag_en_name} API",
        "version": "1.0.0",
        "description": f"{dag_desc}"
    }, "servers": [
        {
            "url": service_url
        }
    ], "paths": {
        f"/run_for_bigmodel/{user_id}/{org_id}/{dag_en_name}": {
            "post": {
                "summary": f"{dag_name}",
                "operationId": f"action_{dag_en_name}",
                "description": f"{dag_desc}",
                "parameters": [
                    {
                        "in": "header",
                        "name": "content-type",
                        "schema": {
                            "type": "string",
                            "example": "application/json"
                        },
                        "required": True
                    }
                ],
                "responses": {
                    "200": {
                        "description": "成功获取查询结果",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": [],
                                    "properties": {}
                                }
                            }
                        }
                    },
                    "default": {
                        "description": "请求失败时的错误信息",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object"
                                }
                            }
                        }
                    }
                },
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": [],
                                "properties": {}
                            }
                        }
                    }
                }
            }
        }
    }}

    # 完善入参列表
    request_schema = \
        openapi_schema_dict["paths"][f"/run_for_bigmodel/{user_id}/{org_id}/{dag_en_name}"]["post"]["requestBody"][
            "content"]["application/json"]["schema"]

    update_openapi_schema_with_start_node_params(request_schema, dag_spec_dict)

    # 完善出参列表
    response_schema = \
        openapi_schema_dict["paths"][f"/run_for_bigmodel/{user_id}/{org_id}/{dag_en_name}"]["post"]["responses"][
            "200"]["content"]["application/json"]["schema"]

    update_openapi_schema_with_end_node_params(response_schema, dag_spec_dict)

    return openapi_schema_dict


def plugin_desc_config_generator_user_defined(data: dict) -> dict:
    dag_name = data['pluginName']
    service_url = data['serviceURL']
    # identifier = data['identifier']
    # if identifier == '':
    #     raise Exception("userID not found")
    org_id = data['org_id']
    if org_id == '':
        raise Exception("org_id not found")
    user_id = data['user_id']
    if user_id == '':
        raise Exception("user_id not found")
    service_url = service_url.replace("/plugin/api", "/openapi/user_defined_plugin/v1", 1)

    dag_en_name = data['pluginENName']
    if dag_en_name == '':
        raise Exception("plugin english name not found")
    dag_desc = data['pluginDesc']
    dag_spec_dict = data['pluginSpec']
    if not isinstance(dag_spec_dict, dict) or "nodes" not in dag_spec_dict:
        raise ValueError("Invalid workflow schema format")

    openapi_schema_dict = {"openapi": "3.0.0", "info": {
        "title": f"{dag_en_name} API",
        "version": "1.0.0",
        "description": f"{dag_desc}"
    }, "servers": [
        {
            "url": service_url
        }
    ], "paths": {
        f"/run_for_user_defined/{user_id}/{org_id}/{dag_en_name}": {
            "post": {
                "summary": f"{dag_name}",
                "operationId": f"action_{dag_en_name}",
                "description": f"{dag_desc}",
                "parameters": [
                    {
                        "in": "header",
                        "name": "Authorization",
                        "schema": {
                            "type": "string",
                            "example": "Bearer {maas平台上申请的token}"
                        },
                        "required": True,
                        "description": "Bearer Token 用于API鉴权，格式为 'Bearer {maas平台上申请的token}'"
                    }
                ],
                "security": [
                    {
                        "Authorization": []
                    }
                ],
                "responses": {
                    "200": {
                        "description": "成功获取查询结果",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": [],
                                    "properties": {}
                                }
                            }
                        }
                    },
                    "default": {
                        "description": "请求失败时的错误信息",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object"
                                }
                            }
                        }
                    }
                },
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": [],
                                "properties": {}
                            }
                        }
                    }
                }
            }
        }
    },
                           "components": {
                               "securitySchemes": {
                                   "Authorization": {
                                       "type": "http",
                                       "scheme": "bearer",
                                       "description": "使用 Bearer Token 进行鉴权。Token 需要在请求头中传递，格式为 `Authorization: Bearer <token>`。"
                                   }
                               }
                           }
                           }

    # 完善入参列表
    request_schema = \
        openapi_schema_dict["paths"][f"/run_for_user_defined/{user_id}/{org_id}/{dag_en_name}"]["post"]["requestBody"][
            "content"]["application/json"]["schema"]

    update_openapi_schema_with_start_node_params(request_schema, dag_spec_dict)

    # 完善出参列表
    response_schema = \
        openapi_schema_dict["paths"][f"/run_for_user_defined/{user_id}/{org_id}/{dag_en_name}"]["post"]["responses"][
            "200"]["content"]["application/json"]["schema"]

    update_openapi_schema_with_end_node_params(response_schema, dag_spec_dict)

    return openapi_schema_dict


# 生成流式输出的openAPI_schema
def plugin_desc_config_generator_for_stream(data: dict) -> dict:
    dag_name = data['pluginName']
    service_url = data['serviceURL']
    # identifier = data['identifier']
    # if identifier == '':
    #     raise Exception("userID not found")
    org_id = data['org_id']
    if org_id == '':
        raise Exception("org_id not found")
    user_id = data['user_id']
    if user_id == '':
        raise Exception("user_id not found")

    dag_en_name = data['pluginENName']
    if dag_en_name == '':
        raise Exception("plugin english name not found")
    dag_desc = data['pluginDesc']
    dag_spec_dict = data['pluginSpec']
    if not isinstance(dag_spec_dict, dict) or "nodes" not in dag_spec_dict:
        raise ValueError("Invalid workflow schema format")

    openapi_schema_dict = {
        "openapi": "3.0.0",
        "info": {
            "title": f"{dag_en_name} API",
            "version": "1.0.0",
            "description": f"{dag_desc}"
        },
        "servers": [
            {
                "url": service_url
            }
        ],
        "paths": {
            f"/run_for_stream/{user_id}/{org_id}/{dag_en_name}": {
                "post": {
                    "summary": f"{dag_name}",
                    "operationId": f"action_{dag_en_name}",
                    "description": f"{dag_desc}",
                    "parameters": [
                        {
                            "in": "header",
                            "name": "Authorization",
                            "schema": {
                                "type": "string",
                                "example": "Bearer {maas平台上申请的token}"
                            },
                            "required": True,
                            "description": "Bearer Token 用于API鉴权，格式为 'Bearer {maas平台上申请的token}'"
                        }
                    ],
                    "security": [
                        {
                            "Authorization": []
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "SSE流式响应",
                            "content": {
                                "text/event-stream": {
                                    "schema": {
                                        "type": "object",
                                        "required": [],
                                        "properties": {
                                            "id": {
                                                "type": "string",
                                                "description": "响应ID"
                                            },
                                            "object": {
                                                "type": "string",
                                                "description": "对象类型",
                                                "enum": ["chat.completion"]
                                            },
                                            "created": {
                                                "type": "integer",
                                                "description": "创建时间戳"
                                            },
                                            "model": {
                                                "type": "string",
                                                "description": "使用的模型名称"
                                            },
                                            "choices": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "index": {
                                                            "type": "integer",
                                                            "description": "选择项索引"
                                                        },
                                                        "delta": {
                                                            "type": "object",
                                                            "properties": {
                                                                "role": {
                                                                    "type": "string",
                                                                    "description": "角色",
                                                                    "enum": ["assistant"]
                                                                },
                                                                "content": {
                                                                    "type": "string",
                                                                    "description": "流式返回的内容片段"
                                                                }
                                                            }
                                                        },
                                                        "finish_reason": {
                                                            "type": "string",
                                                            "description": "结束原因"
                                                        }
                                                    }
                                                }
                                            },
                                            "usage": {
                                                "type": "object",
                                                "properties": {
                                                    "prompt_tokens": {
                                                        "type": "integer",
                                                        "description": "提示词消耗的token数量"
                                                    },
                                                    "completion_tokens": {
                                                        "type": "integer",
                                                        "description": "补全消耗的token数量"
                                                    },
                                                    "total_tokens": {
                                                        "type": "integer",
                                                        "description": "总token数量"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "default": {
                            "description": "请求失败时的错误信息",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object"
                                    }
                                }
                            }
                        }
                    },
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": [],
                                    "properties": {}
                                }
                            }
                        }
                    }
                }
            }
        },
        "components": {
            "securitySchemes": {
                "Authorization": {
                    "type": "http",
                    "scheme": "bearer",
                    "description": "使用Bearer Token进行鉴权。Token需要在请求头中传递，格式为`Authorization: Bearer {maas平台上申请的token}`。"
                }
            }
        }
    }

    # 完善入参列表
    request_schema = \
        openapi_schema_dict["paths"][f"/run_for_stream/{user_id}/{org_id}/{dag_en_name}"]["post"]["requestBody"][
            "content"]["application/json"]["schema"]

    update_openapi_schema_with_start_node_params(request_schema, dag_spec_dict)

    # 注意，流式节点，不需要完善出参列表，直接返回大模型的输出结果，所以这里注释掉
    response_schema = \
        openapi_schema_dict["paths"][f"/run_for_stream/{user_id}/{org_id}/{dag_en_name}"]["post"]["responses"][
            "200"]["content"]["text/event-stream"]["schema"]
    update_openapi_schema_with_end_node_params_for_stream(response_schema, dag_spec_dict)

    return openapi_schema_dict


def update_openapi_schema_with_start_node_params(request_schema, dag_spec_dict):
    """
    根据DAG规范中的StartNode节点，更新请求体的schema。

    :param request_schema: 请求体的schema字典，将被更新。
    :param dag_spec_dict: DAG规范的字典，包含节点信息。
    :return: 无返回值，直接修改request_schema。
    """
    # 检查request_schema和dag_spec_dict是否有效
    if not isinstance(request_schema, dict):
        raise ValueError("response_schema must be a dictionary")
    if not isinstance(dag_spec_dict, dict) or 'nodes' not in dag_spec_dict:
        raise ValueError("dag_spec_dict must be a dictionary with 'nodes' key")

    # 查找StartNode节点
    start_node_dict = {}
    for node in dag_spec_dict['nodes']:
        if "type" not in node:
            raise Exception("Invalid node schema")
        if node["type"] == "StartNode":
            start_node_dict = node
            break
    if len(start_node_dict) == 0:
        raise Exception("Start node not found")

    # 更新请求体的schema
    for param in start_node_dict['data']['outputs']:

        param_name = param['name']
        param_type = param['type']
        param_desc = param['desc']

        # 入参支持普通参数与Array类型入参
        if param_type == "array":
            request_schema['required'].append(param_name)
            request_schema['properties'][param_name] = {
                "type": "array",
                "description": param_desc,
                "items": _get_schema_from_param(param['list_schema'])
            }
        else:
            # 普通变量
            request_schema['required'].append(param_name)
            request_schema['properties'][param_name] = {
                "type": param_type,
                "description": param_desc
            }


def update_openapi_schema_with_end_node_params(response_schema, dag_spec_dict):
    """
    根据DAG规范中的EndNode节点，更新响应体的schema。

    :param response_schema: 响应体的schema字典，将被更新。
    :param dag_spec_dict: DAG规范的字典，包含节点信息。
    :return: 无返回值，直接修改response_schema。
    """
    # 检查response_schema和dag_spec_dict是否有效
    if not isinstance(response_schema, dict):
        raise ValueError("response_schema must be a dictionary")
    if not isinstance(dag_spec_dict, dict) or 'nodes' not in dag_spec_dict:
        raise ValueError("dag_spec_dict must be a dictionary with 'nodes' key")

    # 查找EndNode节点
    end_node_dict = {}
    for node in dag_spec_dict['nodes']:
        if "type" not in node:
            raise Exception("Invalid node schema")
        if node["type"] == "EndNode" or node["type"] == "EndStreamingNode":
            end_node_dict = node
            break
    if len(end_node_dict) == 0:
        raise Exception("End node not found")

    # 更新响应体的schema

    for param in end_node_dict['data']['inputs']:
        param_name = param['name']
        param_type = param['type']

        logger.info(f"param:{param}")
        logger.info(f"response_schema:{response_schema}")
        # 入参支持普通参数、Array类型入参和Object类型入参
        if param_type == "array":
            response_schema['required'].append(param_name)
            response_schema['properties'][param_name] = {
                "type": "array",
                "items": _get_schema_from_param(param['list_schema'])
            }
        elif param_type == "object":
            response_schema['required'].append(param_name)
            response_schema['properties'][param_name] = _get_schema_from_param(param['object_schema'])
        else:
            # 普通变量
            response_schema['required'].append(param_name)
            response_schema['properties'][param_name] = {
                "type": param_type
            }


def update_openapi_schema_with_end_node_params_for_stream(response_schema, dag_spec_dict):
    """
    根据DAG规范中的EndNode节点，更新响应体的SSE格式schema。

    :param response_schema: 响应体的schema字典，将被更新。
    :param dag_spec_dict: DAG规范的字典，包含节点信息。
    :return: 无返回值，直接修改response_schema。
    """
    # 检查response_schema和dag_spec_dict是否有效
    if not isinstance(response_schema, dict):
        raise ValueError("response_schema must be a dictionary")
    if not isinstance(dag_spec_dict, dict) or 'nodes' not in dag_spec_dict:
        raise ValueError("dag_spec_dict must be a dictionary with 'nodes' key")

    # 查找EndNode节点
    end_node_dict = {}
    for node in dag_spec_dict['nodes']:
        if "type" not in node:
            raise Exception("Invalid node schema")
        if node["type"] == "EndNode" or node["type"] == "EndStreamingNode":
            end_node_dict = node
            break
    if len(end_node_dict) == 0:
        raise Exception("End node not found")

    # 更新响应体的schema

    for param in end_node_dict['data']['inputs']:
        param_name = param['name']
        param_type = param['type']

        logger.info(f"param:{param}")
        logger.info(f"response_schema:{response_schema}")
        # 入参支持普通参数、Array类型入参和Object类型入参
        if param_type == "array":
            response_schema['required'].append(param_name)
        elif param_type == "object":
            response_schema['required'].append(param_name)
        else:
            # 普通变量
            response_schema['required'].append(param_name)

def _get_schema_from_param(param_schema):
    """
    递归获取参数对应的schema。

    :param param_schema: 参数的schema字典。
    :return: 参数对应的schema字典。
    """
    if not isinstance(param_schema, dict):
        return {"type": "string"}  # 默认返回字符串类型

    param_type = param_schema.get('type', 'string')

    if param_type == "array":
        return {
            "type": "array",
            "items": _get_schema_from_param(param_schema['list_schema'])
        }
    elif param_type == "object":
        properties = {}
        for field in param_schema['object_schema']:
            field_name = field['name']
            properties[field_name] = _get_schema_from_param(field)
        return {
            "type": "object",
            "properties": properties
        }
    else:
        return {
            "type": param_type
        }


def generate_workflow_schema_template(is_stream=False) -> str:
    end_node_type = 'EndNode'
    if is_stream:
        end_node_type = 'EndStreamingNode'

    start_node_id = "start_node"
    end_node_id = "end_node"
    workflow_schema = {
        "edges": [
        ],
        "nodes": [
            {
                "data": {
                    "inputs": [],
                    "outputs": [
                        {
                            "name": "",
                            "type": "string",
                            "desc": "",
                            "object_schema": "",
                            "list_schema": "",
                            "required": "false",
                            "value": {
                                "type": "generated",
                                "content": ""
                            }
                        }
                    ],
                    "settings": {
                        "staticAuthToken": ""
                    }
                },
                "id": start_node_id,
                "name": "开始",
                "type": "StartNode"
            },
            {
                "data": {
                    "inputs": [
                        {
                            "name": "",
                            "type": "string",
                            "desc": "",
                            "object_schema": "",
                            "list_schema": "",
                            "required": "false",
                            "value": {
                                "type": "ref",
                                "content": {
                                    "ref_node_id": "",
                                    "ref_var_name": ""
                                }
                            }
                        }
                    ],
                    "outputs": [],
                    "settings": {}
                },
                "id": end_node_id,
                "name": "结束",
                "type": end_node_type
            }
        ]
    }
    workflow_schema_json = json.dumps(workflow_schema)
    return workflow_schema_json


# 插件中文名称自动转换拼音
def chinese_to_pinyin(input_str: str) -> str:
    pinyin_list = lazy_pinyin(input_str)
    # 字符串中中文拼音首字母大写，其他情况保留原格式
    if input_str.isascii():
        capitalize_pinyin_list = [item.upper() if item.isalpha() else item for item in pinyin_list]
    else:
        capitalize_pinyin_list = [item.capitalize() if item.isalpha() else item for item in pinyin_list]
    pinyin_str = ''.join(capitalize_pinyin_list)
    return pinyin_str


# 插件复制后缀添加
def add_max_suffix(config_name: str, existing_config_copies: dict) -> int:
    existing_suffixes = []
    for config_copy in existing_config_copies:
        match = re.match(rf"{re.escape(config_name)}_(\d+)", config_copy.config_en_name)
        if match:
            existing_suffixes.append(int(match.group(1)))
    # 找出最大后缀
    name_suffix = max(existing_suffixes, default=0) + 1
    return name_suffix


def generate_stream(dag_id: str, queue_id: str):
    try:
        # SSE 响应通常是缓冲的，不会立即发送到客户端
        # time.sleep(0.5) 给了浏览器足够时间来初始化 EventSource 连接
        time.sleep(0.5)

        # # 发送初始连接事件，确保连接已建立
        # yield "event: connected\ndata: {\"status\":\"connected\"}\n\n"
        # # 添加一个空行作为刷新触发器
        # yield "\n"

        while True:
            # 阻塞式获取消息
            message = redis_queue.dequeue(queue_id, timeout=30)

            if not message:  # 空消息表示超时
                continue

            try:
                # 检查结束标记
                if message.strip() == 'data: [DONE]':
                    logger.info("进入结束流程")
                    # 然后发送包含dag_id的特殊事件
                    yield f"event: executeID\ndata: {dag_id}\n\n"
                    break

                # 正常数据消息
                logger.info(f"dag: {dag_id}, generate message: {message} (type: {type(message)})")
                # 确保消息格式正确
                if not message.startswith('data:'):
                    message = f"data: {message}"
                if not message.endswith('\n\n'):
                    message = f"{message}\n\n"
                yield message

            except Exception as e:
                logger.error(f"dag: {dag_id}, Error generate message: {str(e)}")
                yield f"{str(e)}\n\n"

    except Exception as e:
        logger.error(f"dag: {dag_id}, Stream error: {str(e)}")
        yield f"{str(e)}\n\n"
    finally:
        redis_queue.delete_queue(queue_id, dag_id=dag_id)  # 确保队列被清理

def extract_start_node(dag):
    """
    从 DAG 结构中提取起始节点（name="开始" 且 type="StartNode"）
    
    参数:
        dag (dict): 完整的 DAG JSON 结构
        
    返回:
        dict: 起始节点信息，若未找到返回 None
    """
    for node in dag.get("nodes", []):
        # 检查节点是否符合条件 [6,8](@ref)
        if node.get("name") == "开始" and node.get("type") == "StartNode":
            return node
    return None