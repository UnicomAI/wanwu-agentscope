# -*- coding: utf-8 -*-
"""The Web Server of the AgentScope Studio."""
import asyncio
import json

# memory leak check
import objgraph
from pympler import tracker, muppy, summary

tr = tracker.SummaryTracker()
import openai

openai.api_key = "xx"

import uuid
import base64
import sys

from datetime import datetime
from typing import Union, Optional
from sqlalchemy.exc import SQLAlchemyError
from flask import Response

import agentscope.aibigmodel_workflow.utils as utils
import agentscope.utils.jwt_auth as auth
import agentscope.aibigmodel_workflow.service as service
import agentscope.aibigmodel_workflow.database as database
from agentscope.web.workstation.workflow_utils import WorkflowNodeStatus
from agentscope.utils.jwt_auth import SIMPLE_CLOUD, PRIVATE_CLOUD
from agentscope.web.workstation.workflow_dag import build_dag_for_aibigmodel
from agentscope.service.web.mcp_client import MCPClient

from flask import request, jsonify, g

from agentscope.aibigmodel_workflow.loghandler import loghandler

from agentscope.aibigmodel_workflow.config import app, db, socketio, SERVER_PORT

from agentscope.aibigmodel_workflow.service import _ExecuteTable, _WorkflowTable, _PluginTable

# 定义不需要 JWT 验证的公开路由
PUBLIC_ENDPOINTS = [
    "/plugin/api/run_for_bigmodel/",
    "/workflow/list_internal"
    "/workflow/mem1"
]

sys.path.append("/opt/app/agentscope/src")

# before_request 钩子函数作为全局中间件
@app.before_request
def jwt_auth_middleware():
    for url in PUBLIC_ENDPOINTS:
        # 如果是公开路由，跳过 JWT 验证
        if url in request.path:
            return

    token = request.headers.get('Authorization')
    if not token:
        return jsonify({"code": 1002, "msg": "Token is missing!"})

    # 处理 Bearer token
    try:
        token = token.split(" ")[1]  # Bearer <token>
    except IndexError:
        return jsonify({"code": 1000, "msg": "Invalid token format!"})

    org_id = request.headers.get('x-org-id')
    claims, err = auth.parse_jwt_with_claims(token,org_id)
    if err:
        # 返回错误码和错误信息
        return jsonify({"code": err['code'], "msg": err['message']})

    # 存储 claims 信息，便于后续请求中使用其中的信息
    g.claims = claims


@app.route("/workflow/mem1", methods=["POST"])
def test():
    # 手动触发垃圾回收
    import gc
    gc.collect()

    # 全局类型数量
    print("\n1. Most common types:")
    objgraph.show_most_common_types(limit=10)

    # 增量变化
    print("\n1. Most diff types:")
    objgraph.show_growth(limit=10)

    # 追踪内存分配
    print("\n2. Memory total:")
    all_objects = muppy.get_objects()
    sum1 = summary.summarize(all_objects)
    summary.print_(sum1)

    print("\n2. Memory diff types detail:")
    tr.print_diff()

    return "success"


# 发布调试成功的workflow
@app.route("/plugin/api/publish", methods=["POST"])
def plugin_publish() -> Response:
    workflow_id = request.json.get("workflowID")
    plugin_field = request.json.get("pluginField")
    description = request.json.get("pluginQuestionExample")
    jwt_token = request.headers.get('Authorization')
    user_id = auth.get_user_id()
    org_id = auth.get_org_id()
    tenant_ids = auth.get_tenant_ids()
    cloud_type = auth.get_cloud_type()
    # 查询workflow_info表获取插件信息
    workflow_result = database.fetch_records_by_filters(_WorkflowTable,
                                                        id=workflow_id)

    if not workflow_result:
        return jsonify({"code": 7, "msg": "No workflow config data exists"})

    if workflow_result.execute_status != WorkflowNodeStatus.SUCCESS:
        return jsonify({"code": 7, "msg": "插件未调试成功，无法发布"})

    # 插件名称不允许重复
    if cloud_type == SIMPLE_CLOUD:
        plugin = database.fetch_records_by_filters(_PluginTable,
                                                   method='all',
                                                   user_id=user_id,
                                                   org_id=org_id,
                                                   plugin_en_name=workflow_result.config_en_name)
    elif cloud_type == PRIVATE_CLOUD:
        plugin = database.fetch_records_by_filters(_PluginTable,
                                                   method='all',
                                                   tenant_id__in=tenant_ids,
                                                   plugin_en_name=workflow_result.config_en_name)
    else:
        return jsonify({"code": 7, "msg": "不支持的云类型"})

    # 插件的英文名称唯一
    if len(plugin) > 0:
        return jsonify(
            {"code": 7, "msg": f"发现多个相同英文名称的插件，请确保唯一性: {workflow_result.config_en_name}"})

    result = service.plugin_publish(workflow_id,org_id, user_id, workflow_result, plugin_field, description, jwt_token,
                                    workflow_result.is_stream)

    return result

@app.route("/workflow/openapi_schema", methods=["GET"])
def plugin_openapi_schema() -> tuple[Response, int] | Response:
    workflow_id = request.args.get("workflowID")
    cloud_type = auth.get_cloud_type()

    if workflow_id == "":
        return jsonify({"code": 7, "msg": "workflow id not found"})

    if cloud_type == SIMPLE_CLOUD:
        user_id = auth.get_user_id()
        org_id = auth.get_org_id()
        if user_id == "":
            return jsonify({"code": 7, "msg": "user id not found"})
        if org_id == "":
            return jsonify({"code": 7, "msg": "orgID is required"})
    elif cloud_type == PRIVATE_CLOUD:
        tenant_ids = auth.get_tenant_ids()
        if len(tenant_ids) == 0:
            return jsonify({"code": 7, "msg": "tenant id not found"})
    else:
        return jsonify({"code": 7, "msg": "不支持的云类型"})

    plugin = database.fetch_records_by_filters(_PluginTable,
                                               id=workflow_id)

    if not plugin:
        return jsonify({"code": 7, "msg": f"plugin: {workflow_id} not found"})

    openapi_schema_json_str = plugin.plugin_desc_config
    openapi_schema_bytes = openapi_schema_json_str.encode('utf-8')
    openapi_schema_base64 = base64.b64encode(openapi_schema_bytes)
    openapi_schema_base64_str = openapi_schema_base64.decode('utf-8')
    return jsonify({"code": 0, "data": {"base64OpenAPISchema": openapi_schema_base64_str}})



@app.route("/workflow/openapi_schema_internal", methods=["GET"])
def plugin_openapi_schema_internal() -> tuple[Response, int] | Response:
    workflow_id = request.args.get("workflowID")
    cloud_type = auth.get_cloud_type()

    if workflow_id == "":
        return jsonify({"code": 7, "msg": "workflow id not found"})

    if cloud_type == SIMPLE_CLOUD:
        user_id = auth.get_user_id()
        if user_id == "":
            return jsonify({"code": 7, "msg": "user id not found"})
    elif cloud_type == PRIVATE_CLOUD:
        tenant_ids = auth.get_tenant_ids()
        if len(tenant_ids) == 0:
            return jsonify({"code": 7, "msg": "tenant id not found"})
    else:
        return jsonify({"code": 7, "msg": "不支持的云类型"})

    plugin = database.fetch_records_by_filters(_PluginTable,
                                               id=workflow_id)

    if not plugin:
        return jsonify({"code": 7, "msg": f"plugin: {workflow_id} not found"})

    openapi_schema_json_str = plugin.plugin_desc_config
    openapi_schema_bytes = openapi_schema_json_str.encode('utf-8')
    openapi_schema_base64 = base64.b64encode(openapi_schema_bytes)
    openapi_schema_base64_str = openapi_schema_base64.decode('utf-8')
    return jsonify({"code": 0, "data": {"base64OpenAPISchema": openapi_schema_base64_str}})


# 已经发布的workflow直接运行
# @app.route("/plugin/api/run_for_bigmodel/<identifier>/<plugin_en_name>", methods=["POST"])
@app.route("/plugin/api/run_for_bigmodel/<user_id>/<org_id>/<plugin_en_name>", methods=["POST"])
def plugin_run_for_bigmodel(user_id,org_id, plugin_en_name) -> str:
    """
    Input query data and get response.
    """
    loghandler.logger.info(f"===成功调用插件API {plugin_en_name} 租户ID: user_id={user_id}，org_id={org_id} \n")
    if plugin_en_name == "":
        return json.dumps({"code": 7, "msg": "plugin_en_name empty"})

    # 大模型的入参适配
    input_params = request.json
    if not isinstance(input_params, dict):
        return json.dumps({"code": 7, "msg": f"input param type is {type(input_params)}, not dict"})
    loghandler.logger.info(f"=== AI request: {input_params=}")

    # plugin = database.fetch_records_by_filters(
    #     table=_PluginTable,
    #     tenant_id=identifier,
    #     plugin_en_name=plugin_en_name
    # ) or database.fetch_records_by_filters(
    #     table=_PluginTable,
    #     user_id=identifier,
    #     plugin_en_name=plugin_en_name
    # )

    plugin = database.fetch_records_by_filters(
        table=_PluginTable,
        user_id=user_id,
        org_id=org_id,
        plugin_en_name=plugin_en_name
    )

    if not plugin:
        return json.dumps({"code": 7, "msg": "plugin not exists"})

    result = service.plugin_run_for_bigmodel(plugin, input_params, plugin_en_name)

    return result


@app.route("/plugin/api/run_for_stream/<user_id>/<org_id>/<plugin_en_name>", methods=["POST"])
def plugin_run_for_stream(user_id,org_id, plugin_en_name) -> Response:
    """
    Input query data and get response.
    """
    loghandler.logger.info(f"====插件API {plugin_en_name} 租户ID: user_id={user_id}，org_id={org_id} \n")

    if plugin_en_name == "":
        return jsonify({"code": 7, "msg": "plugin_en_name empty"})

    # 大模型的入参适配
    input_params = request.json
    if not isinstance(input_params, dict):
        return jsonify({"code": 7, "msg": f"input param type is {type(input_params)}, not dict"})

    # plugin = database.fetch_records_by_filters(
    #     table=_PluginTable,
    #     tenant_id=identifier,
    #     plugin_en_name=plugin_en_name
    # ) or database.fetch_records_by_filters(
    #     table=_PluginTable,
    #     user_id=identifier,
    #     plugin_en_name=plugin_en_name
    # )

    plugin = database.fetch_records_by_filters(
        table=_PluginTable,
        user_id=user_id,
        org_id=org_id,
        plugin_en_name=plugin_en_name
    )

    if not plugin:
        return jsonify({"code": 7, "msg": "plugin not exists"})

    result = service.plugin_run_for_streaming(plugin, input_params, plugin_en_name)
    return result


# 适配GUI Agent的插件
# @app.route("/plugin/api/run_for_user_defined/<identifier>/<plugin_en_name>", methods=["POST"])
@app.route("/plugin/api/run_for_user_defined/<user_id>/<org_id>/<plugin_en_name>", methods=["POST"])
def plugin_run_for_user_defined(user_id,org_id, plugin_en_name) -> str:
    """
    Input query data and get response.
    """
    loghandler.logger.info(f"===成功调用插件API {plugin_en_name} 租户ID: user_id={user_id}，org_id={org_id}")

    if plugin_en_name == "":
        return json.dumps({"code": 7, "msg": "plugin_en_name empty"})

    # 大模型的入参适配
    input_params = request.json
    if not isinstance(input_params, dict):
        return json.dumps({"code": 7, "msg": f"input param type is {type(input_params)}, not dict"})
    loghandler.logger.info(f"=== AI request: {input_params=}")

    # plugin = database.fetch_records_by_filters(
    #     table=_PluginTable,
    #     tenant_id=identifier,
    #     plugin_en_name=plugin_en_name
    # ) or database.fetch_records_by_filters(
    #     table=_PluginTable,
    #     user_id=identifier,
    #     plugin_en_name=plugin_en_name
    # )

    plugin = database.fetch_records_by_filters(
        table=_PluginTable,
        user_id=user_id,
        org_id=org_id,
        plugin_en_name=plugin_en_name
    )

    if not plugin:
        return json.dumps({"code": 7, "msg": "plugin not exists"})

    result = service.plugin_run_for_bigmodel(plugin, input_params, plugin_en_name)

    return result


@app.route("/node/run_api", methods=["POST"])
def node_run_api() -> Response:
    """
    Input query data and get response.
    """
    # 用户输入的data信息，包含start节点所含信息，config文件存储地址
    content = {"content": "node_run"}
    nodes = request.json.get("nodeSchema")
    nodes_value = nodes.get("nodes", [])
    if len(nodes_value) != 1:
        message = "Node schema is invalid"
        return jsonify({"code": 7, "msg": message})
    node = nodes_value[0]

    try:
        # 使用node_id, 获取需要运行的node配置
        node_config = utils.node_format_convert(node)
        user_id = auth.get_user_id()
        dag = build_dag_for_aibigmodel(node_config, {}, "", user_id)
    except Exception as e:
        loghandler.logger.error(f"node_run_api failed: {repr(e)}")
        return jsonify({"code": 7, "msg": repr(e)})

    # content中的data内容
    result, nodes_result = dag.run_with_param(content, nodes)
    if len(nodes_result) != 1:
        return jsonify({"code": 7, "msg": nodes_result})
    if nodes_result[0]["node_status"] != WorkflowNodeStatus.SUCCESS:
        return jsonify({"code": 7, "msg": nodes_result[0]["node_message"]})

    return jsonify(code=0, data=nodes_result[0]["outputs"])

@app.route("/node/run_python", methods=["POST"])
def node_run_python() -> Response:
    """
    Input query data and get response.
    """
    # 用户输入的data信息，包含start节点所含信息，config文件存储地址
    content = request.json.get("data")
    if not isinstance(content, dict):
        return jsonify({"code": 7, "msg": f"input param type is {type(content)}, not dict"})

    node_schema = request.json.get("nodeSchema")
    loghandler.logger.info(f"nodeSchema: {node_schema}")

    try:
        # 存入数据库的数据为前端格式，需要转换为后端可识别格式
        converted_config = utils.workflow_format_convert(node_schema)
        loghandler.logger.info(f"config: {converted_config}")
        user_id = auth.get_user_id()
        dag = build_dag_for_aibigmodel(converted_config, {}, "", user_id)
    except Exception as e:
        loghandler.logger.error(f"node_run_python failed: {repr(e)}")
        return jsonify({"code": 7, "msg": repr(e)})

    result, nodes_result = dag.run_with_param(content, node_schema)
    if len(nodes_result) != 1:
        return jsonify({"code": 7, "msg": nodes_result})
    if nodes_result[0]["node_status"] != WorkflowNodeStatus.SUCCESS:
        return jsonify({"code": 7, "msg": nodes_result[0]["node_message"]})

    return jsonify(code=0, data=nodes_result[0]["outputs"])

# 根据工作流id获取其开始节点信息（主要为了获取workflow的输入参数定义）
@app.route("/workflow/parameter", methods=["GET"])
def workflow_get_parameter() -> tuple[Response, int] | Response:
    """
    """
    workflow_id = request.args.get('workflowID')
    if workflow_id == "":
        return jsonify({"code": 7, "msg": f"workflowID is Null"})

    # 查询workflow_info表获取插件信息
    workflow_result = database.fetch_records_by_filters(_WorkflowTable,
                                                        id=workflow_id)
    
    if not workflow_result:
        return jsonify({"code": 7, "msg": "No workflow config data exists"})
    
    if not workflow_result.dag_content:
        return jsonify({"code": 7, "msg": "No workflow dag content exists"})
    
    dag_content = json.loads(workflow_result.dag_content)
    start_node_info = utils.extract_start_node(dag_content)

    workflowName = workflow_result.config_name
    # 为start_node_info添加workflowName属性
    if start_node_info:
        start_node_info["workflowName"] = workflowName

    return jsonify(
        {
            "code": 0,
            "data": start_node_info,
            "msg": ""
        },
    )

# 工作流发布成公开应用之后，试用
@app.route("/workflow/use", methods=["POST"])
def workflow_use() -> Response:
    """
    Input query data and get response.
    """
    content = request.json.get("data")
    if not isinstance(content, dict):
        return jsonify({"code": 7, "msg": f"input param type is {type(content)}, not dict"})

    workflow_id = request.json.get("workflowID")
    if workflow_id == "":
        return jsonify({"code": 7, "msg": f"workflowID is Null"})

    # 查询workflow_info表获取插件信息
    workflow_result = database.fetch_records_by_filters(_WorkflowTable,
                                                        id=workflow_id)
    if not workflow_result:
        return jsonify({"code": 7, "msg": "No workflow config data exists"})
    
    workflow_schema = json.loads(workflow_result.dag_content)

    result = service.workflow_run(workflow_id, workflow_result, workflow_schema, content)

    return result

# 画布中的workflow，调试运行
@app.route("/workflow/run", methods=["POST"])
def workflow_run() -> Response:
    """
    Input query data and get response.
    """
    # 用户输入的data信息，包含start节点所含信息，config文件存储地址
    content = request.json.get("data")
    if not isinstance(content, dict):
        return jsonify({"code": 7, "msg": f"input param type is {type(content)}, not dict"})

    workflow_schema = request.json.get("workflowSchema")
    workflow_id = request.json.get("workflowID")
    if workflow_id == "":
        return jsonify({"code": 7, "msg": f"workflowID is Null"})
    loghandler.logger.info(f"workflow_schema: {workflow_schema}")

    # 查询workflow_info表获取插件信息
    workflow_result = database.fetch_records_by_filters(_WorkflowTable,
                                                        id=workflow_id)
    if not workflow_result:
        return jsonify({"code": 7, "msg": "No workflow config data exists"})

    # 保存更新workflow记录与workflow运行记录
    result = service.workflow_run(workflow_id, workflow_result, workflow_schema, content)

    return result


# 画布中的workflow，调试运行
@app.route("/workflow/run_stream", methods=["POST"])
def workflow_run_stream() -> Response:
    """
    Input query data and get response.
    """
    # 用户输入的data信息，包含start节点所含信息，config文件存储地址
    content = request.json.get("data")
    if not isinstance(content, dict):
        return jsonify({"code": 7, "msg": f"input param type is {type(content)}, not dict"})

    workflow_schema = request.json.get("workflowSchema")
    workflow_id = request.json.get("workflowID")
    if workflow_id == "":
        return jsonify({"code": 7, "msg": f"workflowID is Null"})
    loghandler.logger.info(f"workflow_schema: {workflow_schema}")

    # 查询workflow_info表获取插件信息
    workflow_result = database.fetch_records_by_filters(_WorkflowTable,
                                                        id=workflow_id)
    if not workflow_result:
        return jsonify({"code": 7, "msg": "No workflow config data exists"})

    # 限制流式插件输出必须为大模型节点的输出
    dag_content = json.loads(workflow_result.dag_content)
    contains_stream_node = any(
        node.get("type") == "LLMStreamingNode"
        for node in dag_content.get("nodes", [])
    )
    if not contains_stream_node:
        return jsonify(
            {"code": 7, "msg": "流式插件要求将大模型节点(流式输出)作为最终输出节点，以确保数据能够正确以流式格式返回。"})

    # 保存更新workflow记录与workflow运行记录
    result = service.workflow_run_for_streaming(workflow_id, workflow_result, workflow_schema, content)

    return result


@app.route("/workflow/mcp_server_tool_list", methods=["POST"])
def mcp_server_tool_list() -> Response:
    """
    获取tool_list并验证（存在至少一个tool则通过校验）
    """
    mcp_server_url = request.json.get("mcpServerUrl")
    if not mcp_server_url:
        return jsonify({
            "code": 7,
            "msg": "Missing required parameter: mcpServerUrl"
        })

    loghandler.logger.info(f"===MCP Server URL: {mcp_server_url}=====")

    async def _async_logic(url: str):
        # 1. 获取MCP工具列表
        try:
            mcp_client = MCPClient()  # 注意：确保MCPClient支持异步
            tools_list = await mcp_client.connect_server(url)
            # 2. 校验工具数量
            if not tools_list:
                return {
                    "code": 7,
                    "msg": "No available tools found on MCP server",
                }

            # 3. 处理数据
            if not isinstance(tools_list, (list, dict)):
                tools_list = list(tools_list)  # 转换为可序列化格式

            return {
                "code": 0,
                "msg": "Success",
                "data": {
                    "tools_list": tools_list  # 注意：去掉json.dumps避免双重序列化
                }
            }

        except Exception as err:
            loghandler.logger.error(f"Failed to get tool list: {str(err)}")
            return {
                "code": 7,
                "msg": f"MCP server connection error: {str(err)}",
            }

        finally:
            # 尝试断开连接并清理资源
            await mcp_client.disconnect()

    try:
        # 运行异步逻辑
        response_data = asyncio.run(_async_logic(mcp_server_url))
        return jsonify(response_data)

    except Exception as e:
        loghandler.logger.error(f"Async execution failed: {str(e)}")
        return jsonify({
            "code": 7,
            "msg": "Internal server error",
        })


@app.route("/workflow/create", methods=["POST"])
def workflow_create() -> Response:
    # request 参数获取
    data = request.json
    config_name = data.get("configName")
    config_en_name = data.get("configENName")
    config_desc = data.get("configDesc")
    tenant_id = data.get('tenantId')
    is_stream = data.get('isStream', False)
    if ' ' in config_en_name:
        return jsonify({"code": 7, "msg": "英文名称不允许有空格"})
    if not config_name or not config_desc:
        return jsonify({"code": 7, "msg": "configName,configDesc is required"})
    # 适配私有云
    cloud_type = auth.get_cloud_type()
    user_id = auth.get_user_id()
    org_id = auth.get_org_id()
    tenant_ids = auth.get_tenant_ids()

    # 用户未输入英文名称，则自动生成，用户输入英文名称，则保存用户输入
    if config_en_name == "":
        config_en_name = utils.chinese_to_pinyin(config_name)
        workflow_results = _WorkflowTable.query.filter(
            _WorkflowTable.config_en_name.like(f"{config_en_name}%"),
            _WorkflowTable.org_id == org_id,
            _WorkflowTable.user_id == user_id if cloud_type == SIMPLE_CLOUD else _WorkflowTable.tenant_id.in_(
                tenant_ids)
        ).all()
        # 查询表中同一用户下是否有重复config_en_name的记录
        if not workflow_results:
            response = create_new_workflow(org_id,user_id, config_name, config_en_name, config_desc, tenant_id, is_stream)
        else:
            name_suffix = utils.add_max_suffix(config_en_name, workflow_results)
            # 生成新的配置名称
            new_config_en_name = f"{config_en_name}_{name_suffix}"
            response = create_new_workflow(org_id,user_id, config_name, new_config_en_name, config_desc, tenant_id, is_stream)
        return response
    else:
        workflow_results = _WorkflowTable.query.filter(
            _WorkflowTable.user_id == user_id,
            _WorkflowTable.org_id == org_id,
            _WorkflowTable.config_en_name == config_en_name
        ).all()
        # 查询表中同一用户下是否有重复config_en_name的记录
        if not workflow_results:
            response = create_new_workflow(org_id,user_id, config_name, config_en_name, config_desc, tenant_id, is_stream)
            return response
        else:
            return jsonify({"code": 7, "msg": "该英文名称已存在, 请重新填写"})


def create_new_workflow(org_id: str,user_id: str, config_name: str, config_en_name: str, config_desc: str,
                        tenant_id: str, is_stream: bool) -> Response:
    dag_content = utils.generate_workflow_schema_template()
    workflow_id = uuid.uuid4()
    try:
        db.session.add(
            _WorkflowTable(
                id=str(workflow_id),
                user_id=user_id,
                org_id=org_id,
                config_name=config_name,
                config_en_name=config_en_name,
                config_desc=config_desc,
                status=utils.WorkflowStatus.WORKFLOW_DRAFT,
                updated_time=datetime.now(),
                dag_content=dag_content,
                tenant_id=tenant_id,
                is_stream=is_stream,
            ),
        )
        db.session.commit()
    except SQLAlchemyError as e:
        db.session.rollback()
        return jsonify({"code": 7, "msg": str(e)})

    data = {
        "workflowID": str(workflow_id),
        "configName": config_name,
        "configENName": config_en_name,
        "configDesc": config_desc
    }
    return jsonify({"code": 0, "data": data, "msg": "Workflow file created successfully"})


@app.route("/workflow/delete", methods=["DELETE"])
def workflow_delete() -> Response:
    workflow_id = request.json.get("workflowID")
    cloud_type = auth.get_cloud_type()
    jwt_token = request.headers.get('Authorization')

    # 根据 cloud_type 确定查询条件
    query_filter = []

    if cloud_type == SIMPLE_CLOUD:
        user_id = auth.get_user_id()
        org_id = auth.get_org_id()
        query_filter.append(_WorkflowTable.id == workflow_id)
        query_filter.append(_WorkflowTable.user_id == user_id)
        query_filter.append(_WorkflowTable.org_id == org_id)
    elif cloud_type == PRIVATE_CLOUD:
        tenant_ids = auth.get_tenant_ids()
        query_filter.append(_WorkflowTable.id == workflow_id)
        query_filter.append(_WorkflowTable.tenant_id.in_(tenant_ids))
    else:
        return jsonify({"code": 7, "msg": "不支持的云类型"})

    workflow_results = _WorkflowTable.query.filter(*query_filter).all()

    if workflow_results:
        # response = service.unregister_openapi_plugins(workflow_id, jwt_token)
        # if response.status == service.service_status.ServiceExecStatus.ERROR:
        #     return jsonify({"code": 7, "msg": f"插件删除失败: 网关解绑插件API服务失败"})
        try:
            db.session.query(_WorkflowTable).filter(*query_filter).delete()
            # 删除 PluginTable 中与 Workflow 关联的数据
            db.session.query(_PluginTable).filter_by(id=workflow_id).delete()
            db.session.commit()
        except SQLAlchemyError as e:
            db.session.rollback()
            return jsonify({"code": 5000, "msg": str(e)})
        return jsonify({"code": 0, "msg": "Workflow file deleted successfully"})
    else:
        return jsonify({"code": 7, "msg": "Record not found"})


@app.route("/workflow/save", methods=["POST"])
def workflow_save() -> Response:
    """
    Save the workflow JSON data to the local user folder.
    """
    data = request.json
    config_name = data.get("configName")
    config_en_name = data.get("configENName")
    config_desc = data.get("configDesc")
    workflow_dict = data.get("workflowSchema")
    workflow_id = data.get("workflowID")
    user_id = auth.get_user_id()
    org_id = auth.get_org_id()
    tenant_ids = auth.get_tenant_ids()

    if ' ' in config_en_name:
        return jsonify({"code": 7, "msg": "英文名称不允许有空格"})

    if not config_name or not config_en_name:
        return jsonify({"code": 7, "msg": "插件名称不能为空，请重新填写"})

    if not workflow_id or not user_id:
        return jsonify({"code": 7, "msg": "workflowID is required"})
    if not org_id:
        return jsonify({"code": 7, "msg": "orgID is required"})

    result = service.workflow_save(workflow_id, config_name, config_en_name, config_desc, workflow_dict, user_id,org_id,
                                   tenant_ids)
    return result


@app.route("/workflow/clone", methods=["POST"])
def workflow_clone() -> Response:
    """
    Copy the workflow JSON data as a new one.
    """
    data = request.json
    workflow_id = data.get("workflowID")
    user_id = auth.get_user_id()
    org_id = auth.get_org_id()
    cloud_type = auth.get_cloud_type()
    tenant_ids = auth.get_tenant_ids()
    if not workflow_id:
        return jsonify({"code": 7, "msg": "workflowID is required"})

    # 查找工作流配置
    if cloud_type == SIMPLE_CLOUD:
        workflow_config = database.fetch_records_by_filters(_WorkflowTable,
                                                            id=workflow_id,
                                                            user_id=user_id,
                                                            org_id=org_id)
    elif cloud_type == PRIVATE_CLOUD:
        workflow_config = database.fetch_records_by_filters(
            _WorkflowTable,
            id=workflow_id,
            tenant_id__in=tenant_ids)

    else:
        return jsonify({"code": 7, "msg": "不支持的云类型"})

    if not workflow_config:
        return jsonify({"code": 7, "msg": "workflow_config does not exist"})
    # 拆分到service
    result = service.workflow_clone(workflow_config, user_id,org_id, tenant_ids)
    return result


@app.route("/workflow/example_clone", methods=["POST"])
def workflow_example_clone() -> Response:
    # request 参数获取
    data = request.json
    workflow_id = data.get("workflowID")
    tenant_id = data.get("tenantId")
    config_name = data.get("configName")
    config_en_name = data.get("configENName")
    config_desc = data.get("configDesc")

    if ' ' in config_en_name:
        return jsonify({"code": 7, "msg": "英文名称不允许有空格"})
    if not config_name or not config_desc:
        return jsonify({"code": 7, "msg": "configName,configDesc is required"})

    # 适配私有云
    cloud_type = auth.get_cloud_type()
    user_id = auth.get_user_id()
    org_id = auth.get_org_id()
    tenant_ids = auth.get_tenant_ids()

    # 用户未输入英文名称，则自动生成，用户输入英文名称，则保存用户输入
    if config_en_name == "":
        config_en_name = utils.chinese_to_pinyin(config_name)
        workflow_results = _WorkflowTable.query.filter(
            _WorkflowTable.config_en_name.like(f"{config_en_name}%"),
            _WorkflowTable.user_id == user_id if cloud_type == SIMPLE_CLOUD else _WorkflowTable.tenant_id.in_(
                tenant_ids),
            _WorkflowTable.org_id == org_id
        ).all()
        # 查询表中同一用户下是否有重复config_en_name的记录
        if not workflow_results:
            response = service.workflow_example_clone(workflow_id, org_id, user_id, config_name, config_en_name, config_desc,
                                                      tenant_id)
        else:
            name_suffix = utils.add_max_suffix(config_en_name, workflow_results)
            # 生成新的配置名称
            config_name = f"{config_name}{name_suffix}"
            new_config_en_name = f"{config_en_name}_{name_suffix}"
            response = service.workflow_example_clone(workflow_id, org_id, user_id, config_name, new_config_en_name,
                                                      config_desc, tenant_id)
        return response
    else:
        workflow_results = _WorkflowTable.query.filter(
            _WorkflowTable.user_id == user_id,
            _WorkflowTable.org_id == org_id,
            _WorkflowTable.config_en_name == config_en_name
        ).all()
        # 查询表中同一用户下是否有重复config_en_name的记录
        if not workflow_results:
            response = service.workflow_example_clone(workflow_id, org_id, user_id, config_name, config_en_name, config_desc,
                                                      tenant_id)
            return response
        else:
            return jsonify({"code": 7, "msg": "该英文名称已存在, 请重新填写"})


@app.route("/workflow/get", methods=["GET"])
def workflow_get() -> tuple[Response, int] | Response:
    """
    Reads and returns workflow data from the specified JSON file.
    """
    workflow_id = request.args.get('workflowID')
    cloud_type = auth.get_cloud_type()
    if not workflow_id:
        return jsonify({"error": "workflowID is required"}), 7

    if cloud_type == SIMPLE_CLOUD:
        user_id = auth.get_user_id()
        org_id = auth.get_org_id()
        workflow_config = database.fetch_records_by_filters(_WorkflowTable,
                                                            method='first',
                                                            id=workflow_id,
                                                            user_id=user_id,
                                                            org_id=org_id)

    elif cloud_type == PRIVATE_CLOUD:
        tenant_ids = auth.get_tenant_ids()
        workflow_config = database.fetch_records_by_filters(_WorkflowTable,
                                                            method='first',
                                                            id=workflow_id,
                                                            tenant_id__in=tenant_ids)
    else:
        return jsonify({"code": 7, "msg": "不支持的云类型"})

    if not workflow_config:
        return jsonify({"code": 7, "msg": "workflow_config not exists"})

    dag_content = json.loads(workflow_config.dag_content)
    data = {
        "configName": workflow_config.config_name,
        "configENName": workflow_config.config_en_name,
        "configDesc": workflow_config.config_desc,
        "workflowSchema": dag_content,
        "tenant_id": workflow_config.tenant_id
    }
    return jsonify(
        {
            "code": 0,
            "data": data,
            "msg": ""
        },
    )


@app.route("/workflow/status", methods=["GET"])
def workflow_get_process() -> tuple[Response, int] | Response:
    """
    Reads and returns workflow process results from the specified JSON file.
    """
    execute_id = request.args.get("executeID")
    cloud_type = auth.get_cloud_type()
    if cloud_type == SIMPLE_CLOUD:
        user_id = auth.get_user_id()
        org_id = auth.get_org_id()
        workflow_result = database.fetch_records_by_filters(_ExecuteTable,
                                                            method='first',
                                                            execute_id=execute_id,
                                                            user_id=user_id,
                                                            org_id=org_id)
    elif cloud_type == PRIVATE_CLOUD:
        tenant_ids = auth.get_tenant_ids()
        workflow_result = database.fetch_records_by_filters(_ExecuteTable,
                                                            method='first',
                                                            execute_id=execute_id,
                                                            tenant_id__in=tenant_ids)
    else:
        return jsonify({"code": 7, "msg": "不支持的云类型"})

    if not workflow_result:
        return jsonify({"code": 7, "msg": "workflow_result not exists"})

    workflow_result.execute_result = json.loads(workflow_result.execute_result)

    return jsonify(
        {
            "code": 0,
            "data": {"result": workflow_result.execute_result},
            "msg": ""
        },
    )


@app.route("/workflow/list", methods=["GET"])
def workflow_get_list() -> tuple[Response, int] | Response:
    """
    Reads and returns workflow data from the specified JSON file.
    """
    cloud_type = auth.get_cloud_type()
    page = request.args.get('pageNo', default=1)
    limit = request.args.get('pageSize', default=10)
    keyword = request.args.get('keyword', default='')
    status = request.args.get('status', default='')
    is_stream = request.args.get('isStream', default='')

    result = service.get_workflow_list(cloud_type, keyword, status, page, limit, is_stream)

    return result


# 获取插件列表-内部调用使用，去除用户鉴权
@app.route("/workflow/list_internal", methods=["GET"])
def workflow_get_list_for_internal() -> tuple[Response, int] | Response:
    """
    Reads and returns workflow data from the specified JSON file.
    """
    page = request.args.get('pageNo', default=1)
    limit = request.args.get('pageSize', default=10000)
    keyword = request.args.get('keyword', default='')
    status = request.args.get('status', default='')

    result = service.get_workflow_list_for_internal(keyword, status, page, limit)

    return result


# 获取APP的静态token
@app.route("/workflow/static_token", methods=["GET"])
def get_app_token() -> tuple[Response, int] | Response:
    appid = request.args.get('appid')
    api_key = request.args.get('apiKey')
    if not all([appid, api_key]):
        return jsonify({"code": 7, "msg": "参数校验错误，请检查参数"})

    result = service.get_app_token(appid, api_key)
    return result


def create_preset_data():
    # 检查数据表中是否存在预置数据
    if not _WorkflowTable.query.filter_by(id='example').first():
        preset_data = _WorkflowTable(
            id='example',
            user_id='1',
            org_id='1',
            config_name='美食推荐-标准示例',
            config_en_name='MeiShiTuiJian-Example',
            config_desc='根据输入的位置信息搜索国内的美食店铺',
            dag_content='{"nodes": [{"id": "c0f17048-59b9-4417-8596-43d8c2c05dd4", "name": "\u5f00\u59cb", '
                        '"type": "StartNode", "data": {"outputs": [{"list_schema": "", "name": "keywords", '
                        '"object_schema": "", "type": "string", "value": {"type": "generated", "content": ""}, '
                        '"required": "false", "desc": '
                        '"\u7f8e\u98df\u76f8\u5173\u7684\u540d\u79f0\uff0c\u4f8b\u5982\u5496\u5561\u9986\u3001\u996d'
                        '\u5e97\u7b49"}, {"name": "pois", "type": "string", "value": {"type": "generated", '
                        '"content": ""}, "required": true, "desc": '
                        '"\u5177\u4f53\u7684\u5730\u70b9\u540d\u79f0\uff0c\u4f8b\u5982\u5317\u4eac\u3001\u6d77\u6dc0'
                        '\u533a\u3001\u540e\u5382\u6751\u8def"}], "inputs": [], "settings": {}}}, '
                        '{"id": "b1c1def1-41a4-4473-8cdb-9137857fdd1d", "name": "\u7ed3\u675f", "type": "EndNode", '
                        '"data": {"outputs": [], "inputs": [{"list_schema": "", "name": "answer", "newRefContent": '
                        '"API_1/pois", "object_schema": "", "type": "string", "value": {"type": "ref", "content": {'
                        '"ref_node_id": "apinode_1735027638635", "ref_var_name": "pois"}}, "required": "false", '
                        '"desc": ""}], "settings": {}}}, {"id": "apinode_", "name": "API", "type": "ApiNode", '
                        '"data": {"outputs": [{"name": "count", "type": "string", "value": {"type": "generated", '
                        '"content": ""}}, {"name": "infocode", "type": "string", "value": {"type": "generated", '
                        '"content": ""}}, {"name": "pois", "type": "object", "value": {"type": "generated", '
                        '"content": ""}}, {"name": "info", "type": "string", "value": {"type": "generated", '
                        '"content": ""}}, {"name": "status", "type": "string", "value": {"type": "generated", '
                        '"content": ""}}], "inputs": [{"newValue": "\u96cd\u548c\u5bab", "extra": {"location": '
                        '"query"}, "name": "keywords", "newRefContent": "\u5f00\u59cb/pois", "type": "string", '
                        '"value": {"type": "ref", "content": {"ref_node_id": "c0f17048-59b9-4417-8596-43d8c2c05dd4", '
                        '"ref_var_name": "pois"}}, "required": false, "desc": ""}, {"newValue": '
                        '"77b5f0d102c848d443b791fd469b732d", "extra": {"location": "query"}, "name": "key", '
                        '"type": "string", "value": {"type": "generated", "content": '
                        '"77b5f0d102c848d443b791fd469b732d"}, "required": false, "desc": ""}, {"newValue": "", '
                        '"extra": {"location": "query"}, "name": "", "type": "string", "value": {"type": "ref", '
                        '"content": {"ref_node_id": "", "ref_var_name": ""}}, "required": false, "desc": ""}], '
                        '"settings": {"headers": {}, "http_method": "GET", "content_type": "application/json", '
                        '"url": "https://restapi.amap.com/v5/place/text"}}}, {"id": "pythonnode_", '
                        '"name": "\u4ee3\u7801", "type": "PythonNode", "data": {"outputs": [{"name": "key0", '
                        '"newRefContent": "", "type": "string", "value": {"type": "generated", "content": ""}, '
                        '"required": false, "desc": ""}], "inputs": [{"name": "pois", "newRefContent": "API/pois", '
                        '"type": "string", "value": {"type": "ref", "content": {"ref_node_id": "apinode_", '
                        '"ref_var_name": "pois"}}, "required": true, "desc": ""}], "settings": {"code": '
                        '"IyDlrprkuYnkuIDkuKogbWFpbiDlh73mlbDvvIznlKjmiLflj6rog73lnKhtYWlu5Ye95pWw6YeM5YGa5Luj56CB5byA5Y+R44CCDQojIOWFtuS4re+8jOWbuuWumuS8oOWFpSBwYXJhbXMg5Y+C5pWw77yI5a2X5YW45qC85byP77yJ77yM5a6D5YyF5ZCr5LqG6IqC54K56YWN572u55qE5omA5pyJ6L6T5YWl5Y+Y6YeP44CCDQojIOWFtuS4re+8jOWbuuWumui/lOWbniBvdXRwdXRfcGFyYW1zIOWPguaVsO+8iOWtl+WFuOagvOW8j++8ie+8jOWug+WMheWQq+S6huiKgueCuemFjee9rueahOaJgOaciei+k+WHuuWPmOmHj+OAgg0KIyDov5DooYznjq/looMgUHl0aG9uMy4NCg0KIyBtYWluIOWHveaVsO+8jOWbuuWumuS8oOWFpSBwYXJhbXMg5Y+C5pWwDQpkZWYgbWFpbihwYXJhbXMpOg0KICAgICMg55So5oi36Ieq5a6a5LmJ6YOo5YiGLi4uLi4uDQoNCiAgICAjIOWbuuWumui/lOWbniBvdXRwdXRfcGFyYW1zIOWPguaVsA0KICAgIG91dHB1dF9wYXJhbXMgPSB7DQogICAgICAgIyDnlKjmiLfoh6rlrprkuYnpg6jliIYuLi4uLi4NCiAgICAgICAia2V5MCI6IHBhcmFtc1sncG9pcyddWzBdWyJsb2NhdGlvbiJdLA0KICAgIH0NCiAgICByZXR1cm4gb3V0cHV0X3BhcmFtcw0K", "language": "Python"}}}, {"id": "apinode_1735027638635", "name": "API_1", "type": "ApiNode", "data": {"outputs": [{"name": "count", "type": "string", "value": {"type": "generated", "content": ""}}, {"name": "infocode", "type": "string", "value": {"type": "generated", "content": ""}}, {"name": "pois", "type": "object", "value": {"type": "generated", "content": ""}}, {"name": "info", "type": "string", "value": {"type": "generated", "content": ""}}, {"name": "status", "type": "string", "value": {"type": "generated", "content": ""}}], "inputs": [{"newValue": "116.418294,39.949199", "extra": {"location": "query"}, "name": "location", "newRefContent": "\u4ee3\u7801/key0", "type": "string", "value": {"type": "ref", "content": {"ref_node_id": "pythonnode_", "ref_var_name": "key0"}}, "required": false, "desc": ""}, {"newValue": "\u5496\u5561\u9986", "extra": {"location": "query"}, "name": "keywords", "newRefContent": "\u5f00\u59cb/keywords", "type": "string", "value": {"type": "ref", "content": {"ref_node_id": "c0f17048-59b9-4417-8596-43d8c2c05dd4", "ref_var_name": "keywords"}}, "required": false, "desc": ""}, {"newValue": "77b5f0d102c848d443b791fd469b732d", "extra": {"location": "query"}, "name": "key", "type": "string", "value": {"type": "generated", "content": "77b5f0d102c848d443b791fd469b732d"}, "required": false, "desc": ""}, {"newValue": "", "extra": {"location": "query"}, "name": "", "type": "string", "value": {"type": "ref", "content": {"ref_node_id": "", "ref_var_name": ""}}, "required": false, "desc": ""}], "settings": {"headers": {}, "http_method": "GET", "content_type": "application/json", "url": "https://restapi.amap.com/v5/place/around"}}}], "edges": [{"source_node_id": "pythonnode_", "source_port": "pythonnode_-right", "target_node_id": "apinode_1735027638635", "target_port": "apinode_1735027638635-left"}, {"source_node_id": "apinode_1735027638635", "source_port": "apinode_1735027638635-right", "target_node_id": "b1c1def1-41a4-4473-8cdb-9137857fdd1d", "target_port": "b1c1def1-41a4-4473-8cdb-9137857fdd1d-left"}, {"source_node_id": "apinode_", "source_port": "apinode_-right", "target_node_id": "pythonnode_", "target_port": "pythonnode_-left"}, {"source_node_id": "c0f17048-59b9-4417-8596-43d8c2c05dd4", "source_port": "c0f17048-59b9-4417-8596-43d8c2c05dd4-right", "target_node_id": "apinode_", "target_port": "apinode_-left"}]}',
            status='draft',
            updated_time=datetime.now(),
            execute_status='',
            tenant_id='',
            example_flag=utils.WorkflowType.WORKFLOW_EXAMPLE  # 设置示例标志为样例
        )
        db.session.add(preset_data)
        db.session.commit()


def init(
        host: str = "0.0.0.0",
        port: int = SERVER_PORT,
        run_dirs: Optional[Union[str, list[str]]] = None,
        debug: bool = False,
) -> None:
    """Start the AgentScope Studio web UI with the given configurations.

    Args:
        host (str, optional):
            The host of the web UI. Defaults to "127.0.0.1"
        port (int, optional):
            The port of the web UI. Defaults to 5000.
        run_dirs (`Optional[Union[str, list[str]]]`, defaults to `None`):
            The directories to search for the history of runtime instances.
        debug (`bool`, optional):
            Whether to enable the debug mode. Defaults to False.
    """

    # Set the history directories
    if isinstance(run_dirs, str):
        run_dirs = [run_dirs]

    global _RUNS_DIRS
    _RUNS_DIRS = run_dirs

    # Create the cache directory
    with app.app_context():
        db.create_all()
        create_preset_data()

    # if debug:
    #     app.logger.setLevel("DEBUG")
    # else:
    #     app.logger.setLevel("INFO")

    # To be compatible with the old table schema, we need to check and convert
    # the id column of the message_table from INTEGER to VARCHAR.
    # _check_and_convert_id_type(str(_cachedb), "message_table")

    socketio.run(
        app,
        host=host,
        port=port,
        debug=debug,
        allow_unsafe_werkzeug=True,
    )


if __name__ == "__main__":
    init()

    # 1. 所有节点的入参和出参，统一到一个全局变量池子里，并且初始化时能够正确串联。
    # 2. API节点和python节点的封装和定义，完备代码实现。
