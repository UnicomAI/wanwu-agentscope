import json
import time
import uuid
import traceback

import agentscope.aibigmodel_workflow.utils as utils
import agentscope.utils.jwt_auth as auth

from datetime import datetime
from flask import jsonify, Response
from sqlalchemy.exc import SQLAlchemyError
from agentscope.web.workstation.workflow_utils import WorkflowNodeStatus
from agentscope.utils.jwt_auth import SIMPLE_CLOUD, PRIVATE_CLOUD
from agentscope.web.workstation.workflow_dag import build_dag_for_aibigmodel
from agentscope.aibigmodel_workflow.loghandler import loghandler
from agentscope.aibigmodel_workflow.config import db, SERVICE_URL, SERVICE_INNER_URL, BFF_SERVICE_ENDPOINT, KONG_SERVICE, AGENTSCOPE_HOST
from agentscope.aibigmodel_workflow.database import _ExecuteTable, _WorkflowTable, _PluginTable, _AppTokenTable, \
    fetch_records_by_filters, save_app_token
from agentscope.service import api_request, service_status
from urllib.parse import urlencode

# 导入redis_queue，优先使用绝对导入，失败时使用相对导入
try:
    from config import redis_queue
except ImportError:
    from .config import redis_queue
from sqlalchemy import or_, and_


def plugin_publish(workflow_id, org_id,user_id, workflow_result, plugin_field, description, jwt_token, is_stream):
    # 插件描述信息生成，对接智能体格式
    dag_content = json.loads(workflow_result.dag_content)

    data = {
        "id": workflow_id,
        "pluginName": workflow_result.config_name,
        "pluginDesc": workflow_result.config_desc,
        "pluginENName": workflow_result.config_en_name,
        "pluginField": plugin_field,
        "pluginDescription": description,
        "pluginSpec": dag_content,
        "identifier": user_id if auth.get_cloud_type() == SIMPLE_CLOUD else workflow_result.tenant_id,
        "serviceURL": SERVICE_URL,
        "user_id": user_id,
        "org_id": org_id
    }

    # 检查是否存在 "GUIAgentNode" 类型的节点
    contains_gui_agent = any(node.get("type") == "GUIAgentNode" for node in dag_content["nodes"])

    # 根据标志变量处理逻辑
    if contains_gui_agent and cloud_type == SIMPLE_CLOUD:
        openapi_schema = utils.plugin_desc_config_generator_user_defined(data)
        openapi_schema_json_str = json.dumps(openapi_schema)
        # 向网关注册API接口，实现用户调用API的管控
        # response = register_openapi_plugins(workflow_id, data, jwt_token)
        # if response.status == service_status.ServiceExecStatus.ERROR:
        #     return jsonify({"code": 7, "msg": f"插件发布失败: 网关注册插件API服务失败"})

    elif is_stream and cloud_type == SIMPLE_CLOUD:
        openapi_schema = utils.plugin_desc_config_generator_for_stream(data)
        openapi_schema_json_str = json.dumps(openapi_schema)
        # 向网关注册API接口，实现用户调用API的管控
        # response = register_openapi_plugins(workflow_id, data, jwt_token)
        # if response.status == service_status.ServiceExecStatus.ERROR:
        #     return jsonify({"code": 7, "msg": f"插件发布失败: 网关注册插件API服务失败"})
    else:
        openapi_schema = utils.plugin_desc_config_generator(data)
        openapi_schema_json_str = json.dumps(openapi_schema)

    try:
        db.session.add(
            _PluginTable(
                id=workflow_id,
                user_id=user_id,
                org_id=org_id,
                plugin_name=workflow_result.config_name,
                plugin_en_name=workflow_result.config_en_name,
                plugin_desc=workflow_result.config_desc,
                dag_content=workflow_result.dag_content,
                plugin_field=data["pluginField"],
                plugin_desc_config=openapi_schema_json_str,
                published_time=datetime.now(),
                tenant_id=workflow_result.tenant_id,
                contains_gui_agent=contains_gui_agent,
                is_stream=workflow_result.is_stream
            ),
        )
        db.session.query(_WorkflowTable).filter_by(id=workflow_id).update(
            {_WorkflowTable.status: utils.WorkflowStatus.WORKFLOW_PUBLISHED})
        db.session.commit()
    except SQLAlchemyError as e:
        db.session.rollback()
        return jsonify({"code": 7, "msg": str(e)})
    except Exception as e:
        loghandler.logger.error(f"plugin_publish failed: {e}")
        return jsonify({"code": 7, "msg": str(e)})

    return jsonify({"code": 0, "msg": "Workflow file published successfully"})


def build_and_run_dag(workflow_schema, content, workflow_id, user_id):
    try:
        converted_config = utils.workflow_format_convert(workflow_schema)
        dag = build_dag_for_aibigmodel(converted_config, {}, workflow_id, user_id)
    except Exception as e:
        loghandler.logger.error(f"Workflow_run failed: {repr(e)}")
        return None, None, None, WorkflowNodeStatus.FAILED

    start_time = time.time()
    try:
        result, nodes_result = dag.run_with_param(content, workflow_schema)
        loghandler.logger.info(f"输出结果result: {result} \n 节点输出结果nodes_result: {nodes_result}")
    except Exception as e:
        loghandler.logger.error(f"Workflow execution failed: {repr(e)}")
        return dag, None, None, WorkflowNodeStatus.FAILED

    end_time = time.time()
    executed_time = round(end_time - start_time, 3)
    execute_status = WorkflowNodeStatus.SUCCESS if all(
        node.get('node_status') in [WorkflowNodeStatus.SUCCESS, WorkflowNodeStatus.RUNNING_SKIP]
        for node in nodes_result) else WorkflowNodeStatus.FAILED
    execute_result = utils.get_workflow_running_result(nodes_result, dag.uuid, execute_status, str(executed_time))

    loghandler.logger.info(f"Execute_result: {execute_result}")
    execute_result = json.dumps(execute_result)

    return dag, result, execute_result, execute_status


def build_and_run_dag_for_stream(workflow_schema, content, workflow_id):
    error_msg = ""

    try:
        converted_config = utils.workflow_format_convert(workflow_schema)
        loghandler.logger.info(f"config: {converted_config}")
        user_id = auth.get_user_id()
        dag = build_dag_for_aibigmodel(converted_config, {}, workflow_id, user_id)
    except Exception as e:
        error_msg = f"{user_id}, workflow: {workflow_id}, workflow build failed: {repr(e)}"
        loghandler.logger.error(f'{user_id}, {traceback.format_exc()}')
        loghandler.logger.error(error_msg)
        return None, None, None, None, error_msg

    start_time = time.time()
    try:
        combined_topic_id, nodes_result = dag.run_with_param_for_streaming(content, workflow_schema)
    except Exception as e:
        error_msg = f"dag: {dag.uuid}, Workflow execution failed: {repr(e)}"
        loghandler.logger.error(error_msg)
        return dag, None, None, None, error_msg

    end_time = time.time()
    executed_time = round(end_time - start_time, 3)
    execute_status = WorkflowNodeStatus.SUCCESS if all(
        node.get('node_status') in [WorkflowNodeStatus.SUCCESS, WorkflowNodeStatus.RUNNING_SKIP]
        for node in nodes_result) else WorkflowNodeStatus.FAILED
    execute_result = utils.get_workflow_running_result(nodes_result, dag.uuid, execute_status, str(executed_time))

    loghandler.logger.info(f"Execute_result: {execute_result}")
    execute_result = json.dumps(execute_result)

    error_msg = dag.get_nodes_wrong_status_message(dag.uuid, nodes_result)
    return dag, combined_topic_id, execute_result, execute_status, error_msg


def workflow_run(workflow_id, workflow_result, workflow_schema, content):
    user_id = auth.get_user_id()
    org_id = auth.get_org_id()
    tenant_ids = auth.get_tenant_ids()
    cloud_type = auth.get_cloud_type()

    # 生成执行插件dag
    dag, result, execute_result, execute_status = build_and_run_dag(workflow_schema, content, workflow_id, user_id)
    if not execute_result:
        return jsonify({"code": 7, "msg": "execute result not exists"})

    if dag is None:
        return jsonify({"code": 7, "msg": f"dag is None, {execute_result}"})

    try:
        # 限制一个用户执行记录为500次，超过500次删除最旧的记录
        query = db.session.query(_ExecuteTable).filter_by(user_id=user_id if user_id == 'SIMPLE_CLOUD' else
        _ExecuteTable.tenant_id.in_(tenant_ids))

        if query.count() > 500:
            oldest_record = db.session.query(_ExecuteTable).filter_by(user_id=user_id).filter_by(org_id=org_id).order_by(
                _ExecuteTable.executed_time).first() if cloud_type == SIMPLE_CLOUD else db.session.query(
                _ExecuteTable).filter(_ExecuteTable.tenant_id.in_(tenant_ids)).order_by(
                _ExecuteTable.executed_time).first()
            if oldest_record:
                db.session.delete(oldest_record)

        db.session.add(_ExecuteTable(execute_id=dag.uuid, execute_result=execute_result, user_id=user_id,org_id=org_id,
                                     executed_time=datetime.now(), workflow_id=workflow_id,
                                     tenant_id=workflow_result.tenant_id))

        if cloud_type == SIMPLE_CLOUD:
            db.session.query(_WorkflowTable).filter_by(id=workflow_id, user_id=user_id, org_id=org_id).update({
                _WorkflowTable.execute_status: execute_status})
        elif cloud_type == PRIVATE_CLOUD:
            db.session.query(_WorkflowTable).filter(
                _WorkflowTable.id == workflow_id, _WorkflowTable.tenant_id.in_(tenant_ids)).update(
                {_WorkflowTable.execute_status: execute_status})
        else:
            return jsonify({"code": 7, "msg": "不支持的云类型"})
        db.session.commit()
    except SQLAlchemyError as e:
        db.session.rollback()
        return jsonify({"code": 7, "msg": str(e)})

    return jsonify(code=0, data=result, executeID=dag.uuid)


def workflow_run_for_streaming(workflow_id, workflow_result, workflow_schema, content):
    user_id = auth.get_user_id()
    tenant_ids = auth.get_tenant_ids()
    cloud_type = auth.get_cloud_type()

    loghandler.logger.info(f"===插件 {workflow_id} 调试 ===")
    # 生成执行插件dag与生成的combined_topic_id流
    dag, combined_topic_id, execute_result, execute_status, err_msg = (
        build_and_run_dag_for_stream(workflow_schema, content, workflow_id))
    execute_status = WorkflowNodeStatus.SUCCESS
    if err_msg != "":
        execute_status = WorkflowNodeStatus.FAILED

    if dag is None:
        return jsonify({"code": 7, "msg": err_msg})

    try:
        db.session.add(_ExecuteTable(execute_id=dag.uuid, execute_result=execute_result, user_id=user_id,
                                     executed_time=datetime.now(), workflow_id=workflow_id,
                                     tenant_id=workflow_result.tenant_id))

        if cloud_type == SIMPLE_CLOUD:
            db.session.query(_WorkflowTable).filter_by(id=workflow_id, user_id=user_id).update({
                _WorkflowTable.execute_status: execute_status})
        elif cloud_type == PRIVATE_CLOUD:
            db.session.query(_WorkflowTable).filter(
                _WorkflowTable.id == workflow_id, _WorkflowTable.tenant_id.in_(tenant_ids)).update(
                {_WorkflowTable.execute_status: execute_status})
        else:
            return jsonify({"code": 7, "msg": "不支持的云类型"})
        db.session.commit()
    except SQLAlchemyError as e:
        db.session.rollback()
        return jsonify({"code": 7, "msg": str(e)})

    # 从Redis读取输出消息流
    if err_msg != "":
        redis_queue.delete_queue(combined_topic_id, dag_id=dag.uuid)
        loghandler.logger.error(err_msg)
        return jsonify({"code": 7, "msg": err_msg})
    return Response(utils.generate_stream(dag.uuid, combined_topic_id), mimetype="text/event-stream")


def plugin_run_for_bigmodel(plugin, input_params, plugin_en_name):
    try:
        # 存入数据库的数据为前端格式，需要转换为后端可识别格式
        config = json.loads(plugin.dag_content)
        converted_config = utils.workflow_format_convert(config)
        user_id = auth.get_user_id()
        dag = build_dag_for_aibigmodel(converted_config, {}, "", user_id)
    except Exception as e:
        loghandler.logger.error(f"plugin_run_for_bigmodel failed: {repr(e)}")
        return json.dumps({"code": 7, "msg": repr(e)})

    # 调用运行dag
    start_time = time.time()
    result, nodes_result = dag.run_with_param(input_params, config)
    # 检查是否如期运行
    for node_dict in nodes_result:
        node_status = node_dict['node_status']
        if node_status == WorkflowNodeStatus.FAILED:
            node_message = node_dict['node_message']
            return json.dumps({"code": 7, "msg": node_message})

    end_time = time.time()
    executed_time = round(end_time - start_time, 3)
    # 获取workflow与各节点的执行结果
    execute_status = WorkflowNodeStatus.SUCCESS if all(
        node.get('node_status') in [WorkflowNodeStatus.SUCCESS, WorkflowNodeStatus.RUNNING_SKIP]
        for node in nodes_result) else WorkflowNodeStatus.FAILED
    execute_result = utils.get_workflow_running_result(nodes_result, dag.uuid, execute_status, str(executed_time))
    if not execute_result:
        return json.dumps({"code": 7, "msg": "execute result not exists"})

    # 大模型调用时，不需要增加数据库流水记录
    loghandler.logger.info(f"=== AI request: {plugin_en_name=}, result: {result}, execute_result: {execute_result}")
    return json.dumps(result, ensure_ascii=False)


def plugin_run_for_streaming(plugin, input_params, plugin_en_name):
    try:
        # 存入数据库的数据为前端格式，需要转换为后端可识别格式
        config = json.loads(plugin.dag_content)
        converted_config = utils.workflow_format_convert(config)
        user_id = auth.get_user_id()
        dag = build_dag_for_aibigmodel(converted_config, {}, "", user_id)
    except Exception as e:
        loghandler.logger.error(f"plugin_run_for_streaming failed: {repr(e)}")
        return json.dumps({"code": 7, "msg": repr(e)})

    # 调用运行dag
    err_msg = ""
    try:
        combined_topic_id, nodes_result = dag.run_with_param_for_streaming(input_params, config)
    except Exception as e:
        err_msg = f"dag: {dag.uuid}, Workflow execution failed: {repr(e)}"
        loghandler.logger.error(err_msg)
        return json.dumps({"code": 7, "msg": err_msg})

    err_msg = dag.get_nodes_wrong_status_message(dag.uuid, nodes_result)

    # 从Redis读取输出消息流
    if err_msg != "":
        redis_queue.delete_queue(combined_topic_id, dag_id=dag.uuid)
        loghandler.logger.error(err_msg)
        return jsonify({"code": 7, "msg": err_msg})
    return Response(utils.generate_stream(dag.uuid, combined_topic_id), mimetype="text/event-stream")


def get_workflow_list(cloud_type, keyword=None, status=None, page=1, limit=10, is_stream=0):
    try:
        # 用户查询时，将样例数据与用户数据同时返回
        if cloud_type == SIMPLE_CLOUD:
            user_id = auth.get_user_id()
            org_id = auth.get_org_id()
            if not user_id:
                return jsonify({"code": 7, "msg": "userID is required"})
            if not org_id:
                return jsonify({"code": 7, "msg": "orgID is required"})
            query = db.session.query(_WorkflowTable).filter(
                or_(
                    and_(
                        _WorkflowTable.user_id == user_id,
                        _WorkflowTable.org_id == org_id
                    ),
                    _WorkflowTable.example_flag == utils.WorkflowType.WORKFLOW_EXAMPLE
                )
            )
        elif cloud_type == PRIVATE_CLOUD:
            tenant_ids = auth.get_tenant_ids()
            if len(tenant_ids) == 0:
                return jsonify({"code": 7, "msg": "tenant_ids is required"})
            query = db.session.query(_WorkflowTable).filter(
                (_WorkflowTable.tenant_id.in_(tenant_ids)) |
                (_WorkflowTable.example_flag == utils.WorkflowType.WORKFLOW_EXAMPLE)
            )
        else:
            return jsonify({"code": 7, "msg": "不支持的云类型"})

        if keyword:
            query = query.filter(_WorkflowTable.config_name.contains(keyword) |
                                 _WorkflowTable.config_en_name.contains(keyword))
        if is_stream:
            query = query.filter_by(is_stream=is_stream)
        if status:
            query = query.filter_by(status=status)

        # 获取符合user_id条件的所有记录数
        total = query.count()

        # 先按example_flag排序，再按updated_time排序
        workflows = query.order_by(_WorkflowTable.example_flag.desc(),
                                   _WorkflowTable.updated_time.desc()).paginate(page=int(page), per_page=int(limit))

        workflows_list = [workflow.to_dict() for workflow in workflows]
        data = {"list": workflows_list, "pageNo": int(page), "pageSize": int(limit), "total": total}
        return jsonify({"code": 0, "data": data})
    except SQLAlchemyError as e:
        loghandler.logger.error(f"Error occurred while fetching workflow list: {e}")
        return jsonify({"code": 5000, "msg": "Error occurred while fetching workflow list."})


def get_workflow_list_for_internal(keyword=None, status=None, page=1, limit=10000):
    try:

        query = db.session.query(_WorkflowTable)

        if keyword:
            query = query.filter(_WorkflowTable.config_name.contains(keyword) |
                                 _WorkflowTable.config_en_name.contains(keyword))
        if status:
            query = query.filter_by(status=status)

        # 获取符合user_id条件的所有记录数
        total = query.count()

        # 先按example_flag排序，再按updated_time排序
        workflows = query.order_by(_WorkflowTable.example_flag.desc(),
                                   _WorkflowTable.updated_time.desc()).paginate(page=int(page), per_page=int(limit))

        workflows_list = [workflow.to_dict() for workflow in workflows]
        data = {"list": workflows_list, "pageNo": int(page), "pageSize": int(limit), "total": total}
        return jsonify({"code": 0, "data": data})
    except SQLAlchemyError as e:
        loghandler.logger.error(f"Error occurred while fetching workflow list: {e}")
        return jsonify({"code": 5000, "msg": "Error occurred while fetching workflow list."})


def workflow_clone(workflow_config, user_id, org_id,tenant_ids):
    # 查询相同英文名称的工作流配置，并为新副本生成唯一的名称
    if auth.get_cloud_type() == SIMPLE_CLOUD:
        existing_config_copies = fetch_records_by_filters(_WorkflowTable,
                                                          method='all',
                                                          config_en_name__like=f"{workflow_config.config_en_name}%",
                                                          user_id=user_id,
                                                          org_id=org_id)
    elif auth.get_cloud_type() == PRIVATE_CLOUD:
        existing_config_copies = fetch_records_by_filters(_WorkflowTable,
                                                          method='all',
                                                          config_en_name__like=f"{workflow_config.config_en_name}%",
                                                          tenant_id__in=tenant_ids)
    else:
        return jsonify({"code": 7, "msg": "不支持的云类型"})
    # 找出最大后缀
    name_suffix = utils.add_max_suffix(workflow_config.config_en_name, existing_config_copies)
    # 生成新的配置名称和状态
    new_config_name = f"{workflow_config.config_name}_副本{name_suffix}"
    new_config_en_name = f"{workflow_config.config_en_name}_{name_suffix}"
    # TODO:清空开始节点的系统变量值-目前只有静态Token
    dag_content = workflow_sys_param_reset(workflow_config.dag_content)
    try:
        # 生成新的工作流 ID
        new_workflow_id = uuid.uuid4()
        # 创建新工作流记录
        new_workflow = _WorkflowTable(
            id=str(new_workflow_id),
            user_id=workflow_config.user_id,
            org_id=workflow_config.org_id,
            config_name=new_config_name,
            config_en_name=new_config_en_name,
            config_desc=workflow_config.config_desc,
            dag_content=dag_content,
            status=utils.WorkflowStatus.WORKFLOW_DRAFT,
            is_stream=workflow_config.is_stream,
            updated_time=datetime.now(),
            tenant_id=workflow_config.tenant_id
        )
        db.session.add(new_workflow)
        db.session.commit()
    except SQLAlchemyError as e:
        db.session.rollback()
        return jsonify({"code": 5000, "message": str(e)})
    except Exception as e:
        db.session.rollback()
        loghandler.logger.error(f"workflow_clone failed: {e}")
        return jsonify({"code": 7, "message": str(e)})

    # 返回新创建的工作流信息
    response_data = {
        "code": 0,
        "data": {"workflow_id": new_workflow.id},
        "msg": "Workflow cloned successfully"
    }
    return jsonify(response_data)


def workflow_example_clone(workflow_id,org_id, user_id, config_name, config_en_name, config_desc,
                           tenant_id):
    # 根据样例的workflow_id，查找对应的插件数据并复制
    # 无用户信息，直接检索
    workflow_config = fetch_records_by_filters(_WorkflowTable,
                                               method='first',
                                               id=workflow_id)

    dag_content = workflow_config.dag_content
    # 查询相同英文名称的工作流配置，并为新副本生成唯一的名称
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
                tenant_id=tenant_id
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
    return jsonify({"code": 0, "data": data, "msg": "Workflow file cloned successfully"})


def workflow_save(workflow_id, config_name, config_en_name, config_desc, workflow_dict, user_id,org_id, tenant_ids):
    # 查询条件
    if auth.get_cloud_type() == SIMPLE_CLOUD:
        workflow_results = fetch_records_by_filters(_WorkflowTable,
                                                    id=workflow_id,
                                                    user_id=user_id,
                                                    org_id=org_id)
    elif auth.get_cloud_type() == PRIVATE_CLOUD:
        workflow_results = fetch_records_by_filters(_WorkflowTable,
                                                    id=workflow_id,
                                                    tenant_id__in=tenant_ids)
    else:
        return jsonify({"code": 7, "msg": "不支持的云类型"})

    if not workflow_results:
        return jsonify({"code": 5000, "msg": "Internal Server Error"})

    # 样例数据防御性措施，禁止自动保存更改
    if workflow_results.example_flag == utils.WorkflowType.WORKFLOW_EXAMPLE:
        return jsonify({"code": 0, "data": {"workflowID": str(workflow_id)}, "msg": "样例数据复制后可以进行修改"})

    if auth.get_cloud_type() == SIMPLE_CLOUD:
        # 检查英文名称唯一性
        en_name_check = fetch_records_by_filters(_WorkflowTable,
                                                 config_en_name=config_en_name,
                                                 user_id=user_id,
                                                 org_id=org_id)
    elif auth.get_cloud_type() == PRIVATE_CLOUD:
        en_name_check = fetch_records_by_filters(_WorkflowTable,
                                                 config_en_name=config_en_name,
                                                 tenant_id__in=tenant_ids)
    else:
        return jsonify({"code": 7, "msg": "不支持的云类型"})
    if en_name_check and config_en_name != workflow_results.config_en_name:
        return jsonify({"code": 7, "msg": "该英文名称已存在, 请重新填写"})

    try:
        workflow = json.dumps(workflow_dict)
        # 防御性措施
        if len(workflow_dict['nodes']) == 0 or workflow_dict['nodes'] == [{}]:
            workflow = utils.generate_workflow_schema_template()

        # 更新逻辑
        update_data = {
            _WorkflowTable.config_name: config_name,
            _WorkflowTable.config_en_name: config_en_name,
            _WorkflowTable.config_desc: config_desc,
            _WorkflowTable.dag_content: workflow,
            _WorkflowTable.updated_time: datetime.now(),
            _WorkflowTable.execute_status: ""
        }

        if auth.get_cloud_type() == SIMPLE_CLOUD:
            db.session.query(_WorkflowTable).filter_by(id=workflow_id, user_id=user_id,org_id=org_id).update(update_data)
        else:
            db.session.query(_WorkflowTable).filter(
                _WorkflowTable.id == workflow_id,
                _WorkflowTable.tenant_id.in_(tenant_ids)
            ).update(update_data)

        db.session.commit()
    except SQLAlchemyError as e:
        db.session.rollback()
        return jsonify({"code": 5000, "msg": str(e)})

    return jsonify({"code": 0, "data": {"workflowID": str(workflow_id)}, "msg": "Workflow file saved successfully"})


def get_app_token(app_id, api_key):
    # 查询数据库中的 static_token
    record = fetch_records_by_filters(_AppTokenTable, id=app_id)
    if record and record.static_token:
        return jsonify({"code": 0, "data": {"static_token": record.static_token}})

    # 存入数据库
    static_token, err = save_app_token(app_id, api_key)
    if err:
        loghandler.logger.error(f"Failed to save static_token, err:{err}")
        return jsonify({"code": 7, "msg": "服务内部错误，请联系管理员"})
    return jsonify({"code": 0, "data": {"static_token": static_token}})


# 重置系统参数
def workflow_sys_param_reset(dag_content: str) -> str:
    # 将输入的 JSON 字符串解析为字典
    try:
        dag_dict = json.loads(dag_content)
    except json.JSONDecodeError as e:
        raise ValueError("输入的 JSON 字符串格式无效") from e

    # 标记是否找到 StartNode
    found_start_node = False

    # 遍历 nodes 列表，找到 type 为 "StartNode" 的节点
    for node in dag_dict["nodes"]:
        if node["type"] == "StartNode":
            found_start_node = True
            # 确保 settings 字典存在
            if "settings" not in node["data"]:
                node["data"]["settings"] = {}
            # 将 staticAuthToken 置为空
            node["data"]["settings"]["staticAuthToken"] = ""
            break

    # 如果未找到 StartNode，抛出异常
    if not found_start_node:
        raise ValueError("未找到 type 为 'StartNode' 的节点")

    # 将处理后的字典压缩为 JSON 字符串并返回
    return json.dumps(dag_dict, separators=(",", ":"))


def workflow_delete(workflow_id: str, query_filter: list, token: str) -> Response:
    # 向KONG解除插件的注册
    # response = unregister_openapi_plugins(workflow_id, token)
    # if response.status == service_status.ServiceExecStatus.ERROR:
    #     return jsonify({"code": 7, "msg": f"插件删除失败: 网关解绑插件API服务失败"})

    # 删除 WorkflowTable 中的数据
    db.session.query(_WorkflowTable).filter(*query_filter).delete()
    # 删除 PluginTable 中与 Workflow 关联的数据
    db.session.query(_PluginTable).filter_by(id=workflow_id).delete()
    db.session.commit()


# 向Kong服务注册发布的用户自定义插件服务
def register_openapi_plugins(workflow_id: str, data: dict, token: str):
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
    headers = {
        "Authorization": token
    }

    # 插件Path
    path = f"/run_for_user_defined/{user_id}/{org_id}/{dag_en_name}"

    response = api_request(
        url=f"{KONG_SERVICE}/{workflow_id}",
        method="PUT",
        json={
            "protocol": "http",
            "host": AGENTSCOPE_HOST,
            "port": 6672,
            "path": path
        },
        headers=headers
    )

    loghandler.logger.info(f"Response Content: {response.content} Response Status: {response.status}")

    return response


# 向Kong服务注销发布的用户自定义插件服务
def unregister_openapi_plugins(workflow_id: str, token: str):
    headers = {
        "Authorization": token
    }

    response = api_request(
        url=f"{KONG_SERVICE}/{workflow_id}",
        method="DELETE",
        headers=headers
    )

    loghandler.logger.info(f"Response Content: {response.content} Response Status: {response.status}")

    return response
