import json
import pytest
from unittest.mock import Mock
from agentscope.aibigmodel_workflow.service import plugin_run_for_bigmodel, WorkflowNodeStatus  # 替换为实际模块路径


class TestPluginRunForBigModel:
    @pytest.fixture
    def mock_plugin(self):
        plugin = Mock()
        plugin.dag_content = json.dumps({
            "nodes": [
                {
                    "id": "3ee3b636-7e2d-406a-9e9c-80598bb24913",
                    "name": "开始",
                    "type": "StartNode",
                    "data": {
                        "outputs": [
                            {
                                "desc": "TEST",
                                "list_schema": "",
                                "name": "input",
                                "object_schema": "",
                                "required": "false",
                                "type": "string",
                                "value": {
                                    "content": "",
                                    "type": "generated"
                                }
                            }
                        ],
                        "inputs": [],
                        "settings": {}
                    }
                },
                {
                    "id": "8b67fb4c-8966-4798-96df-a8755edd7509",
                    "name": "结束",
                    "type": "EndNode",
                    "data": {
                        "outputs": [],
                        "inputs": [
                            {
                                "desc": "",
                                "list_schema": "",
                                "name": "output",
                                "object_schema": "",
                                "required": "false",
                                "type": "string",
                                "value": {
                                    "content": {
                                        "ref_node_id": "3ee3b636-7e2d-406a-9e9c-80598bb24913",
                                        "ref_var_name": "input"
                                    },
                                    "type": "ref"
                                },
                                "newRefContent": "开始/input"
                            }
                        ],
                        "settings": {}
                    }
                }
            ],
            "edges": [
                {
                    "source_node_id": "3ee3b636-7e2d-406a-9e9c-80598bb24913",
                    "source_port": "3ee3b636-7e2d-406a-9e9c-80598bb24913-right",
                    "target_node_id": "8b67fb4c-8966-4798-96df-a8755edd7509",
                    "target_port": "8b67fb4c-8966-4798-96df-a8755edd7509-left"
                }
            ]
        })
        return plugin

    @pytest.fixture
    def mock_dag(self):
        # 创建一个模拟的 DAG 对象
        dag = Mock()

        # 设置 run_with_param 方法的返回值
        dag.run_with_param.return_value = (
            {"result": "data"},  # 模拟的 DAG 运行结果
            [{"node_status": WorkflowNodeStatus.SUCCESS}]  # 模拟的节点状态
        )

        return dag

    def test_successful_execution(self, mock_plugin):
        """测试完整成功流程"""
        # 准备有效输入参数
        input_params = {
            "input": "success_response"
        }

        print(mock_plugin)
        print(type(mock_plugin.dag_content))

        # 执行测试
        response = plugin_run_for_bigmodel(mock_plugin, input_params, "test_plugin")
        response_data = json.loads(response)

        # 验证结果
        assert "output" in response_data
        assert response_data["output"] == "success_response"

    def test_dag_build_failure_invalid_json(self, mocker, mock_plugin):
        # Force JSON parse error
        mock_plugin.dag_content = "invalid_json"
        mocker.patch("loguru.logger.error")

        response = plugin_run_for_bigmodel(mock_plugin, {}, "test_plugin")
        response_data = json.loads(response)

        assert response_data["code"] == 7
        assert "JSONDecodeError" in response_data["msg"]

    def test_node_execution_failure(self, mock_plugin, mock_dag):
        # Mock failed node
        failed_node = {
            "node_status": WorkflowNodeStatus.FAILED,
            "node_message": "Node processing error"
        }
        mock_dag.run_with_param.return_value = (None, [failed_node])

        # Execute
        response = plugin_run_for_bigmodel(mock_plugin, {}, "test_plugin")
        response_data = json.loads(response)

        assert response_data["code"] == 7
        assert response_data["msg"] == "Exception('input param dict kwargs empty')"

    def test_empty_execute_result(self, mocker, mock_plugin, mock_dag):
        # 准备有效输入参数
        input_params = {
            "input": "success_response"
        }
        # Mock successful nodes but empty result
        mock_dag.run_with_param.return_value = ({"result": "data"}, [{"node_status": WorkflowNodeStatus.SUCCESS}])
        mocker.patch("agentscope.aibigmodel_workflow.utils.get_workflow_running_result", return_value=None)

        response = plugin_run_for_bigmodel(mock_plugin, input_params, "test_plugin")
        response_data = json.loads(response)

        assert response_data["code"] == 7
        assert response_data["msg"] == 'execute result not exists'

    # def test_mixed_node_statuses(self, mocker, mock_plugin, mock_dag):
    #     # 准备有效输入参数
    #     input_params = {
    #         "input": ""
    #     }
    #     # Mock partially successful nodes
    #     nodes_result = [
    #         {"node_status": WorkflowNodeStatus.SUCCESS},
    #         {"node_status": WorkflowNodeStatus.FAILED, "node_message": "Critical failure"}
    #     ]
    #     mock_dag.run_with_param.return_value = (None, nodes_result)
    #     mocker.patch("agentscope.aibigmodel_workflow.utils.get_workflow_running_result", return_value=None)
    #
    #     response = plugin_run_for_bigmodel(mock_plugin, input_params, "test_plugin")
    #     response_data = json.loads(response)
    #
    #     print(response_data)
    #     assert response_data["code"] == 7
    #     assert response_data["msg"] == "Critical failure"
