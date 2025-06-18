import json
import traceback
from agentscope.web.workstation.workflow_dag import build_dag_for_aibigmodel
from agentscope.studio._app import _remove_file_paths
from agentscope.aibigmodel_workflow.utils import workflow_format_convert
from typing import Tuple


def convert_to_py(content: str, **kwargs) -> Tuple:
    """
    Convert json config to python code.
    """
    try:
        cfg = json.loads(content)
        print(cfg)
        return "True", build_dag_for_aibigmodel(cfg, {}).compile(**kwargs)
    except Exception as e:
        return "False", _remove_file_paths(
            f"Error: {e}\n\n" f"Traceback:\n" f"{traceback.format_exc()}",
        )


def test_convert_to_py():
    # 测试正确的输入
    content = ("{'2': {'data': {'args': {'api_key': 'qweqweqweqw', 'config_name': 'qwen', 'messages_key': 'input', "
               "'model_name': 'qwen-max', 'model_type': 'dashscope_chat', 'seed': 0, 'temperature': '0.1'}}, "
               "'inputs': {}, 'name': 'dashscope_chat', 'outputs': {}}, '3': {'data': {'args': {'name': 'User'}}, "
               "'inputs': {'input_1': {'connections': []}}, 'name': 'UserAgent', "
               "'outputs': {'output_1': {'connections': [{'node': '4', 'output': 'input_1'}]}}}, "
               "'4': {'data': {'args': {'model_config_name': 'qwen', 'name': 'Assistant', "
               "'sys_prompt': 'You are an assistant'}}, 'inputs': {'input_1': "
               "{'connections': [{'input': 'output_1', 'node': '3'}]}}, 'name': 'DialogAgent', "
               "'outputs': {'output_1': {'connections': []}}}}")

    result = convert_to_py(content, runtime_id=12312312, studio_url="test")
    print(result)
    assert result[0] == "True"
    assert "dashscope_chat" in result[1]
    assert "api_key" in result[1]

    # 测试错误的输入
    content = '{"invalid": "json"}'
    result = convert_to_py(content)
    assert result[0] == "False"
    assert "Error: " in result[1]
    assert "Traceback" in result[1]


def test_json_load():
    import json

    data = {
        "1": {
            "data": {
                "args": {
                    "inputs": [],
                    "outputs": [
                        {
                            "name": "poi",
                            "type": "string",
                            "value": "beijing"
                        },
                        {
                            "name": "keywords",
                            "type": "string",
                            "value": "lifadian"
                        }
                    ],
                    "settings": {}
                }
            },
            "inputs": {
                "input_1": {
                    "connections": []
                }
            },
            "name": "StartNode",
            "outputs": {
                "output_1": {
                    "connections": [
                        {
                            "node": "2",
                            "output": "input_1"
                        }
                    ]
                }
            }
        },
        "2": {
            "data": {
                "args": {
                    "inputs": [
                        {
                            "name": "poi",
                            "value": [],
                        },
                        {
                            "name": "keywords",
                            "value": [],
                        },
                    ],
                    "outputs": [],
                    "settings": {}
                }
            },
            "inputs": {
                "input_1": {
                    "connections": [
                        {
                            "input": "output_1",
                            "node": "1"
                        }
                    ]
                }
            },
            "name": "EndNode",
            "outputs": {
                "output_1": {
                    "connections": []
                }
            }
        }
    }

    content = json.dumps(data)

    print(content)
    result = convert_to_py(content, runtime_id=12312312, studio_url="test")
    print(result)
    assert result[0] == "True"
    assert "a" in result[1]


def test_json_load_file():
    with open('../test.json', 'r') as file:
        data = json.load(file)

    content = json.dumps(data)

    print(content)
    result = convert_to_py(content, runtime_id=12312312, studio_url="test")
    print(result)
    assert result[0] == "True"
    assert "a" in result[1]


def test_workflow_run():
    from agentscope.web.workstation.workflow import (
        load_config,
    )
    script_path = "./test.json"
    config_frontend = load_config(script_path)
    config_backend = workflow_format_convert(config_frontend)
    dict1 = {'poi': [{"location": "beijing"}]}
    dag = build_dag_for_aibigmodel(config_backend, {})
    dag.run_with_param(dict1, config_frontend)
    # hack, 释放变量池里的内存空间
    if len(dag.params_pool) > 0:
        del dag.params_pool


def test_parse_json_to_dict():
    from agentscope.web.workstation.workflow_node import parse_json_to_dict

    input = '''
{
    "outputs": [
        {
            "name": "poi",
            "type": "string",
            "desc": "\u5174\u8da3\u70b9\u540d\u79f0\uff0c\u4f8b\u5982\u57ce\u5e02\u3001\u53bf\u57ce\u7b49",
            "object_schema": null,
            "list_schema": null,
            "value": {
                "type": "generated",
                "content": null
            }
        },
        {
            "name": "keywords",
            "type": "string",
            "desc": "\u5174\u8da3\u70b9\u5468\u8fb9\u76f8\u5173\u7684\u5173\u952e\u8bcd\uff0c\u4f8b\u5982\u5496\u5561\u9986\u3001\u8425\u4e1a\u5385\u7b49",
            "object_schema": null,
            "list_schema": null,
            "value": {
                "type": "generated",
                "content": null
            }
        }
    ]
}
    '''
    output = parse_json_to_dict(input)
    print(type(output))
    print(output['outputs'][0]['name'])
    print(output['outputs'][0]['object_schema'])
    print(output['outputs'][0]['value'])


def test_parse_pythoncode_to_code():
    from agentscope.service.execute_code.exec_python import execute_python_code
    from agentscope.web.workstation.workflow_node import parse_json_to_dict

    input = "{\"settings\":{\"code\":\"# \\u5b9a\\u4e49\\u4e00\\u4e2a main \\u51fd\\u6570\\uff0c\\u4f20\\u5165 params \\u53c2\\u6570\\u3002params \\u4e2d\\u5305\\u542b\\u4e86\\u8282\\u70b9\\u914d\\u7f6e\\u7684\\u8f93\\u5165\\u53d8\\u91cf\\u3002\\n# \\u9700\\u8981\\u5b9a\\u4e49\\u4e00\\u4e2a\\u5b57\\u5178\\u4f5c\\u4e3a\\u8f93\\u51fa\\u53d8\\u91cf\\n# \\u5f15\\u7528\\u8282\\u70b9\\u5b9a\\u4e49\\u7684\\u53d8\\u91cf\\uff1aparams['\\u53d8\\u91cf\\u540d']\\n# \\u8fd0\\u884c\\u73af\\u5883 Python3\\uff1b\\u9884\\u7f6e Package\\uff1aNumPy\\n\\ndef main(params):\\n\\n    # \\u521b\\u5efa\\u4e00\\u4e2a\\u5b57\\u5178\\u4f5c\\u4e3a\\u8f93\\u51fa\\u53d8\\u91cf\\n    output_object ={\\n    \\n        # \\u5f15\\u7528\\u8282\\u70b9\\u5b9a\\u4e49\\u7684 city \\u53d8\\u91cf\\n        \\\"key0\\\": params['pois'],\\n\\n    }\\n    return   output_object\\n        \\n\",\"language\":\"Python\"}}"
    print(input)
    output = parse_json_to_dict(input)
    code = output['settings']['code']
    response = execute_python_code(code, use_docker=False)
    print(response)


if __name__ == "__main__":
    # 运行测试
    test_workflow_run()

    # 解析json string
    # test_parse_pythoncode_to_code()
