# -*- coding: utf-8 -*-
"""Search question in the web"""
import time
from typing import Any, Dict, Optional, Union, Tuple
import requests
import json as j

from agentscope.service.service_response import ServiceResponse
from agentscope.service.service_status import ServiceExecStatus
from loguru import logger
from collections.abc import Iterator


def api_request(
        method: str,
        url: str,
        auth: Optional[str] = None,
        api_key: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
        retry_delay: int = 3,
        timeout: Union[float, Tuple[float, float]] = (5, 600), # Connect timeout 5s, read timeout 10min
        **kwargs: Any,
) -> ServiceResponse:
    """
    发送 API 请求并返回响应，支持重试。

    Args：
    method（`str`）：要使用的HTTP方法（例如，'GET'、'POST'）。
    url（`str`）：请求的url。
    auth（`Optional[str]`）：要在授权标头中使用的API密钥或令牌。
    api_key（`Optional[str]`）：要在参数中使用的api键。
    params（`Optional[Dict[str，Any]]`）：查询请求的参数。
    data（`Optional[Dict[str，Any]]`）：在请求正文中发送表单数据。
    json（`Optional[Dict[str，Any]]`）：在请求正文中发送的json数据。
    headers（`Optional[Dict[str，Any]]`）：请求中要包含的标头。
    max_retries（`int`）：最大重试次数。
    retry_delay（`int`）：重试之间的延迟（秒）。
    timeout（`Union[float, Tuple[float, float]]`）：请求超时设置，可以是一个浮点数（总超时）或元组（连接超时，读取超时）。
    **kwargs（`Any`）：传递给请求的其他关键字参数。

    返回：
    `ServiceResponse：具有"status"和"content"的响应对象。
    """
    if headers is None:
        headers = {}
    if auth:
        headers['Authorization'] = auth

    if params is None:
        params = {}
    if api_key:
        params['key'] = api_key

    if json and len(json) == 0:
        json = None

    last_exception = None

    for attempt in range(max_retries):
        try:
            with requests.request(
                    method=method,
                    url=url,
                    params=params,
                    data=data,
                    json=json,
                    headers=headers,
                    timeout=timeout,  # 使用传入的超时设置
                    verify=True,
                    **kwargs,
            ) as resp:
                resp.raise_for_status()  # Raise an error for bad responses

                try:
                    # 尝试将响应解析为 JSON
                    content = resp.json()
                except ValueError as e:
                    # 如果响应不是 JSON，返回原始文本
                    logger.error(f"Failed to parse JSON response: {str(e)}")
                    return ServiceResponse(ServiceExecStatus.ERROR, f"Invalid response format: {str(e)}")

                return ServiceResponse(ServiceExecStatus.SUCCESS, content)

        except requests.RequestException as e:
            last_exception = e
            if attempt < max_retries - 1:  # 如果不是最后一次重试，等待一段时间
                time.sleep(retry_delay)
            continue

    return ServiceResponse(ServiceExecStatus.ERROR, str(last_exception))


def api_request_for_big_model(
        method: str,
        url: str,
        auth: Optional[str] = None,
        api_key: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
) -> ServiceResponse:
    """
    发送API请求并返回响应。

    Args：
    method（`str`）：要使用的HTTP方法（例如，'GET'、'POST'）。
    url（`str`）：请求的url。
    auth（`Optional[str]`）：要在授权标头中使用的API密钥或令牌。
    api_key（`Optional[str]`）：要在参数中使用的api键。
    params（`Optional[Dict[str，Any]]`）：查询请求的参数。
    data（`Optional[Dict[str，Any]]`）：在请求正文中发送表单数据。
    json（`Optional[Dict[str，Any]]`）：在请求正文中发送的json数据。
    headers（`Optional[Dict[str，Any]]`）：请求中要包含的标头。
    **kwargs（`Any`）：传递给请求的其他关键字参数。

    返回：
    `ServiceResponse：具有“status”和“content”的响应对象。
    """
    if headers is None:
        headers = {}
    if auth:
        headers['Authorization'] = auth

    if params is None:
        params = {}
    if api_key:
        params['key'] = api_key

    if json and len(json) == 0:
        json = None

    try:
        with requests.request(
                method=method,
                url=url,
                params=params,
                data=j.dumps(data),
                json=json,
                headers=headers,
                timeout=600,
                verify=True,
                stream=False,
                **kwargs,
        ) as resp:
            resp.raise_for_status()  # Raise an error for bad responses

        try:
            # 尝试将响应解析为 JSON
            content = resp.json()
        except ValueError as e:
            # 如果响应不是 JSON，返回原始文本
            logger.error(f"Failed to parse JSON response: {str(e)}")
            return ServiceResponse(ServiceExecStatus.ERROR, f"Invalid response format: {str(e)}")

        return ServiceResponse(ServiceExecStatus.SUCCESS, content)

    except requests.RequestException as e:
        logger.error(f"Request failed: {str(e)}")
        return ServiceResponse(ServiceExecStatus.ERROR, str(e))


def api_request_for_big_model_stream(
        method: str,
        url: str,
        auth: Optional[str] = None,
        api_key: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,  # 避免与内置json参数冲突
        headers: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
) -> Iterator[str]:
    if headers is None:
        headers = {}
    if auth:
        headers['Authorization'] = auth
    headers['Accept'] = 'text/event-stream'

    params = params or {}
    if api_key:
        params['key'] = api_key

    # 处理请求体数据
    json_payload = json_data if json_data is not None else None
    data_payload = j.dumps(data) if data else None

    resp = requests.request(
        method=method,
        url=url,
        params=params,
        data=data_payload,
        json=json_payload,
        headers=headers,
        timeout=600,  # 更合理的超时设置
        stream=True,
        verify=False,
        **kwargs,
    )

    # 检查HTTP状态码
    if resp.status_code != 200:
        error_msg = {"error": True, "status_code": resp.status_code, "message": f"HTTP错误: {resp.status_code}"}
        try:
            # 尝试解析响应体获取更详细的错误信息
            error_content = resp.json()
            error_msg["details"] = error_content
        except:
            # 如果无法解析JSON，使用文本内容
            error_msg["details"] = resp.text[:500]  # 限制长度防止过大

        yield error_msg
        return

    try:
        resp.raise_for_status()

        # 验证Content-Type
        if 'text/event-stream' not in resp.headers.get('Content-Type', ''):
            yield "Non-SSE response received"
            return

        buffer = ""
        for chunk in resp.iter_content(chunk_size=8192):
            if not chunk:
                break
            buffer += chunk.decode('utf-8', errors='replace')
            while '\n\n' in buffer:
                event, buffer = buffer.split('\n\n', 1)
                event_data = ""
                for line in event.split('\n'):
                    if line.startswith('data:'):
                        event_data += line[5:].strip() + '\n'
                    # if event_data:
                    #     yield ServiceResponse(ServiceExecStatus.SUCCESS, event_data.strip())
                    yield event_data.strip()
    except Exception as e:
        # yield ServiceResponse(ServiceExecStatus.ERROR, str(e))
        yield str(e)
    finally:
        if 'resp' in locals():
            resp.close()


# 测试请求，示例：随机土味情话
if __name__ == '__main__':

    body = {
        "model": "deepseek-r1-distill-llama-8b",
        "stream": True,
        "messages": [
            {
                "role": "user",
                "content": "9.9和9.11谁大"
            }
        ]
    }

    header = {"Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
                               ".eyJpZCI6IjczMWE1ZmVlLTBhYjctNDQzMS1iMGQzLWY2ODA3ZmJhNWFlNSIsInRlbmFudElEcyI6bnVsbCwidXNlclR5cGUiOjAsInVzZXJuYW1lIjoiYWRtaW4iLCJuaWNrbmFtZSI6ImFkbWluIiwiYnVmZmVyVGltZSI6MTc0MTk0OTYxNywiZXhwIjoyMzcyNjYyNDE3LCJqdGkiOiI1ZjFjZTkxZGI0MjU0YWI4OGE3ZDA5ZjExYjliNjU5OSIsImlzcyI6IjczMWE1ZmVlLTBhYjctNDQzMS1iMGQzLWY2ODA3ZmJhNWFlNSIsIm5iZiI6MTc0MTk0MjExNywic3ViIjoia29uZyJ9.4wBWTCs5x3TsZTkiKMzMP02gDaj71OeihNmI1k5Pz-A"}

    for response in api_request_for_big_model_stream('POST',
                                                     'http://llmproxy.maas-prod:8000/llmproxy/api/v1/compatible-mode/chat/completions'
            , data=body):
        print(response.content)  # 输出如：data: {"time": "2023-10-01T12:34:56"}

    # 通用 API 调用示例
    # response = api_request(
    #     method="GET",
    #     url="https://api.uomg.com/api/rand.qinghua?format=json",
    # )
    # 通用 API 调用示例
    # response2 = api_request(
    #     method="GET",
    #     url="https://restapi.amap.com/v5/place/text",
    #     params={"keywords": "雍和宫"},  # 查询参数
    #     api_key="77b5f0d102c848d443b791fd469b732d",  # 作为查询参数传递 API 密钥
    # )
    # print(response)
    # print(response2)
