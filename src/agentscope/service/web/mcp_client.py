import json
import asyncio
import re
import sys
from typing import Optional
from contextlib import AsyncExitStack

from mcp import ClientSession, StdioServerParameters
from mcp.types import Tool, TextContent, ImageContent, EmbeddedResource
from mcp.client.sse import sse_client

from loguru import logger


def tool_to_dict(tool: Tool) -> dict:
    """将工具对象转换为标准JSON结构

    Args:
        tool: 工具对象，包含name、description和inputSchema属性

    Returns:
        符合MCP工具定义标准的JSON字典
    """
    # 基础工具结构
    tool_json = {
        "name": tool.name,  # 工具唯一标识
        "description": getattr(tool, "description", ""),  # 可选描述
        "inputSchema": tool.inputSchema,
    }
    return tool_json


def content_to_dict(result_content: TextContent | ImageContent | EmbeddedResource) -> dict:
    """将工具运行结果为标准JSON结构

    Args:
        result_content: 工具返回结果，包含type、text属性

    Returns:
        符合MCP工具定义标准的JSON字典
    """
    if result_content.type != "text":
        return {}
    content_dict = {
        "type": result_content.type,
        "text": result_content.text,
    }
    return content_dict


class MCPClient:
    def __init__(self):
        self._exit_stack: Optional[AsyncExitStack] = None
        self.session: Optional[ClientSession] = None
        self._lock = asyncio.Lock()  # 防止并发连接/断开问题
        self.is_connected = False

    async def connect_server(self, mcp_server_url):
        async with self._lock:  # 防止并发调用 connect
            url = mcp_server_url
            # print(f"尝试连接到: {url}")
            self._exit_stack = AsyncExitStack()
            # 1. 进入 SSE 上下文，但不退出
            sse_cm = sse_client(url)
            # 手动调用 __aenter__ 获取流，并存储上下文管理器以便后续退出
            streams = await self._exit_stack.enter_async_context(sse_cm)
            # print("SSE 流已获取。")

            # 2. 进入 Session 上下文，但不退出
            session_cm = ClientSession(streams[0], streams[1])
            # 手动调用 __aenter__ 获取 session
            self.session = await self._exit_stack.enter_async_context(session_cm)
            # print("ClientSession 已创建。")

            # 3. 初始化 Session
            await self.session.initialize()
            # print("Session 已初始化。")

        # 列出可用工具
        response = await self.session.list_tools()
        tools = response.tools
        tools_list = [tool_to_dict(tool) for tool in tools]
        # print(tools_list)
        return tools_list

    async def disconnect(self):
        """关闭 Session 和连接。"""
        async with self._lock:
            await self._exit_stack.aclose()

    # 指定特定的tool并发出tool_call请求
    async def execute_tool(self, tool_name: str, arguments: dict) -> dict:
        # 获取tool_list进行校验
        response = await self.session.list_tools()
        tools = response.tools

        if any(tool.name == tool_name for tool in tools):
            try:
                result = await self.session.call_tool(
                    tool_name, arguments
                )
                content_dict = [content_to_dict(result_content) for result_content in result.content]
                result_dict = {
                    "result": {
                        "isError": result.isError,  # 工具唯一标识
                        "content": content_dict,
                    }
                }
                return result_dict

            except Exception as e:
                logger.error(f"Error executing tool: {str(e)}")
                result_dict = {
                    "result": {
                        "isError": True,  # 工具唯一标识
                        "content": [
                            {
                                "type": "text",
                                "text": str(e),
                            }
                        ]
                    }
                }
                return result_dict


# async def main():
#     try:
#         url = ""
#         client = MCPClient()
#         await client.connect_server(url)
#         result = await client.execute_tool('maps_text_search', {"city": "北京", "keywords": "理发店"})
#         print(result)
#     except Exception as e:
#         print(f"主程序发生错误: {type(e).__name__}: {e}")
#     finally:
#         # 无论如何，最后都要尝试断开连接并清理资源
#         print("\n正在关闭客户端...")
#         await client.disconnect()
#         print("客户端已关闭。")
#
#
# if __name__ == '__main__':
#     asyncio.run(main())