# wanwu-agentscope
本项目是基于[AgentScope](https://github.com/modelscope/agentscope)项目进行的二次开发，遵循其Apache 2.0 协议许可证。主要修改其工作流部分的代码，作为联通元景万悟Lite的工作流模块。

# 快速开始
要求 Python 3.9 或更高版本

## 从源码安装

```bash
# 从 GitHub 拉取源代码
git clone https://github.com/UnicomAI/wanwu-agentscope.git

# 以编辑模式安装
cd wanwu-agentscope
pip install -e .
pip install pypinyin pymysql redis objgraph pympler mcp
python3 ./src/agentscope/aibigmodel_workflow/app.py
```
