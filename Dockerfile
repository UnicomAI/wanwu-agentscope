FROM python:3.12-slim

# 设置国内加速源（阿里云）
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/ && \
    pip config set install.trusted-host mirrors.aliyun.com

WORKDIR /agentscope
COPY . .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -e . && \
    pip install --no-cache-dir pypinyin pymysql redis objgraph pympler mcp

CMD ["python3", "/agentscope/src/agentscope/aibigmodel_workflow/app.py"]