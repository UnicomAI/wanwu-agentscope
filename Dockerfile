FROM python:3.12-slim
WORKDIR /agentscope

COPY . /agentscope
RUN pip install --upgrade pip && \
    pip install -e . && \
    pip install pypinyin pymysql redis objgraph pympler mcp

CMD ["python3", "/agentscope/src/agentscope/aibigmodel_workflow/app.py"]