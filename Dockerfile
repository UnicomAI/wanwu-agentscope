FROM registry.ai-yuanjing.cn/maas/agentscope:v12

# 强制不使用缓存（需配合 --build-arg 使用）
ARG CACHEBUST=$(date +%s)

# 复制代码到 /opt/app/agentscope
RUN mkdir -p /opt/app/agentscope
RUN rm -rf /opt/app/agentscope/*
COPY . /opt/app/agentscope/

# 设置工作目录
WORKDIR /opt/app/agentscope

# 关键路径设置（包含项目根目录和 src 目录）
ENV PYTHONPATH=/opt/app/agentscope:/opt/app/agentscope/src

# 设置权限（仅对必要文件设置 755）
RUN chmod 755 /opt/app/agentscope/src/agentscope/aibigmodel_workflow/app.py

# 使用 WORKDIR 的相对路径（或绝对路径 /opt/app/agentscope/...）
ENTRYPOINT ["python3", "./src/agentscope/aibigmodel_workflow/app.py"]