#!/bin/bash
rm -rf /ainfs_huhe_dev/agentscope_code/agentscope/src/agentscope/aibigmodel_workflow/config.yaml
cp -r /ainfs_huhe_dev/agentscope_code/config.yaml /ainfs_huhe_dev/agentscope_code/agentscope/src/agentscope/aibigmodel_workflow/
container_id=$(docker ps -a --filter "name=agentscope_workflow" -q)

if [ -z "$container_id" ]; then
    echo "Container not found."
else
    docker stop $container_id
    docker rm $container_id
    echo "Container stopped and removed."
fi

docker run -tid --restart=always  -p 6672:6672 --net=kong-net --name agentscope_workflow -v /ainfs_huhe_dev/agentscope_code:/agentscope --init agentscope_workflow:v2 python3 /agentscope/agentscope/src/agentscope/aibigmodel_workflow/app.py

sleep 2
container_id=$(docker ps -a --filter "name=agentscope_workflow" -q)
docker logs $container_id