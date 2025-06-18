
from sqlalchemy import text, TextClause
from agentscope.aibigmodel_workflow.config import db
from agentscope.aibigmodel_workflow.config import app


def get_private_cloud_tenant_id(workflow_id: str, plugin_id: str) -> str:
    if workflow_id == '' and plugin_id == '':
        return ''

    if workflow_id != '':
        sql = text('select tenant_id from llm_workflow_info where id = :id')
        return get_tenant_id(sql, workflow_id)

    if plugin_id != '':
        sql = text('select tenant_id from llm_plugin_info where id = :id')
        return get_tenant_id(sql, plugin_id)


def get_tenant_id(sql: TextClause, id: str) -> str:
    with app.app_context():
        # 获取数据库会话
        session = db.session

        # 获取查询结果的第一行
        result = session.execute(sql, {'id': id})
        row = result.fetchone()
        if row is None:
            return ''
        return row[0]


if __name__ == '__main__':
    # 单测
    result = get_private_cloud_tenant_id('4909b071-a4db-4692-a771-75a950624cab', '')
    print(f'tenant id result is {result}')
