from agentscope.aibigmodel_workflow.config import db
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mysql import LONGTEXT


class _ExecuteTable(db.Model):  # type: ignore[name-defined]
    """Execute workflow."""
    __tablename__ = "llm_execute_info"
    execute_id = db.Column(db.String(100), primary_key=True)  # 运行ID
    execute_result = db.Column(LONGTEXT)
    user_id = db.Column(db.String(100))  # 用户ID
    org_id = db.Column(db.String(100))  # 用户的组织ID
    executed_time = db.Column(db.DateTime)
    workflow_id = db.Column(db.String(100))  # workflowID
    tenant_id = db.Column(db.String(100))  # TenantID


class _WorkflowTable(db.Model):  # type: ignore[name-defined]
    """Workflow store table."""
    __tablename__ = "llm_workflow_info"
    id = db.Column(db.String(100), primary_key=True)  # workflowID
    user_id = db.Column(db.String(100))  # 用户ID
    org_id = db.Column(db.String(100))  # 用户的组织ID
    config_name = db.Column(db.String(100))
    config_en_name = db.Column(db.String(100))
    config_desc = db.Column(db.Text)
    dag_content = db.Column(db.Text(length=2 ** 32 - 1), default='{}')
    status = db.Column(db.String(10))
    updated_time = db.Column(db.DateTime)
    execute_status = db.Column(db.String(10))
    tenant_id = db.Column(db.String(100))  # TenantID
    example_flag = db.Column(db.Integer, default=0)  # 样例ID，0表示客户创建，1表示预置样例
    is_stream = db.Column(db.Integer, default=0)  # 是否是流式插件，0表示普通非流式，1表示流式插件

    def to_dict(self):
        return {
            'id': self.id,
            'userID': self.user_id,
            'orgID': self.org_id,
            'configName': self.config_name,
            'configENName': self.config_en_name,
            'configDesc': self.config_desc,
            'status': self.status,
            'example_flag': self.example_flag,
            'updatedTime': self.updated_time.strftime('%Y-%m-%d %H:%M:%S'),
            "is_stream": self.is_stream
        }


class _PluginTable(db.Model):  # type: ignore[name-defined]
    """Plugin table."""
    __tablename__ = "llm_plugin_info"
    id = db.Column(db.String(100), primary_key=True)  # ID
    user_id = db.Column(db.String(100))  # 用户ID
    org_id = db.Column(db.String(100))  # 用户的组织ID
    plugin_name = db.Column(db.String(100))  # 插件名称
    plugin_en_name = db.Column(db.String(100))  # 插件英文名称
    plugin_desc = db.Column(db.Text)  # 插件描述
    dag_content = db.Column(db.Text(length=2 ** 32 - 1))  # 插件dag配置文件
    plugin_field = db.Column(db.String(100))  # 插件领域
    plugin_desc_config = db.Column(db.Text)  # 插件描述配置文件
    published_time = db.Column(db.DateTime)  # 插件发布时间
    tenant_id = db.Column(db.String(100))  # TenantID
    contains_gui_agent = db.Column(db.Integer, default=0)  # 是否为GUI插件，0表示普通场景，1表示包含GUI Agent场景
    is_stream = db.Column(db.Integer, default=0)  # 是否是流式插件，0表示普通非流式，1表示流式插件


def fetch_records_by_filters(table, columns=None, method='first', **kwargs):
    """
    Get workflow by filters.

    Args:
        table: The database table to select
        columns (list): The columns to select.
        method (str): The method to use for the query ('all' or 'first').
        **kwargs: Keyword arguments to filter the query.

    Returns:
        list or _PluginTable: The plugins that match the filters or None if no match is found.
    """
    query = table.query
    if columns:
        query = query.with_entities(*columns)
    # 构建过滤条件
    filters = []
    for key, value in kwargs.items():
        if key.endswith('__in'):
            filters.append(getattr(table, key[:-4]).in_(value))
        elif key.endswith('__like'):
            filters.append(getattr(table, key[:-6]).like(value))
        else:
            filters.append(getattr(table, key) == value)

    # 应用过滤条件
    query = query.filter(*filters)

    if method == 'all':
        return query.all()
    elif method == 'first':
        return query.first()
    else:
        raise ValueError(f"Invalid method '{method}'. Use 'all' or 'first'.")


class _AppTokenTable(db.Model):  # type: ignore[name-defined]
    """AppToken table."""
    __tablename__ = "llm_app_token"
    id = db.Column(db.String(100), primary_key=True)  # AppID
    static_token = db.Column(db.String(1000))    # 静态token


def save_app_token(app_id, api_key):
    """插入新的 ID 和 static_token"""
    new_record = _AppTokenTable(id=app_id, static_token=api_key)
    try:
        db.session.add(new_record)
        db.session.commit()
        return api_key, None  # 成功时返回 None
    except SQLAlchemyError as e:
        db.session.rollback()
        return '', f"数据库错误: {str(e)}"
