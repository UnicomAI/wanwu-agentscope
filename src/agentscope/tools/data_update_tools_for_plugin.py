import pymysql
import json

# 用户输入的需要替换的 model
origin_model = "unicom-70b-chat"

# 用户输入的新 model
new_model = "yuanjing-70b-chat"

# 数据库连接参数
db_config = {
    'host': '127.0.0.1',  # 数据库地址
    'port': 3306,  # 数据库端口
    'user': 'root',  # 数据库用户名
    'password': '2292558Huawei',  # 数据库密码
    'database': 'agentscope',  # 数据库名称
    'charset': 'utf8mb4'  # 字符集
}

# 连接数据库
connection = pymysql.connect(**db_config)

# 递归函数来遍历和修改字典
def modify_dict(d):
    if isinstance(d, dict):
        for key, value in list(d.items()):
            if key == "model" and value == origin_model:
                d[key] = new_model
            elif key == "name" and value == "repetition_penalty":
                d[key] = "presence_penalty"  # 将 repetition_penalty 重命名为 presence_penalty
            modify_dict(value)
    elif isinstance(d, list):
        for item in d:
            modify_dict(item)

try:
    with connection.cursor() as cursor:
        # 查询原始数据
        sql_select = "SELECT id, dag_content FROM llm_plugin_info"
        cursor.execute(sql_select)
        results = cursor.fetchall()

        # 遍历结果并更新数据
        for row in results:
            record_id, dag_content = row
            dag_dict = json.loads(dag_content)

            # 修改字典中的特定字段
            modify_dict(dag_dict)

            # 将修改后的字典转换回 JSON 字符串
            updated_dag_content = json.dumps(dag_dict, ensure_ascii=False)

            # 更新数据库
            sql_update = "UPDATE llm_plugin_info SET dag_content = %s WHERE id = %s"
            cursor.execute(sql_update, (updated_dag_content, record_id))

        # 提交事务
        connection.commit()
finally:
    connection.close()