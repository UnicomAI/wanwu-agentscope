import pymysql
import json

# 用户输入的需要替换的URL
origin_url = input("请输入需要替换的URL: ")

# 用户输入的新URL
new_url = input("请输入新的URL: ")

# 数据库连接参数
db_config = {
    'host': '',
    'port': '',
    'user': 'root',
    'password': '2292558Huawei',
    'database': 'agentscope',
    'charset': 'utf8mb4'
}

# 连接数据库
connection = pymysql.connect(**db_config)

try:
    with connection.cursor() as cursor:
        # 查询原始数据
        sql_select = "SELECT id, plugin_desc_config FROM llm_plugin_info"
        cursor.execute(sql_select)
        results = cursor.fetchall()

        # 遍历结果并更新URL
        for row in results:
            record_id, plugin_desc_config = row
            config_dict = json.loads(plugin_desc_config)
            for server in config_dict.get('servers', []):
                if server.get('url') == origin_url:
                    server['url'] = new_url
            updated_config = json.dumps(config_dict, ensure_ascii=False)

            # 更新数据库
            sql_update = "UPDATE llm_plugin_info SET plugin_desc_config = %s WHERE id = %s"
            cursor.execute(sql_update, (updated_config, record_id))

        # 提交事务
        connection.commit()
finally:
    connection.close()
