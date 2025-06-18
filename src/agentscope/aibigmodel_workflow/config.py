import os
import time
import yaml
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO
from flask_cors import CORS
from sqlalchemy import create_engine, text

# 导入redis_utils，优先使用绝对导入，失败时使用相对导入
try:
    import redis_utils
except ImportError:
    from . import redis_utils

from agentscope.aibigmodel_workflow.loghandler import LogHandler

# 获取当前文件（config.py）的绝对路径
current_dir = os.path.dirname(os.path.abspath(__file__))
# 构建 config.yaml 的绝对路径
config_path = os.path.join(current_dir, "config.yaml")

app = Flask(__name__)

# 设置时区为东八区
os.environ['TZ'] = 'Asia/Shanghai'
if not os.name == 'nt':  # 检查是否为 Windows 系统
    time.tzset()

# 读取 YAML 文件
test_without_mysql = False
if test_without_mysql:
    # Set the cache directory
    from pathlib import Path

    _cache_dir = Path.home() / ".cache" / "agentscope-studio"
    _cache_db = _cache_dir / "agentscope.db"
    os.makedirs(str(_cache_dir), exist_ok=True)
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{str(_cache_db)}"
else:
    # 加载配置文件
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
        
    # 环境变量覆盖配置
    # 映射规则：环境变量名使用大写，多级配置使用下划线连接
    # 例如：
    # - DATABASE_USERNAME 对应 config['database']['username']
    # - SERVICE_LLM_URL 对应 config['service']['llm_url']
    def override_config_from_env(config_dict):
        """从环境变量中覆盖配置
        
        通过环境变量覆盖YAML配置，映射规则为：
        - 环境变量名使用大写
        - 多级配置使用下划线连接
        - 例如：DATABASE_USERNAME 对应 config['database']['username']
        
        Args:
            config_dict: 配置字典
        
        Returns:
            更新后的配置字典
        """
        # 遍历所有环境变量
        for env_key, env_value in os.environ.items():
            # 尝试将环境变量映射到配置
            config_keys = env_key.split('_')
            
            # 至少需要两个部分才能映射到配置
            if len(config_keys) >= 2:
                # 所有键转为小写以匹配YAML配置
                keys_lower = [k.lower() for k in config_keys]
                
                # 递归查找和更新配置
                current_dict = config_dict
                found = True
                
                # 遍历除最后一个键以外的所有键，检查路径是否存在
                for i, key in enumerate(keys_lower[:-1]):
                    if key in current_dict:
                        current_dict = current_dict[key]
                    else:
                        found = False
                        break
                
                # 如果路径存在且最后一个键也存在，则更新值
                last_key = keys_lower[-1]
                if found and last_key in current_dict:
                    # 获取原始值以确定类型
                    original_value = current_dict[last_key]
                    
                    # 根据原始值的类型进行类型转换
                    try:
                        if isinstance(original_value, int):
                            current_dict[last_key] = int(env_value)
                        elif isinstance(original_value, float):
                            current_dict[last_key] = float(env_value)
                        elif isinstance(original_value, bool):
                            current_dict[last_key] = env_value.lower() in ('true', 'yes', '1')
                        else:
                            current_dict[last_key] = env_value
                            
                        print(f"配置已从环境变量覆盖: {env_key} ")
                    except (ValueError, TypeError) as e:
                        print(f"环境变量类型转换错误: {env_key}, 错误: {str(e)}")
    
        
        return config_dict
    
    # 应用环境变量覆盖
    config = override_config_from_env(config)

    # 从 YAML 文件中提取参数
    DB_NAME = config['db']['name']

    DIALECT = config['database']['dialect']
    DRIVER = config['database']['driver']
    USERNAME = config['database']['username']
    PASSWORD = config['database']['password']
    HOST = config['database']['host']
    PORT = config['database']['port']
    DATABASE = config['database']['database']

    TIDB_DIALECT = config['tidb']['dialect']
    TIDB_DRIVER = config['tidb']['driver']
    TIDB_USERNAME = config['tidb']['username']
    TIDB_PASSWORD = config['tidb']['password']
    TIDB_HOST = config['tidb']['host']
    TIDB_PORT = config['tidb']['port']
    TIDB_DATABASE = config['tidb']['database']

    OCEANBASE_DIALECT = config['oceanbase']['dialect']
    OCEANBASE_DRIVER = config['oceanbase']['driver']
    OCEANBASE_USERNAME = config['oceanbase']['username']
    OCEANBASE_PASSWORD = config['oceanbase']['password']
    OCEANBASE_HOST = config['oceanbase']['host']
    OCEANBASE_PORT = config['oceanbase']['port']
    OCEANBASE_DATABASE = config['oceanbase']['database']

    SERVICE_URL = config['service']['service_url']
    SERVICE_INNER_URL = config['service']['service_inner_url']
    RAG_URL = config['service']['rag_url']
    LLM_URL = config['service']['llm_url']
    LLM_TOKEN = config['service']['llm_token']

    GUI_URL = config['service']['gui_agent_url']
    FILE_GENERATE_URL = config['service']['file_generate_url']
    FILE_PARSE_URL = config['service']['file_parse_url']

    BFF_SERVICE_ENDPOINT = config['service']['bff_service_endpoint']
    KONG_SERVICE = config['service']['kong_service']
    AGENTSCOPE_HOST = config['service']['agentscope_host']

    SERVER_PORT = config['server']['server_port']

    REDIS_HOST = config['redis']['host']
    REDIS_PORT = config['redis']['port']
    REDIS_DB = config['redis']['db']
    REDIS_PASSWORD = config['redis']['password']

    # 根据DB_NAME选择配置
    if DB_NAME.lower() == 'tidb':
        DIALECT = TIDB_DIALECT
        DRIVER = TIDB_DRIVER
        USERNAME = TIDB_USERNAME
        PASSWORD = TIDB_PASSWORD
        HOST = TIDB_HOST
        PORT = TIDB_PORT
        DATABASE = TIDB_DATABASE
    if DB_NAME.lower() == 'oceanbase':
        DIALECT = OCEANBASE_DIALECT
        DRIVER = OCEANBASE_DRIVER
        USERNAME = OCEANBASE_USERNAME
        PASSWORD = OCEANBASE_PASSWORD
        HOST = OCEANBASE_HOST
        PORT = OCEANBASE_PORT
        DATABASE = OCEANBASE_DATABASE
    
    # 创建不带数据库名的临时连接
    temp_uri = f"{DIALECT}+{DRIVER}://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/"
    engine = create_engine(temp_uri)
    try:
        with engine.connect() as conn:
            # 创建数据库（如果不存在）
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DATABASE} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;"))
            conn.commit()
    except Exception as e:
        print(f"Error creating database: {str(e)}")
        raise

    SQLALCHEMY_DATABASE_URI = "{}+{}://{}:{}@{}:{}/{}?charset=utf8".format(DIALECT, DRIVER, USERNAME, PASSWORD, HOST,
                                                                           PORT,
                                                                           DATABASE)
    app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
    app.config['SQLALCHEMY_ECHO'] = True
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
        'pool_size': 20,  # 推荐值：5-30（根据并发量调整）
        'max_overflow': 10,  # 推荐值：5-20（突发流量缓冲）
        'pool_timeout': 60,  # 推荐值：10-30秒（避免长时间阻塞）
        'pool_recycle': 3600,  # 推荐值：小于数据库的 wait_timeout（如MySQL默认8小时）
        'pool_pre_ping': True,  # 生产环境建议开启
        'pool_use_lifo': True  # 推荐启用（提高连接复用率）
    }

    # 创建全局Redis单例
    redis_queue = redis_utils.RedisThreadQueue(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)
    redis_queue.redis_client.ping()


db = SQLAlchemy()
db.init_app(app)

socketio = SocketIO(app)

# This will enable CORS for all routes
CORS(app)

_RUNS_DIRS = []