from redis import Redis
from redis.connection import ConnectionPool
import threading
from typing import Optional, Union
from loguru import logger
from redis.exceptions import RedisError

class RedisThreadQueue:
    _instance = None
    _lock = threading.Lock()
    _pool = None

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def __init__(self, host='localhost', port=6379, db=0, password=None):
        if not RedisThreadQueue._pool:
            try:
                RedisThreadQueue._pool = ConnectionPool(
                    host=host,
                    port=port,
                    db=db,
                    password=password
                )
                # Test the connection
                test_client = Redis(connection_pool=self._pool)
                test_client.ping()  # Will raise an exception if connection fails
                logger.info(f"Redis connection successful - Host: {host}, Port: {port}, DB: {db}")
            except Exception as e:
                logger.error(f"Redis connection failed - Host: {host}, Port: {port}, DB: {db}. Error: {str(e)}")
                raise  # Re-raise the exception to prevent further operations
        self.redis_client = Redis(connection_pool=self._pool)
        self._active_queues = set()

    def get_queue_name(self, thread_id: str) -> str:
        queue_name = f"{thread_id}"
        self._active_queues.add(queue_name)
        return queue_name

    def enqueue(self, thread_id: str, data: Union[str, bytes], ttl: Optional[int] = 7200) -> bool:
        """入队并可选设置过期时间（TTL）"""
        try:
            queue_name = self.get_queue_name(thread_id)
            self.redis_client.rpush(queue_name, data)
            if ttl is not None:
                self.redis_client.expire(queue_name, ttl)  # 设置过期时间（秒）
            return True
        except Exception as e:
            logger.error(f"入队错误: {str(e)}")
            return False

    def dequeue(self, thread_id: str, timeout: int = 1) -> Optional[str]:
        try:
            queue_name = self.get_queue_name(thread_id)
            result = self.redis_client.blpop([queue_name], timeout=timeout)
            if result:
                return result[1].decode('utf-8')
            return None
        except Exception as e:
            print(f"出队错误: {str(e)}")
            return None

    def get_all_messages(self, thread_id: str, dag_id="") -> list:
        """获取队列中所有已积累的消息，但不从队列中删除它们

        Args:
            thread_id (str): thread id

        Returns:
            list: 队列中的所有消息列表
        """
        try:
            queue_name = self.get_queue_name(thread_id)

            # 使用Redis的LRANGE命令获取队列中的所有消息
            # 0 -1 表示获取从第一个到最后一个元素的所有消息
            messages = self.redis_client.lrange(queue_name, 0, -1)
            # 将字节转换为字符串
            return [msg.decode('utf-8') if isinstance(msg, bytes) else msg for msg in messages]
        except Exception as e:
            logger.error(f"dag: {dag_id}, Failed to get all messages from queue {thread_id}: {str(e)}")
            return []

    def delete_queue(self, thread_id: str, dag_id="") -> bool:
        try:
            queue_name = self.get_queue_name(thread_id)
            self.redis_client.delete(queue_name)
            self._active_queues.discard(queue_name)
            return True
        except Exception as e:
            logger.error(f"dag: {dag_id}, 删除队列错误: {str(e)}")
            return False

    def llen(self, thread_id: str) -> int:
        try:
            queue_name = self.get_queue_name(thread_id)
            return self.redis_client.llen(queue_name)
        except Exception as e:
            print(f"获取队列长度错误: {str(e)}")
            return 0

    def cleanup_all(self) -> bool:
        try:
            for queue_name in self._active_queues:
                self.redis_client.delete(queue_name)
            self._active_queues.clear()
            return True
        except Exception as e:
            print(f"清理队列错误: {str(e)}")
            return False

    def health_check(self) -> bool:
        """检查连接是否健康"""
        try:
            return self.redis_client.ping()
        except RedisError:
            return False

# # 创建全局单例
# redis_queue = RedisThreadQueue(host='localhost', port=6379)