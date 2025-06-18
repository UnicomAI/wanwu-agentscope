import gc
import json
import os
import sys
import threading
import time

from loguru import logger as loguru_logger  # 重命名避免冲突
import psutil
from typing import Optional, Dict, Any
import atexit


class LogHandler:
    """
    安全的日志处理模块（单例模式）
    功能：
    1. 自动禁用危险默认配置
    2. 智能日志截断和采样
    3. 内存监控和告警
    """

    _instance = None
    _lock = threading.Lock()
    _LOG_CACHE = {}
    _MAX_CACHE_SIZE = 5000
    # 标准日志格式
    LOG_FORMAT = (
        "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
        "{level} | "
        "{message}"
    )

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance.__init__(*args, **kwargs)
        return cls._instance

    def __init__(
            self,
            log_dir: str = "./logs",
            enable_console: bool = False,
            max_log_size: str = "1 MB",
            retention_days: str = "7",
            compression: str = "zip",
            log_level: str = "INFO"
    ):
        if getattr(self, '__initialized', False):
            return

        # 初始化配置
        self.log_config = {
            'log_dir': log_dir,
            'enable_console': enable_console,
            'max_log_size': max_log_size,
            'retention': f"{retention_days} days",
            'compression': compression,
            'log_level': log_level
        }

        self._setup_logger()
        self.__initialized = True
        self._last_cleanup_time = 0
        self._cleanup_interval = 3600  # 每小时执行一次清理
        atexit.register(self._cleanup)

    def _setup_logger(self):
        """核心日志配置（线程安全）"""
        try:
            # 禁用默认配置
            os.environ["LOGURU_AUTOINIT"] = "False"
            loguru_logger.remove()

            # 确保日志目录存在
            os.makedirs(self.log_config['log_dir'], exist_ok=True)

            # 主日志文件配置
            loguru_logger.add(
                sink=os.path.join(self.log_config['log_dir'], "agentscope.log"),
                rotation=self.log_config['max_log_size'],
                retention=self.log_config['retention'],
                compression=self.log_config['compression'],
                level=self.log_config['log_level'],
                format=self.LOG_FORMAT,
                enqueue=True,
                backtrace=False,
                diagnose=False,
                catch=True,
                mode="a"  # 追加模式
            )

            if self.log_config['enable_console']:
                loguru_logger.add(
                    sink=sys.stderr,
                    level=self.log_config['log_level'],
                    colorize=True
                )

            # 应用全局配置
            self.logger = loguru_logger.patch(self.memory_monitor).patch(self._patch_log_record)

        except Exception as e:
            sys.stderr.write(f"Logger setup failed: {str(e)}\n")
            self.logger = loguru_logger
            self.logger.add(sys.stderr, level="ERROR")

    def _cleanup(self) -> None:
        """资源清理"""
        self.logger.complete()
        self._LOG_CACHE.clear()

    def _patch_log_record(self, record: Dict[str, Any]) -> Any:
        """全局修改日志记录（自动调用 secure_log）"""
        record["message"] = self.secure_log(record["message"])
        return record

    def secure_log(self, content: Any, max_len: int = 5000) -> str:
        """
        安全日志处理（带缓存和敏感信息过滤）
        参数:
            content: 日志内容
            max_len: 最大长度限制
        返回:
            处理后的安全字符串
        """
        cache_key = (id(content), max_len)
        if cache_key in self._LOG_CACHE:
            return self._LOG_CACHE[cache_key]

        try:
            if isinstance(content, (dict, list)):
                content = json.dumps(content, ensure_ascii=False)
            content = str(content)

            # 敏感信息过滤
            sensitive_keys = ["password", "token", "secret", "key", "credential"]
            for sensitive in sensitive_keys:
                if sensitive in content.lower():
                    content = content.replace(sensitive, "***")

            if len(content) > max_len:
                half = max_len // 2
                result = f"{content[:half]}...{content[-half:]}"  # 前50% + ... + 后50%
            else:
                result = content

            # 更新缓存
            if len(self._LOG_CACHE) >= self._MAX_CACHE_SIZE:
                self._LOG_CACHE.clear()
            self._LOG_CACHE[cache_key] = result

            return result
        except Exception:
            return "[LOG_PARSE_ERROR]"

    def memory_monitor(self, record: Dict[str, Any]) -> None:
        """基于时间间隔的内存监控"""
        current_time = time.time()

        # 仅在缓存过大时清理
        if len(self._LOG_CACHE) > self._MAX_CACHE_SIZE * 0.8:  # 阈值可调
            self._LOG_CACHE.clear()

        # 新增定期清理
        if current_time - self._last_cleanup_time >= self._cleanup_interval:
            self._perform_cleanup()
            self._last_cleanup_time = current_time

    def _perform_cleanup(self):
        """定期清理"""
        try:
            with self._lock:  # 确保线程安全
                # 清理缓存 (保留最近使用的20%)
                cache_size = len(self._LOG_CACHE)
                if cache_size > self._MAX_CACHE_SIZE * 0.2:
                    # 使用LRU策略保留最近使用的20%
                    keys = list(self._LOG_CACHE.keys())
                    keep_size = int(self._MAX_CACHE_SIZE * 0.2)
                    for key in keys[:-keep_size]:
                        del self._LOG_CACHE[key]

                # 清理日志文件处理器
                loguru_logger.remove()
                self._setup_logger()

        except Exception as e:
            self.logger.error(f"Cleanup failed: {str(e)}")

    def get_logger(self):
        """获取日志器实例"""
        return self.logger


# 全局单例实例
loghandler = LogHandler(
    log_dir="./logs",
    enable_console=True,
    max_log_size="200 MB",
    retention_days="7",
    compression="zip",
    log_level="INFO"
)

# ==================== 使用示例 ====================
if __name__ == "__main__":
    # 初始化（全局唯一实例）
    log_handler1 = LogHandler(log_dir="./logs", enable_console=True)
    log_handler2 = LogHandler()  # 返回同一个实例

    print(id(log_handler1) == id(log_handler2))  # 输出 True

    # 测试日志连续性
    for i in range(100):
        log_handler1.logger.info(f"连续日志测试 {i}")

    # 测试大日志截断
    large_data = {"key": "value" * 1000}
    log_handler1.logger.info("大数据测试: {}", log_handler1.secure_log(large_data))
