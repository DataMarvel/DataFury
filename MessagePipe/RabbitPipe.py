# -*- coding: UTF-8 -*-
"""
Created on 2018年11月22日
@author: Leo
@file: RabbitPipe
"""
# Python内置库
import json

# Python第三方库
import pika

# 项目内部库
from DataVision.LoggerHandler.logger import VisionLogger

# 日志路径
LOGGER_PATH = '../../DataVision/LoggerConfig/logger_config.yaml'

# 日志
logger = VisionLogger(LOGGER_PATH)


class RabbitMessageClient:

    def __init__(self,
                 username: str = "",
                 password: str = "",
                 host: str = "localhost",
                 port: int = 5672,
                 virtual_host: str = "/",
                 json_config: bool = False,
                 **rabbit_kwargs):
        """
        RabbitMQ连接初始化
        :param username: 用户名
        :param password: 密码
        :param host: ip地址
        :param port: 端口
        :param virtual_host: 虚拟host(对应的路由)
        :param json_config: 是否使用json读取配置 (优先级最高)
        :param rabbit_kwargs: 其他参数
        """
        # 初始化变量
        self._username = username
        self._password = password
        self._host = host
        self._port = port
        self._virtual_host = virtual_host

        # 判断是否使用json读取
        self._json_config = json_config
        if self._json_config:
            config = self._load_config_from_json()
            if len(config) == 0:
                self._rabbit_client = None
                logger.vision_logger(level="ERROR", log_msg="RabbitMQ配置项不存在或配置字段错误!")
                return
            else:
                try:
                    self._username = config['username']
                    self._password = config['password']
                    self._host = config['host']
                    self._port = config['port']
                    self._virtual_host = config['virtual_host']
                except (KeyError, Exception):
                    logger.vision_logger(level="ERROR", log_msg="RabbitMQ配置字段错误!")
                    return
        # 获取认证
        self._credential = self._get_credential()
        self._parameter = self._get_parameter()
        self._connection = self._get_connection(connection_type="Blocking")
        self._channel = self._get_channel()
        self._queue = self._get_queue_declare()

    @staticmethod
    def _load_config_from_json() -> dict:
        """
        从json读取Kafka配置信息
        :return: 配置
        """
        try:
            try:
                rabbit_config = json.loads(
                    open("./config/message_config.json",
                         encoding="UTF-8").read())['RabbitMQ']
            except FileNotFoundError:
                rabbit_config = json.loads(
                    open("../" + "./config/message_config.json",
                         encoding="UTF-8").read())['RabbitMQ']
            return rabbit_config
        except KeyError:
            return {}

    def _get_credential(self, **kwargs) -> pika.credentials.PlainCredentials:
        """
        将用户名和密码进行认证
        :return: 认证对象
        """
        erase_on_connect = kwargs.get('erase_on_connect')
        if erase_on_connect is None:
            erase_on_connect = False
        return pika.PlainCredentials(
            username=self._username,
            password=self._password,
            erase_on_connect=erase_on_connect)

    def _get_parameter(self) -> pika.connection.ConnectionParameters:
        """
        将参数通过pika进行参数化配置
        :return: pika.connection.ConnectionParameters
        """
        parameters = pika.ConnectionParameters(
            host=self._host,
            port=self._port,
            virtual_host=self._virtual_host,
            credentials=self._credential)
        return parameters

    def _get_connection(self, connection_type: str = "Blocking"):
        """
        获取connection
        :param connection_type: 连接类型(Blocking, select, Tornado, Twisted)
        :return: pika Connection
        """
        if connection_type == "Blocking":
            connection = pika.BlockingConnection(parameters=self._parameter)
            return connection
        elif connection_type == "Select":
            connection = pika.SelectConnection(parameters=self._parameter)
            return connection
        elif connection_type == "Twisted":
            connection = pika.TwistedConnection(parameters=self._parameter)
            return connection
        elif connection_type == "AsyncIO":
            connection = pika.adapters.AsyncioConnection(parameters=self._parameter)
            return connection
        else:
            logger.vision_logger(level="ERROR", log_msg="RabbitMQ不存在此类型Connection!")
            return None

    def _get_channel(self):
        """
        获取MQ通道
        :return:
        """
        return self._connection.channel()

    def _get_queue_declare(self,
                           queue_name: str = "",
                           passive=False,
                           durable=False,
                           exclusive=False,
                           auto_delete=False,
                           nowait=False):
        """
        获取队列
        :param passive: bool 只检查队列是否存在
        :param durable: bool 持久化
        :param exclusive: bool 只允许通过当前连接访问
        :param auto_delete: bool 用户取消或断开连接后删除
        :param nowait: bool 不等待Queue.DeclareOk
        :return: 队列
        """
        if queue_name != "":
            return self._channel.queue_declare(queue_name,
                                               passive=passive,
                                               durable=durable,
                                               exclusive=exclusive,
                                               auto_delete=auto_delete,
                                               nowait=nowait)
        else:
            return None

    def _get_exchange_declare(self):
        """
        获取exchange队列
        :return: 队列
        """
        return self._channel.exchange_declare('')

    def _producer(self):
        """
        发送数据
        """


if __name__ == '__main__':
    m = RabbitMessageClient(json_config=True)
