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
                 queue_name: str = "",
                 exchange: str = "",
                 routing_key: str = "",
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
        self._queue_name = queue_name
        self._exchange = exchange
        self._routing_key = routing_key

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
                    self._queue_name = config['queue_name']
                    self._exchange = config['exchange']
                    self._routing_key = config['routing_key']
                except (KeyError, Exception):
                    logger.vision_logger(level="ERROR", log_msg="RabbitMQ配置字段错误!")
                    return
        # 获取认证
        self._credential = self._get_credential(**rabbit_kwargs)
        self._parameter = self._get_parameter()
        self._connection = self._get_connection(connection_type="Blocking")
        self._channel = self._get_channel()
        self._get_queue_declare(queue_name=self._queue_name)

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
                           callback=None,
                           queue_name: str = "",
                           passive=False,
                           durable=False,
                           exclusive=False,
                           auto_delete=False,
                           nowait=False):
        """
        获取队列
        :param callback: 回调函数 自定义
        :param passive: bool 只检查队列是否存在
        :param durable: bool 队列持久化
        :param exclusive: bool 只允许通过当前连接访问
        :param auto_delete: bool 用户取消或断开连接后删除
        :param nowait: bool 不等待Queue.DeclareOk
        """
        if queue_name != "":
            self._channel.queue_declare(callback,
                                        queue_name,
                                        passive,
                                        durable,
                                        exclusive,
                                        auto_delete,
                                        nowait)

    def _get_exchange_declare(self,
                              callback=None,
                              exchange=None,
                              exchange_type='direct',
                              passive=False,
                              durable=False,
                              auto_delete=False,
                              internal=False,
                              nowait=False):
        """
        获取交换机exchange队列
        :return: 队列
        """
        # TODO 暂时还不能使用 交换机队列
        return self._channel.exchange_declare(callback,
                                              exchange,
                                              exchange_type,
                                              passive,
                                              durable,
                                              auto_delete,
                                              internal,
                                              nowait)

    def producer(self,
                 body,
                 exchange: str = "",
                 routing_key: str = "",
                 properties=None,
                 mandatory: bool = False,
                 immediate: bool = False):
        """
        发送数据
        :param exchange: str 确切地指定消息应该到哪个队列去
        :param routing_key: str 队列名
        :param body: 消息内容(str 或者 unicode)
        :param properties: 配置项
             例如:
             pika.BasicProperties(delivery_mode = 2) # 使消息或任务也持久化存储
             消息队列持久化包括3个部分：
    　　      （1）exchange持久化，在声明时指定durable => 1
    　　      （2）queue持久化，在声明时指定durable => 1
    　　      （3）消息持久化，在投递时指定delivery_mode=> 2（1是非持久化）

             如果exchange和queue都是持久化的，那么它们之间的binding也是持久化的。
             如果exchange和queue两者之间有一个持久化，一个非持久化，就不允许建立绑定。
        :param mandatory: bool
        :param immediate: bool
        """
        if self._channel is not None:
            if exchange == "":
                exchange = self._exchange
            if routing_key == "":
                routing_key = self._queue_name
            if body == "" or body is None:
                raise ValueError("Body argument is empty! Body参数为空")
            self._channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=properties,
                mandatory=mandatory,
                immediate=immediate)
        else:
            logger.vision_logger(level="ERROR", log_msg="队列声明失败!")

    @staticmethod
    def _consumer_default_callback(ch, method, properties, body):
        """
        消费者默认回调函数
        """
        data_body = body.decode('UTF-8')
        logger.vision_logger(level="INFO", log_msg="数据接收成功!数据为 --> {}".format(data_body))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def consumer(self,
                 callback=None,
                 queue: str = "",
                 no_ack: bool = False,
                 exclusive: bool = False):
        """
        接收数据
        :param callback: 回调函数
        :param queue: 队列名称
        :param no_ack: bool -> if set to True, automatic acknowledgement mode will be used
        :param exclusive: bool -> Don't allow other consumers on the queue 不允许其他消费者消费
        """
        if callback is None:
            callback = self._consumer_default_callback
        if queue == "":
            queue = self._queue_name
        self._channel.basic_consume(consumer_callback=callback,
                                    queue=queue,
                                    no_ack=no_ack,
                                    exclusive=exclusive)
        logger.vision_logger(level="INFO", log_msg="等待消费...")
        self._channel.start_consuming()

    def close_connection(self):
        """
        关闭连接
        """
        self._connection.close()


if __name__ == '__main__':
    m = RabbitMessageClient(json_config=True)

    # Producer test
    # m.producer(body="python test! Hello World! - 0")

    # Consumer test
    # m.consumer(callback=None)
