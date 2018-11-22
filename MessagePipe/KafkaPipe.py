# -*- coding: UTF-8 -*-
"""
Created on 2018年11月19日
@author: Leo
@file: KafkaPipe
"""
# Python内置库
import json
import logging
from typing import TypeVar, List, Tuple, Any

# Pykafka
from pykafka import KafkaClient

# 项目内部库
from DataVision.LoggerHandler.logger import VisionLogger

# 日志路径
LOGGER_PATH = '../DataVision/LoggerConfig/logger_config.yaml'

# 日志
logger = VisionLogger(LOGGER_PATH)

# Python的复杂类型提示(Complex type hints)
T = TypeVar('T', str, complex)


class KafkaMessageClient:

    def __init__(self, json_config: bool = False, **kafka_config):
        """
        KAFKA连接初始化
        :param json_config: 从json中读取配置文件
        :param kafka_config: kafka其他配置
                host_port: ip地址和端口号
                zk_connect: zookeeper的ip地址和端口号 用list(tuple(str, str), ..., tuple(str, str)) 进行扩展
        """
        # 初始化
        hosts = ""

        # 获取kafka_config参数的结果
        self._host_port = kafka_config.get('host_port')
        self._zk_connect = kafka_config.get('zk_connect')
        self._socket_timeout = kafka_config.get('timeout')
        if self._socket_timeout is None:
            self._socket_timeout = 30 * 1000

        # 判断是否使用json读取
        self._json_config = json_config

        # 判断是否使用json配置
        if self._json_config:
            config = self._load_config_from_json()
            if len(config) == 0:
                self._kafka_client = None
                logger.vision_logger(level="ERROR", log_msg="Kafka配置项不存在或配置字段错误!")
                return
            else:
                try:
                    hosts = config['hosts']
                    self._zk_connect = config['zk_connect']
                    self._topic = config['topic_name']
                except (KeyError, Exception):
                    logger.vision_logger(level="ERROR", log_msg="Kafka配置字段错误!")
                    return
        else:
            # 获取Kafka host和port
            if self._host_port is not None:
                if isinstance(self._host_port, str):
                    hosts = self._host_port.replace(";", "")
                if isinstance(self._host_port, list):
                    for conn in self._host_port:
                        host = conn[0]
                        port = conn[1]
                        hosts += host + ":" + port + ","
                    hosts = hosts[:-1]
            else:
                logger.vision_logger(level="DEBUG", log_msg="Kafka连接地址和端口暂未配置")

            # 获取zookeeper_connect
            if self._zk_connect is not None:
                if isinstance(self._zk_connect, str):
                    if len(self._zk_connect.split(",")) > 1:
                        self._zk_connect = ",".join(self._zk_connect)
                    else:
                        self._zk_connect = self._zk_connect.replace(";", "").replace(",", "")
                if isinstance(self._zk_connect, list):
                    self._zk_connect = ",".join(self._zk_connect)
            else:
                logger.vision_logger(level="DEBUG", log_msg="Kafka Zookeeper连接地址和端口暂未配置")
        # 连接kafka
        try:
            if self._host_port is not None:
                self._kafka_client = KafkaClient(hosts=hosts,
                                                 socket_timeout_ms=self._socket_timeout)
            elif self._zk_connect is not None:
                self._kafka_client = KafkaClient(zookeeper_hosts=self._zk_connect,
                                                 socket_timeout_ms=self._socket_timeout)
            else:
                logger.vision_logger(level="ERROR", log_msg="Kafka无法连接!")
                self._kafka_client = None
        except Exception as err:
            logger.vision_logger(level="ERROR", log_msg=str(err))
            self._kafka_client = None

    @staticmethod
    def _load_config_from_json() -> dict:
        """
        从json读取Kafka配置信息
        :return: 配置
        """
        try:
            try:
                kafka_config = json.loads(
                    open("./config/message_config.json",
                         encoding="UTF-8").read())['Kafka']
            except FileNotFoundError:
                kafka_config = json.loads(
                    open("../" + "./config/message_config.json",
                         encoding="UTF-8").read())['Kafka']
            return kafka_config
        except KeyError:
            return {}

    def get_client(self) -> KafkaClient:
        """
        获取Kafka Client
        :return: kafka client
        """
        if self._kafka_client is not None:
            logger.vision_logger(level="DEBUG", log_msg="Kafka客户端信息--->{}".format(self._kafka_client))
            logger.vision_logger(level="DEBUG", log_msg="Kafka客户端Topics--->{}".format(
                [d.decode('UTF-8') for d in list(self._kafka_client.topics.keys())])
            )
            logger.vision_logger(level="DEBUG", log_msg="Kafka客户端Brokers--->{}".format(self._kafka_client.brokers))
            logger.vision_logger(level="DEBUG", log_msg="Kafka客户端集群--->{}".format(self._kafka_client.cluster))
        return self._kafka_client

    def get_zk_connect(self) -> str:
        """
        获取zk的配置
        :return: zk配置
        """
        return self._zk_connect

    def get_topic(self) -> str:
        """
        获取topic
        :return: topic读取
        """
        return self._topic

    @staticmethod
    def get_logger() -> logger:
        """
        返回logger
        """
        return logger
