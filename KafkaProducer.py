# -*- coding: UTF-8 -*-
"""
Created on 2018年11月20日
@author: Leo
@file: KafkaProducer
"""
# Python内部库
import json
from collections import OrderedDict

# Python第三方库
import pykafka

# 项目内部库
from DataVision.LoggerHandler.logger import VisionLogger
from DataFury.MessagePipe.KafkaPipe import KafkaMessageClient

# 日志路径
LOGGER_PATH = '../DataVision/LoggerConfig/logger_config.yaml'

# 日志
logger = VisionLogger(LOGGER_PATH)


class KafkaProducer(object):

    def __init__(self,
                 topic_name: str,
                 json_config: bool = False,
                 **kafka_config):
        """
        Kafka生产者
        :param topic_name: topic名
        :param json_config: 从json配置读取
        :param kafka_config: kafka配置(具体看KafkaMessageClient里)
        """
        if topic_name == "":
            logger.vision_logger(level="ERROR", log_msg="Kafka Topic不能为空!")
            return
        else:
            self._topic_name = topic_name.encode("UTF-8")
            # 获取Kafka Client
            kafka_message_client = KafkaMessageClient(json_config=json_config, **kafka_config)
            # 如果使用的json的话就从json配置文件中获取topic名
            if json_config:
                self._topic_name = kafka_message_client.get_topic().encode("UTF-8")
            # 获取client对象
            self._client = kafka_message_client.get_client()
            # 获取topic对象
            self._topic = self._create_topic()

    def _create_topic(self) -> pykafka.Topic:
        """
        创建或获取topic对象
        :return: 返回一个topic的实例
        """
        try:
            return self._client.topics[self._topic_name]
        except Exception as err:
            logger.vision_logger(level="ERROR", log_msg=str(err))

    def get_producer(self, producer_type: str = 'sync', **producer_config) -> pykafka.Producer:
        """
        创建producer对象
        :param producer_type: 生产者类型(common和sync)
        :return: producer对象
        """
        if self._topic is None:
            logger.vision_logger(level="ERROR", log_msg="创建Producer失败")
        else:
            if producer_type in ['common', 'sync']:
                if producer_type == "common":
                    return self._topic.get_producer(**producer_config)
                elif producer_type == "sync":
                    return self._topic.get_sync_producer(**producer_config)
            else:
                logger.vision_logger(level="ERROR", log_msg="创建Producer失败, Producer类型错误")

    def produce(self, producer: pykafka.Producer, data):
        """
        生产数据
        :param producer: 生产者类型
        :param data: 数据
        """
        if isinstance(data, (dict, OrderedDict)):
            try:
                data = json.dumps(data).encode("UTF-8")
            except Exception as err:
                logger.vision_logger(level="ERROR", log_msg=str(err))
        elif isinstance(data, str):
            data = data.encode("UTF-8")
        elif isinstance(data, (int, float)):
            data = bytes(data)
        else:
            logger.vision_logger(level="ERROR", log_msg="暂时不支持此类型的数据进行发送!")
        # 发送数据
        with producer as pd:
            pd.produce(data)


if __name__ == '__main__':
    p = KafkaProducer(topic_name="192.168.30.243", host_port="120.77.209.23:19001")
    p.produce(producer=p.get_producer(), data="123")
