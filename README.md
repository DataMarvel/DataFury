# DataFury
Data Messager for Data Marvel

---

<h3 id="DevInfo">开发简介</h3>

* DataMarvel的数据通道
* 目前计划实现的消息通道有:
    * Kafka
    * RabbitMQ
    * ActiveMQ
    * GRPC
    * thift
    * ...

---

<h3 id="DevProcess">开发进度</h3>

* 目前完成的消息通道:
    * Kafka

* 正在进行:
    * RabbitMQ

---

<h3 id="DevError">一些开发错误</h3>

* pykafka使用自定义日志的时候会出现如下报错: (待作者解决: https://github.com/Parsely/pykafka/issues/863)

```html
--- Logging error ---
Traceback (most recent call last):
  File "C:\Python\Python36\lib\logging\handlers.py", line 73, in emit
    logging.FileHandler.emit(self, record)
  File "C:\Python\Python36\lib\logging\__init__.py", line 1070, in emit
    self.stream = self._open()
  File "C:\Python\Python36\lib\logging\__init__.py", line 1060, in _open
    return open(self.baseFilename, self.mode, encoding=self.encoding)
NameError: name 'open' is not defined
Call stack:
  File "C:\Python\Python36\lib\site-packages\pykafka\handlers.py", line 164, in __del__
    self.stop()
  File "C:\Python\Python36\lib\site-packages\pykafka\handlers.py", line 190, in stop
    log.info("RequestHandler.stop: about to flush requests queue")
Message: 'RequestHandler.stop: about to flush requests queue'
Arguments: ()
```