import pika


class RabbitBase:
    def __init__(self):
        self.connection = pika.BlockingConnection()
