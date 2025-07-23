import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika import ConnectionParameters, PlainCredentials
from app.core.rabbit.exc import RabbitException

RMQ_HOST = "127.0.0.1"
RMQ_PORT = 5672

RMQ_USER = "rmuser"
RMQ_PASSWORD = "rmpassword"

RMQ_EXCHANGE = "email-updates"
RMQ_ROUTING_KEY = "hello"

connections_params = ConnectionParameters(
    host=RMQ_HOST,
    port=RMQ_PORT,
    credentials=PlainCredentials(username=RMQ_USER, password=RMQ_PASSWORD),
)


class RabbitBase:
    def __init__(
        self, connection_params: pika.ConnectionParameters = connections_params
    ):
        self.connection_params = connection_params
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None

    def get_new_connection(self) -> pika.BaseConnection:
        return pika.BlockingConnection(self.connection_params)

    @property
    def channel(self) -> BlockingChannel:
        if self._channel is None:
            raise RabbitException("Please use context manager for Rabbit helper")
        return self._channel

    def __enter__(self):
        self._connection = self.get_new_connection()
        self._channel = self._connection.channel()
        return self

    def __exit__(self, exc_type, exc_val, exc_rb):
        if self._channel.is_open:
            self._channel.close()
        if self._connection.is_open:
            self._connection.close()
