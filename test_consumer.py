from test_producer import RMQ_ROUTING_KEY, log
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import BasicProperties, Basic
import time
from app.core.rabbit.base import RabbitBase


def process_new_message(
    ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes
):
    log.info("ch: %s", ch)
    log.info("method: %s", method)
    log.info("properties: %s", properties)
    log.info("body: %s", body)
    time.sleep(1)
    log.warning("Finished processing message %r", body)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_messages(channel: BlockingChannel) -> None:
    channel.basic_qos(refetch_count=1)
    channel.queue_declare(RMQ_ROUTING_KEY)
    channel.basic_consume(
        queue=RMQ_ROUTING_KEY, on_message_callback=process_new_message
    )
    log.warning("Waiting for messages")
    channel.start_consuming()


def main():
    with RabbitBase() as rabbit:
        consume_messages(channel=rabbit.channel)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye!")
