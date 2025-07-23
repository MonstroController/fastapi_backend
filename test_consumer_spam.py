from test_producer import RMQ_ROUTING_KEY, log
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import BasicProperties, Basic
import time
from app.core.rabbit.common import EmailUpdateRabbit


def process_new_message(
    ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes
):
    log.debug("ch: %s", ch)
    log.debug("method: %s", method)
    log.debug("properties: %s", properties)
    log.debug("body: %s", body)
    log.warning("Start checking new message SPAM")
    time.sleep(1)
    log.warning("Finished processing message %r", body)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    """
    - declare exchange from email
    - bind queue
    - start consuming messages
    """
    with EmailUpdateRabbit() as rabbit:
        rabbit.consume_messages(message_callback=process_new_message)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye!")
