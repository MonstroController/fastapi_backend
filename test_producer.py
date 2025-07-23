from app.core.rabbit.common import EmailUpdateRabbit
from app.core.rabbit.base import RMQ_EXCHANGE
import time
import logging


logging.basicConfig(
    level=logging.INFO,  # или DEBUG, если нужно больше деталей
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

log = logging.getLogger(__name__)


class Producer(EmailUpdateRabbit):

    def produce_hello_message(self, idx: int):
        message_body = f"New message #{idx:02d}"
        log.info("Publish hello message")
        self.channel.basic_publish(
            exchange=RMQ_EXCHANGE, routing_key="", body=message_body
        )
        log.warning("Published hello message")


def main():
    with Producer() as producer:
        for idx in range(1, 11):
            producer.produce_hello_message(idx=idx)
            time.sleep(0.5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye!")
