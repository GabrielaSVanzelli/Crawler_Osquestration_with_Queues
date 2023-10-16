from time import sleep
import pika
from crawler_ml import CrawlerML
from typing import Union, Optional, Iterable
from loguru import logger
import typer
from requests_html import HTMLSession
import traceback

app = typer.Typer()


class SenderTask:
    def __init__(self, queue_name:str) -> None:
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='localhost',
                port=5672,
                connection_attempts=5
            )
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(
            queue=queue_name,
            durable=True,
            exclusive=False
        )
        self.queue_name = queue_name

    def send_message(self, message:str) -> Optional[dict]:
        for _ in range(20):
            try:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.queue_name,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                    )
                )
                return {"Status":"Success"}

            except Exception as e:
                logger.error(e)
                sleep(10)
                continue
        return None

    def run_crawler(self) -> Iterable[dict]:
        crawler = CrawlerML(
            session = HTMLSession(),
            logger = logger
        )
        for data in crawler.get_data():
            yield data



@app.command()
def send_task(queue_name:str=None):
    
    sender = SenderTask(queue_name=queue_name)
    tmp_buffer = set()
    
    for count, data in enumerate(sender.run_crawler()):
        try:
            data = (data)
            message = str(data)
            status = sender.send_message(message=message)

            if status is None:
                logger.error(f"Sent was filled {count+1}")
                tmp_buffer.add(message)

            logger.success(f"Sent message {count+1}")
            continue
        except:
            print(traceback())
            pass


if __name__ == "__main__":
    app()