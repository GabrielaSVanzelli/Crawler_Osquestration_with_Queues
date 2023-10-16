import traceback
from typing import Optional, Union, List

import pika
import typer
from loguru import logger

import hashlib
import datetime

from pymongo import MongoClient
from loguru import logger
from urllib.parse import quote_plus



from dotenv import load_dotenv
import os

load_dotenv()

username = quote_plus(os.getenv("USER"))
password = quote_plus(os.getenv("PASSWORD"))

class Client():
    def __init__(self) -> None:
        self.uri = f"mongodb+srv://{username}:{password}@mlentryes.pyvl2kb.mongodb.net/?retryWrites=true&w=majority"

    def upload_data(self, payload=None):
        client = MongoClient(self.uri)
        try:
            db = client["MercadoLivre"]
            data = db.casas
            data.insert_one(payload)
            logger.success("Upload concluído")
        except Exception as e:
            logger.error(e)
        

client = Client()

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host='localhost', port=5672, heartbeat=0, connection_attempts=100
    )
)
channel = connection.channel()
channel.basic_qos(prefetch_count=1)

app = typer.Typer()

@app.command()
def get_data(queue_name: str = "", update: Optional[str] = None) -> None:
    lista_de_bodies = []
    lista_de_tags = []

    logger.debug(f"Worker | RabbitMQ Queue >> {queue_name}.")

    channel.queue_declare(queue=queue_name, durable=True)
    for method, properties, body in channel.consume(queue_name):
        body = eval(body)
        
        lista_de_bodies.append(body)
        lista_de_tags.append(method.delivery_tag)

        if channel.get_waiting_message_count() != 0:
            continue

        logger.info(f"ChatOp Worker | Processing Data >> Please wait.")
        try:
            if body is not None:
                 client.upload_data(payload=body)
            else:
                logger.error(f"Worker | NoneType Data >> NACK.")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                continue
        except Exception as e:
            logger.error(f"Worker | Processing Data >> Error: {e}")
            print(traceback.format_exc())
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        lista_de_bodies = []
        lista_de_tags = []
            

if __name__ == "__main__":
    app()