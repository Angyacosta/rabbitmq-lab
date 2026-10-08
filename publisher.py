import json
import sys
from datetime import datetime

import pika
from pika.exceptions import AMQPConnectionError, AMQPError

HOST = "localhost"
EXCHANGE = "microservices.events"
QUEUE = "payment.created.queue"
ROUTING_KEY = "payment.created"


def build_message(i: int) -> dict:
    return {
        "eventType": "PaymentCreated",
        "id": f"PAY-{i:03d}",
        "amount": 150000 + i * 1000,
        "status": "APPROVED",
        "createdAt": datetime.now().isoformat(timespec="seconds"),
    }


def main():
    try:
        credentials = pika.PlainCredentials("guest", "guest")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=HOST, credentials=credentials)
        )
        channel = connection.channel()

       
        channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
        channel.queue_declare(queue=QUEUE, durable=True)
        channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=ROUTING_KEY)

        for i in range(1, 6):  # 5 mensajes
            message = build_message(i)
            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=ROUTING_KEY,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,  # mensaje persistente
                ),
            )
            print(f"[Publisher] Enviado: {message}")

        connection.close()
        print("[Publisher] Listo.")
    except AMQPConnectionError:
        print("Error: no se pudo conectar a RabbitMQ. ¿Está corriendo el contenedor?")
        sys.exit(1)
    except AMQPError as e:
        print(f"Error de RabbitMQ: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()