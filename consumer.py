import json
import sys

import pika
from pika.exceptions import AMQPConnectionError, AMQPError

HOST = "localhost"
QUEUE = "payment.created.queue"
REQUIRED_FIELDS = ("eventType", "id", "status", "createdAt")


class InvalidMessageError(Exception):
    """El mensaje no cumple el formato esperado."""


def validate_message(event: dict):
    if not isinstance(event, dict):
        raise InvalidMessageError("el mensaje no es un objeto JSON")
    missing = [f for f in REQUIRED_FIELDS if f not in event]
    if missing:
        raise InvalidMessageError(f"faltan campos obligatorios: {', '.join(missing)}")


def process_message(event: dict):
    # Lógica de negocio separada de la infraestructura
    print(f"[Consumer] Procesando pago {event['id']} "
          f"({event['eventType']}) estado={event['status']} "
          f"fecha={event['createdAt']}")


def on_message(channel, method, properties, body):
    try:
        event = json.loads(body)
        print(f"[Consumer] Recibido: {event}")
        validate_message(event)
        process_message(event)
        channel.basic_ack(delivery_tag=method.delivery_tag)  # confirma y borra de la cola
        print("[Consumer] Ack enviado.")
    except (json.JSONDecodeError, InvalidMessageError) as e:
        print(f"[Consumer] Mensaje inválido, se descarta: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"[Consumer] Error procesando mensaje, se reencola: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    try:
        credentials = pika.PlainCredentials("guest", "guest")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=HOST, credentials=credentials)
        )
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=QUEUE, on_message_callback=on_message, auto_ack=False)

        print("[Consumer] Esperando mensajes. Ctrl+C para salir.")
        channel.start_consuming()
    except AMQPConnectionError:
        print("Error: no se pudo conectar a RabbitMQ. ¿Está corriendo el contenedor?")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n[Consumer] Detenido por el usuario.")
    except AMQPError as e:
        print(f"Error de RabbitMQ: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()