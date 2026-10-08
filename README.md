# Laboratorio RabbitMQ: Comunicación asíncrona entre microservicios

**Universidad de los Llanos** · Facultad de Ciencias Básicas e Ingenierías
Programa de Ingeniería de Sistemas · 2026-II

**Lenguaje:** Python 3 · **Librería:** pika
---

## 1. Descripción

Este proyecto simula la comunicación asíncrona entre dos microservicios usando RabbitMQ como broker de mensajes:

- **Publisher:** microservicio que genera el evento `PaymentCreated` y lo publica en RabbitMQ.
- **RabbitMQ:** recibe el mensaje en un exchange y lo enruta a una cola.
- **Consumer:** microservicio que lee la cola, procesa el mensaje y confirma (ack).

```
Publisher -> Exchange (microservices.events) -> Queue (payment.created.queue) -> Consumer
```

El publisher no necesita saber quién procesará el mensaje; solo lo envía a RabbitMQ.

## 2. Configuración usada

| Elemento | Valor |
|---|---|
| Virtual host | `/` |
| Exchange | `microservices.events` |
| Tipo de exchange | `direct` |
| Cola | `payment.created.queue` |
| Routing key | `payment.created` |
| Usuario | `[guest u otro que hayas usado]` | en nuestro caso guest 

## 3. Estructura del proyecto

```
rabbitmq-lab/
├── publisher.py
├── consumer.py
├── requirements.txt
├── README.md
└── capturas/
```

## 4. Requisitos

- Windows 10 u 11
- Docker Desktop instalado y corriendo
- Postman
- Python 3.10 o superior

## 5. Paso a paso

### 5.1 Verificar Docker
```
docker --version
docker run hello-world
```
Evidencia: `capturas/01_docker_version.png`

### 5.2 Levantar RabbitMQ en Docker
```
docker run -d --hostname rabbit-host --name rabbitmq-lab -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```
Se usa la imagen `rabbitmq:4-management` porque incluye el plugin de gestión (interfaz web y API HTTP). La imagen `rabbitmq:latest` no lo incluye.

Verificar:
```
docker ps
```
Evidencia: `capturas/02_contenedor_corriendo.png`

Comandos útiles:
```
docker start rabbitmq-lab
docker stop rabbitmq-lab
docker logs rabbitmq-lab
```

### 5.3 Entrar al Management UI
Abrir `http://localhost:15672` con usuario `[guest]` y contraseña `[guest]`.
Evidencia: `capturas/03_management_ui.png`

### 5.4 Configurar Postman
Colección **RabbitMQ Lab** con Authorization tipo **Basic Auth** (mismo usuario y contraseña del Management UI). En cada request con body: raw, JSON, y el header `Content-Type: application/json`.
Evidencia: `capturas/04_postman_config.png`

### 5.5 Crear los recursos con Postman
Base URL: `http://localhost:15672`

| # | Método | URL | Body |
|---|---|---|---|
| 1 | PUT | `/api/exchanges/%2F/microservices.events` | `{"type":"direct","durable":true}` |
| 2 | PUT | `/api/queues/%2F/payment.created.queue` | `{"durable":true}` |
| 3 | POST | `/api/bindings/%2F/e/microservices.events/q/payment.created.queue` | `{"routing_key":"payment.created"}` |

Resultado esperado: `201 Created` en las tres.
Evidencia: `capturas/05_exchange_cola_creados.png`

### 5.6 Publicar y consumir con Postman

| # | Acción | Método | URL | Body |
|---|---|---|---|---|
| 4 | Publicar | POST | `/api/exchanges/%2F/microservices.events/publish` | ver abajo |
| 6 | Consumir sin borrar | POST | `/api/queues/%2F/payment.created.queue/get` | `ackmode: ack_requeue_true` |
| 5 | Consumir y borrar | POST | `/api/queues/%2F/payment.created.queue/get` | `ackmode: ack_requeue_false` |
| 7 | Purgar cola | DELETE | `/api/queues/%2F/payment.created.queue/contents` | sin body |

Body de la request 4:
```json
{
  "properties": {},
  "routing_key": "payment.created",
  "payload": "{\"eventType\":\"PaymentCreated\",\"paymentId\":\"PAY-001\",\"amount\":150000,\"status\":\"APPROVED\"}",
  "payload_encoding": "string"
}
```

Body de las requests 5 y 6 (cambia solo `ackmode`):
```json
{ "count": 1, "ackmode": "ack_requeue_false", "encoding": "auto", "truncate": 50000 }
```

Qué se observó:
- Request 4: respuesta `{"routed": true}` y la cola con 1 mensaje en Ready.
- Request 6: devuelve el mensaje y este sigue en la cola.
- Request 5: devuelve el mensaje y este se elimina de la cola.
- Request 7: elimina todos los mensajes pendientes.

Evidencias: `06_postman_publicar.png`, `07_cola_ready_1.png`, `08_postman_consumir_sin_borrar.png`, `09_postman_consumir_borrar.png`, `10_postman_purgar.png`

## 6. Aplicación Publisher / Consumer

### 6.1 Instalar dependencias
```
pip install -r requirements.txt
```

### 6.2 Ejecutar el consumer (terminal 1)
```
python consumer.py
```
Queda esperando mensajes.

### 6.3 Ejecutar el publisher (terminal 2)
```
python publisher.py
```
Envía 5 mensajes JSON.

Evidencias: `11_consumer_publisher.png`, `12_cola_vacia.png`

## 7. Formato del mensaje

```json
{
  "eventType": "PaymentCreated",
  "id": "PAY-001",
  "amount": 151000,
  "status": "APPROVED",
  "createdAt": "2026-10-08T10:00:00"
}
```

## 8. Cumplimiento de requisitos

| # | Requisito | Cómo se cumple |
|---|---|---|
| 1 | Mínimo 5 mensajes | El publisher envía 5 en un ciclo |
| 2 | Formato JSON | Se serializa con `json.dumps` |
| 3 | Campos mínimos | `eventType`, `id`, `status`, `createdAt` |
| 3 | Campos mínimos | El publisher los incluye en cada mensaje y el consumer los valida (`validate_message`) |
| 4 | Consumer lee de la cola | `basic_consume` sobre `payment.created.queue` |
| 5 | Imprime en consola | `print` en el callback |
| 6 | Ack tras procesar | `basic_ack` con `auto_ack=False` |
| 7 | Manejo de errores | Ver sección 9 |
| 8 | README | Este documento |

## 9. Manejo de errores

- **Conexión fallida:** si RabbitMQ no está disponible, el publisher y el consumer muestran un mensaje claro y terminan con código 1.
- **Mensaje inválido (JSON o campos faltantes):** `basic_nack` sin reencolar, para no entrar en un ciclo infinito.
- **Error inesperado al procesar:** `basic_nack` con reencolado para reintentar.
- **Ctrl+C en el consumer:** cierre controlado.

- **Campos obligatorios faltantes:** el consumer valida `eventType`, `id`, `status` y `createdAt`; si falta alguno, descarta el mensaje con `basic_nack` sin reencolar.  

Evidencia: `capturas/16_mensaje_invalido.png`

Prueba realizada: con `docker stop rabbitmq-lab`, el publisher muestra el error de conexión.
Evidencia: `capturas/14_error_conexion.png`

