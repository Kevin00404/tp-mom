import pika
import random
import string
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareMessageError
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self._channel = self._connection.channel()

        self._queue_name = queue_name
        self._channel.queue_declare(queue=self._queue_name)

        self._channel.basic_qos(prefetch_count=1)#  indicar a RabbitMQ que no entregue más de un mensaje a la vez a un trabajador; dicho de otro modo, que no envíe un nuevo mensaje a un trabajador hasta que este haya procesado y confirmado la recepción del anterior. En su lugar, el mensaje se enviará al siguiente trabajador que no esté ocupado.
        self._consuming = False

    #Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
	#cada mensaje de datos o de control con el cuerpo del mensaje.
	# on_message_callback tiene como parámetros:
	# message - El valor tal y como lo recibe el método send de esta clase.
	# ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
	# nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback):
        def internal_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        # Configura la suscripción a la cola
        #queue: La cola que va a escuchar
        #on_message_callback: La función a llamar cuando llegue un mensaje
        #auto_ack=False: No Confirma automáticamente a rabbit que el mensaje fue recibido
        try:
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=internal_callback,
                auto_ack=False
            )

            self._consuming = True
            self._channel.start_consuming()# bloqueante

        except pika.exceptions.AMQPConnectionError as err:
            #se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
            raise MessageMiddlewareDisconnectedError(str(err)) from err

        except Exception as err:
            #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
            raise MessageMiddlewareMessageError(str(err)) from err

        finally:
            self._consuming = False
    
    #Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
    #no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        if not self._consuming or not self._channel or not self._channel.is_open:
            return

        try:
            self._channel.stop_consuming()
            self._consuming = False

        #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
        except pika.exceptions.AMQPConnectionError as err:
            raise MessageMiddlewareDisconnectedError(str(err)) from err

        # Por si el canal ya se cerró internamente, no tiene efecto. Contiene el "no se estaba consumiendo de la cola/exchange"
        except pika.exceptions.AMQPChannelError:
            self._consuming = False

    
    #Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange='',
                routing_key=self._queue_name,
                body=message
            )

        #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
        except pika.exceptions.AMQPConnectionError as err:
            raise MessageMiddlewareDisconnectedError(str(err)) from err
        
        except pika.exceptions.AMQPChannelError as err:
            raise MessageMiddlewareDisconnectedError(str(err)) from err

        #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
        except Exception as err:
            raise MessageMiddlewareMessageError(str(err)) from err

    #Se desconecta de la cola o exchange al que estaba conectado.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        try:
            if self._consuming and self._channel and self._channel.is_open:
                self._channel.stop_consuming()
                self._consuming = False

            if self._channel and self._channel.is_open:
                self._channel.close()

            if self._connection and self._connection.is_open:
                self._connection.close()

        except Exception as err:
            #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
            raise MessageMiddlewareCloseError(str(err)) from err

















class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
