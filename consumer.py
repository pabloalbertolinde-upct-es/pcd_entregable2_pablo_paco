"""
Se recomienda ejecutar el código en el enlace de Google Colab indicado en el documento de word adjunto
para evitar problemas con Kafka. Se debe a que por diversas fallas no nos ha sido posible poner 
kafka en funcionamiento desde visual studio code. 

Cada vez que se ejecuta el código, para asegurar su correcto funcionamiento
ha de eliminarse y reiniciarse el entorno de ejecución.

Para poner en marcha el servidor con el topico temp se ejecutan los siguientes comandos:

!pip install kafka-python
!curl -sSOL https://dlcdn.apache.org/kafka/3.6.2/kafka_2.13-3.6.2.tgz
!tar -xzf kafka_2.13-3.6.2.tgz
!./kafka_2.13-3.6.2/bin/zookeeper-server-start.sh -daemon ./kafka_2.13-3.6.2/config/zookeeper.properties
!./kafka_2.13-3.6.2/bin/kafka-server-start.sh -daemon ./kafka_2.13-3.6.2/config/server.properties
!echo "Esperamos 10 segundos hasta que los servicios kafka y zookeeper estén activos y funcionando"
!sleep 10
!ps -ef | grep kafka
!./kafka_2.13-3.6.2/bin/kafka-topics.sh --create --topic temp --bootstrap-server localhost:9092
!./kafka_2.13-3.6.2/bin/kafka-topics.sh --describe --bootstrap-server 127.0.0.1:9092  --topic temp
"""

from abc import ABC, abstractmethod
from functools import reduce
import statistics
import datetime
import random
import time
from kafka import KafkaProducer, KafkaConsumer
import threading

# Sistema es una clase Singleton, implementada para satisfacer el requisito de tener una única instancia del sistema.
class Sistema:
    _system = None

    def __init__(self):
        self.temps = []
        self.times = []
        self.handler_chain = EstadisticosHandler(MediaStrategy(), UmbralHandler(AumentoHandler()))

    @classmethod
    def get_instance(cls):
        if not cls._system:
            cls._system = cls()
        return cls._system

    def set_data(self, new_data):
        self.temps.append(new_data[1])
        self.times.append(new_data[0])
        self.handler_chain.handle(self)

    def get_data(self):
        return (self.times[-1], self.temps[-1])

# Strategy es una clase abstracta que define la interfaz para los diferentes algoritmos de cálculo de estadísticos.
class Strategy(ABC):
    @abstractmethod
    def calcular_estadisticos(self, temps):
        pass

# Estrategia para calcular la media de las temperaturas.
class MediaStrategy(Strategy):
    def calcular_estadisticos(self, temps):
        return reduce(lambda x, y: x + y, temps) / len(temps)
    
# Estrategia para calcular la desviación estándar de las temperaturas.
class DesviacionEstandarStrategy(Strategy):
    def calcular_estadisticos(self, temps):
        n = len(temps)
        calcular_desviacion_estandar = lambda temps: (sum((x - (sum(temps) / n)) ** 2 for x in temps) / n) ** 0.5
        return calcular_desviacion_estandar(temps)
    
# Estrategia para calcular los cuantiles de las temperaturas.
class CuantilStrategy(Strategy):
    def calcular_estadisticos(self, temps):
        n = len(temps)
        sorted_temps = sorted(temps)
        cuantiles = [sorted_temps[int(n * p)] for p in [0.25, 0.5, 0.75]]
        return cuantiles

# Handler es una clase base para la cadena de responsabilidad en el manejo de las temperaturas.
class Handler:
    def init(self, next_handler=None):
        self.next_handler = next_handler
    def handle(self):
        pass

# Handler que calcula y muestra diferentes estadísticos de las temperaturas.
class EstadisticosHandler(Handler):
    estrategias = [MediaStrategy(), DesviacionEstandarStrategy(), CuantilStrategy()]

    def __init__(self, strategy, next_handler=None):
        self.strategy = strategy
        self.next_handler = next_handler

    def handle(self, sistema):
        if len(sistema.temps) < 12:
            pass
        else:
            for estrategia in self.estrategias:
                self.strategy = estrategia
                estadistico = self.strategy.calcular_estadisticos(sistema.temps[-12:])
                if isinstance(estrategia, MediaStrategy):
                    print(f"Media calculada: {estadistico}")
                elif isinstance(estrategia, DesviacionEstandarStrategy):
                    print(f"Desviación estándar calculada: {estadistico}")
                elif isinstance(estrategia, CuantilStrategy):
                    print(f"Cuantil 0.25: {estadistico[0]}, Cuantil 0.5: {estadistico[1]}, Cuantil 0.75: {estadistico[2]}")
        if self.next_handler:
            self.next_handler.handle(sistema)

# Handler que verifica si la temperatura está por encima de un umbral.
class UmbralHandler(Handler):
    def __init__(self, next_handler=None):
        self.next_handler = next_handler

    def handle(self, sistema):
        check_umbral = lambda temp, umbral: temp > umbral
        if check_umbral(sistema.temps[-1], 30):
            print('Temperatura por encima del umbral')
        if self.next_handler:
            self.next_handler.handle(sistema)

# Handler que verifica si ha habido un aumento significativo en la temperatura.
class AumentoHandler(Handler):
    def __init__(self, next_handler=None):
        self.next_handler = next_handler

    def handle(self, sistema):
        check_aumento = lambda temp, temps_media: temp - temps_media > 10
        if len(sistema.temps) >= 7:
            last30secs = sistema.temps[-7:-1]
            media_last30secs = sum(last30secs) / len(last30secs)
            if check_aumento(sistema.temps[-1], media_last30secs):
                print('Aumento de temperatura mayor a 10 grados en los últimos 30 segundos')
        if self.next_handler:
            self.next_handler.handle(sistema)

# Clase abstracta para el Observer
class Observer(ABC):
    @abstractmethod
    def update(self, timestamp, temperature):
        pass

# Implementación del Productor de Kafka
class KafkaTemperatureProducer:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def produce_temperature(self):
        while True:
            try:
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                temperature = round(random.uniform(15, 35), 2)
                message = f"{timestamp},{temperature}"
                self.producer.send(self.topic, message.encode('utf-8'))
                time.sleep(5)  # Envía datos cada 5 segundos
            except Exception as e:
                print(f"Error en la producción de temperatura: {e}")

# Implementación del Consumidor de Kafka
class KafkaTemperatureConsumer(Observer):
    def __init__(self, bootstrap_servers, topic, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, group_id=group_id, auto_offset_reset='earliest')

    def update(self):
        sistema = Sistema.get_instance()  # Obtener la instancia del sistema
        for message in self.consumer:
            try:
                timestamp, temperature = message.value.decode('utf-8').split(',')
                sistema.set_data((timestamp, float(temperature)))  
                print(sistema.get_data())
                print("\n")
                time.sleep(5)
            except Exception as e:
                print(f"Error en el consumo de temperatura: {e}")

# Prueba de la implementación
if __name__ == "__main__":

    # Configuración de Kafka
    bootstrap_servers = 'localhost:9092'
    topic = 'temp'

    # Crear instancias del productor y el consumidor de Kafka
    producer = KafkaTemperatureProducer(bootstrap_servers, topic)
    consumer = KafkaTemperatureConsumer(bootstrap_servers, topic, "id")

    # Iniciar productor y consumidor en threads separados
    producer_thread = threading.Thread(target=producer.produce_temperature)
    consumer_thread = threading.Thread(target=consumer.update)

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()