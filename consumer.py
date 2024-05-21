from abc import ABC, abstractmethod
from functools import reduce
import statistics
import datetime
import random
import time
from kafka import KafkaProducer, KafkaConsumer
import threading

# Sistema es una clase Singleton, implementada para satisfacer R1.
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

class MediaStrategy(Strategy):
    def calcular_estadisticos(self, temps):
        return reduce(lambda x, y: x + y, temps) / len(temps)
    
class DesviacionEstandarStrategy(Strategy):
    def calcular_estadisticos(self, temps):
        n = len(temps)
        calcular_desviacion_estandar = lambda temps: (sum((x - (sum(temps) / n)) ** 2 for x in temps) / n) ** 0.5
        return calcular_desviacion_estandar(temps)
    
class CuantilStrategy(Strategy):
    def calcular_estadisticos(self, temps):
        n = len(temps)
        sorted_temps = sorted(temps)
        cuantiles = [sorted_temps[int(n * p)] for p in [0.25, 0.5, 0.75]]
        return cuantiles

# EstadisticosHandler se sirve de las estrategias definidas para el cálculo de estadísticos.
class Handler:
    def init(self, next_handler=None):
        self.next_handler = next_handler
    def handle(self):
        pass

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

class UmbralHandler(Handler):
    def __init__(self, next_handler=None):
        self.next_handler = next_handler

    def handle(self, sistema):
        check_umbral = lambda temp, umbral: temp > umbral
        if check_umbral(sistema.temps[-1], 30):
            print('Temperatura por encima del umbral')
        if self.next_handler:
            self.next_handler.handle(sistema)

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
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            temperature = round(random.uniform(15, 35), 2)
            message = f"{timestamp},{temperature}"
            self.producer.send(self.topic, message.encode('utf-8'))
            time.sleep(5)  # Envía datos cada 5 segundos

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
            timestamp, temperature = message.value.decode('utf-8').split(',')
            sistema.set_data((timestamp, float(temperature)))  
            print(sistema.get_data())
            print("\n")
            time.sleep(5) 

if __name__ == "__main__":

    sistema = Sistema.get_instance()
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for i in range(20):
        t = round(random.uniform(15, 35),2)
        tupla = (timestamp, t)
        sistema.set_data(tupla)
        print(sistema.get_data())
        print("\n")