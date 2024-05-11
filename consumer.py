from abc import ABC, abstractmethod
import statistics
import datetime
import random

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
# Se implementa aquí el patrón Strategy para calcular diferentes estadísticos de la temperatura durante los últimos 60 segundos (R4).

class Strategy(ABC):
    @abstractmethod
    def calcular_estadisticos(self, temps):
        pass

class MediaStrategy(Strategy):
    def calcular_estadisticos(self, temps):
        return statistics.mean(temps)

class DesviacionEstandarStrategy(Strategy):
    def calcular_estadisticos(self, temps):
        return statistics.stdev(temps)

# EstadisticosHandler se sirve de las estrategias definidas para el cálculo de estadísticos.
# Se implementa aquí el patrón Chain of Responsibility para satisfacer R3.

class EstadisticosHandler:
    estrategias = [MediaStrategy(), DesviacionEstandarStrategy()]
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
        if self.next_handler:
            self.next_handler.handle(sistema)

class UmbralHandler:
    def __init__(self, next_handler=None):
        self.next_handler = next_handler
    def handle(self, sistema):
        if sistema.temps[-1] > 30:
            print('Temperatura por encima del umbral')

        if self.next_handler:
            self.next_handler.handle(sistema)

class AumentoHandler:
    def __init__(self, next_handler=None):
        self.next_handler = next_handler
    def handle(self, sistema):
        if len(sistema.temps) < 7:
            pass
        else:
            if sistema.temps[-1] - sistema.temps[-7] > 10:
                print('Aumento de temperatura mayor a 10 grados en los últimos 30 segundos')   
        if self.next_handler:
            self.next_handler.handle(sistema)

# Pruebas de la implementación

if __name__ == "__main__":

    sistema = Sistema.get_instance()
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for i in range(20):
        t = round(random.uniform(15, 35),2)
        tupla = (timestamp, t)
        sistema.set_data(tupla)
        print(sistema.get_data())