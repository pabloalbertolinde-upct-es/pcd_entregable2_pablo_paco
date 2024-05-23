from consumer import Sistema, Strategy, MediaStrategy, DesviacionEstandarStrategy, CuantilStrategy, KafkaTemperatureProducer
import random

def test_singleton():
    sistema = Sistema.get_instance()
    sistema2 = Sistema.get_instance()
    assert sistema is sistema2

def test_set_data():
    sistema = Sistema.get_instance()
    timestamp = '2021-09-01 12:00:00'
    temperature = 25.0
    sistema.set_data((timestamp, temperature))
    assert sistema.get_data() == (timestamp, temperature)

def test_media_strategy():
    media = MediaStrategy()
    temps = [random.randint(0, 10) for _ in range(5)]
    assert media.calcular_estadisticos(temps) == sum(temps) / len(temps)

def test_desviacion_estandar_strategy():
    desviacion = DesviacionEstandarStrategy()
    temps = [random.randint(0, 10) for _ in range(5)]
    n = len(temps)
    calcular_desviacion_estandar = lambda temps: (sum((x - (sum(temps) / n)) ** 2 for x in temps) / n) ** 0.5
    assert desviacion.calcular_estadisticos(temps) == calcular_desviacion_estandar(temps)

def test_cuantil_strategy():
    cuantil = CuantilStrategy()
    temps = [random.randint(0, 10) for _ in range(5)]
    n = len(temps)
    sorted_temps = sorted(temps)
    cuantiles = [sorted_temps[int(n * p)] for p in [0.25, 0.5, 0.75]]
    assert cuantil.calcular_estadisticos(temps) == cuantiles

