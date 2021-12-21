"""
    Servidor de tiempos de espera
    *   recibe señales de los sensores por KAFA
    *   envia a peticion los timepos calculados al FWQ_Engine
    tendra dos threads:
    uno que recibe las informacion de KAFA y calcula el timepo de espera
    otro que recibe peticiones y envia los timpos calculados

    Habra una variable globl que compartan con los timpos de todas las
    atracciones

    topic de sensores: 'atracciones'

    TODO controlador de señal SIGINT para detener el servicio como en FWQ_Sensor
    TODO crear Threads conforme se dockes de forma concurrente ¿?
    TODO cread comunicacion con FWQ Engine

    TODO guardar los timepos en un fichero en lugar de en memoria compartida :)
    TODO comunicacion con FWQ_Engine
"""
import re
import threading
import traceback
from sys import argv
import time
import kafka
import socket
import signal

from kafka import KafkaConsumer


def tiempo(valor: int) -> int:
    """
    Calcula el timepo de espera segun el numero de personas en la cola
    :param valor: numero de personas en cola
    """
    return valor


class LectorSensores(threading.Thread):
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def consumir(self, consumer):
        #global TIEMPOS_ESPERA
        TIEMPOS_ESPERA = {}
        while not self.stop_event.is_set():
            for msg in consumer:
                atraccion = msg.value.split(b" ")[0]
                valor = int(msg.value.split(b" ")[1])
                TIEMPOS_ESPERA[atraccion] = tiempo(valor)
                print("leidos: ",TIEMPOS_ESPERA)
                self.escribe_tiempos(TIEMPOS_ESPERA)

    def escribe_tiempos(self, TIEMPOS_ESPERA):
        f = open("./timepos.dat", "+w")
        f.write(str(TIEMPOS_ESPERA))
        f.close()

    def run(self):
        print("INICIO LectorSensores")
        try:
            consumer = KafkaConsumer(bootstrap_servers=f'{self.ip}:{self.port}',
                                     auto_offset_reset='latest',
                                     consumer_timeout_ms=500)
            consumer.subscribe(['atracciones'])
            self.consumir(consumer)
        except Exception as e:
            print("ERROR EN LectorSensores :", e)
            traceback.print_exc()
        finally:
            if 'consumer' in locals():
                consumer.close()
            print("FIN LectorSensores")


class AtiendeEngine(threading.Thread):
    def __init__(self, ip, port):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.ip = ip
        self.port = port

    def stop(self):
        self.stop_event.set()

    def lee(self) -> dict:
        TIEMPOS = '{}'
        f = open("./timepos.dat", "r")
        try:
            TIEMPOS = eval(f.read())
        except Exception as e:
            print("ERROR en lectura: ",e)

        f.close()
        return TIEMPOS

    def run(self):
        HEADER = 10
        print("INICIO AtiendeEngine")
        servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servidor.bind((self.ip, self.port))
        servidor.listen()
        while not self.stop_event.is_set():
            print("AAAAAAAAAAAAA")
            conn, addr = servidor.accept()
            print("iniciada conexion")
            mensaje = str(self.lee())
            size = str(len(mensaje)) + ' '*(HEADER - len(str(len(mensaje))))
            print(repr(size))
            conn.send(size.encode())
            print(f"envia: {mensaje}")
            conn.send(mensaje.encode())
            print("enviado")
            conn.close()


def filtra(args: list) -> bool:
    """
    Indica si el formato de los argumentos es el correcto
    :param args: Argumentos del programa
    """
    if len(args) != 3:
        print("Numero incorrecto de argumentos")
        return False

    regex_0 = '^[0-9]{1,5}$'
    if not re.match(regex_0, args[1]):
        print("Puerto de Escucha incorrecto")
        return False

    regex_1 = '^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:[0-9]{1,5}$'
    regex_2 = '^\S+:[0-9]{1,5}$'
    if not (re.match(regex_1, args[2]) or re.match(regex_2, args[2])):
        print("Direccion incorrecta")
        return False
    return True


if __name__ == '__main__':
    my_ip = socket.gethostbyname(socket.gethostname())
    print('my_ip: ', my_ip)
    my_ip = "localhost"
    my_ip = '127.0.0.1'
    if not filtra(argv):
        print("ERROR: Argumentos incorrectos")
        print("Usage: FWQ_WitingTimeServer.py <puerto_escucha> <ip_kafka:puerto> ")
        print("Example: FWQ_WitingTimeServer.py 6060 192.168.56.33:9092")
        exit()

    ip_kafka = argv[2].split(":")[0]
    port_kafka = int(argv[2].split(":")[1])
    port_escucha = int(argv[1])

    print("INICIO MAIN")
    a = 0
    TIEMPOS_ESPERA = {}  # { atraccion: timepo, ... }

    hilos = [
        LectorSensores(ip_kafka, port_kafka),
        AtiendeEngine(my_ip,port_escucha)
    ]

    for i in hilos:
        i.setDaemon(True)

    for i in hilos:
        i.start()

    time.sleep(10000)

    for i in hilos:
        i.stop()

    for i in hilos:
        i.join()

    print("FIN MAIN")
