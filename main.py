import re
import threading
from sys import argv
import time
import kafka
import socket


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        print("INICIO READER")
        global a
        while not self.stop_event.is_set():
            print("prod")
            a += 1
            time.sleep(1)


class Reader(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        print("INICIO READER")
        global a
        while not self.stop_event.is_set():
            print(f"lee : {a}")
            time.sleep(1)


def hilo_modifica():
    global a
    while True:
        a += 1
        time.sleep(1)


def hilo_lee():
    while True:
        print("lee: ", a)
        time.sleep(1)


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
    if not filtra(argv):
        print("ERROR: Argumentos incorrectos")
        print("Usage: sensor.py <ip_servidor:puerto> <id> ")
        print("Example: sensor.py 192.168.56.33:9092 02")
        exit()

    print("INICIO MAIN")
    a = 0
    hilos = [Producer(), Reader()]
    for i in hilos:
        i.start()
    time.sleep(5)
    for i in hilos:
        i.stop()
    for i in hilos:
        i.join()

    """
    t1 = threading.Thread(target=hilo_modifica)
    t2 = threading.Thread(target=hilo_lee)
    t1.start()
    t2.start()
    time.sleep(15)
    print("FIN")
    t1.join()
    t2.join()
    """
    print("FIN MAIN")
