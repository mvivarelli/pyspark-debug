import os
import debugpy
import zmq
import remote_debug

os.environ['SPARK_LOCAL_IP'] = "172.18.0.1"
srvname = "172.18.0.1"
srvport = 9999
dbgport = 5680


def mysplit(x):
    tmp = os.getenv('HOSTNAME') + " mysplit " + x
    remote_debug.socket.send(tmp.encode('utf-8'))
    return x.split(" ")


def mymap(word):
    tmp = os.getenv('HOSTNAME') + " mymap " + word
    remote_debug.socket.send(tmp.encode('utf-8'))
    return word, 1


def myreducebykey(a, b):
    tmp = os.getenv('HOSTNAME') + " myreducebykey " + str(a) + " " + str(b)
    remote_debug.socket.send(tmp.encode('utf-8'))
    return a + b
