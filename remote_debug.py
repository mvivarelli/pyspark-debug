from pyspark import daemon, worker
import debugpy
import zmq

dbgport = 5680
socket = ""


def remote_debug_wrapped(*args, **kwargs):
    global socket
    # debug enable 
    try:
        debugpy.listen(("0.0.0.0", dbgport))
    except:
        pass
    # debugpy.wait_for_client()
    # debugpy.breakpoint()

    # logging pipe opening
    try:
        context = zmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.connect("tcp://172.18.0.1:5555")    
    except:
        pass

    worker.main(*args, **kwargs)


daemon.worker_main = remote_debug_wrapped

if __name__ == '__main__':
    daemon.manager()
