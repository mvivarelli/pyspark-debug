r"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 Usage: network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit examples/src/main/python/streaming/network_wordcount.py localhost 9999`
"""
import threading
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
import os
import sys
from logbook import Logger, StreamHandler
import zmq
import remote_code_to_debug

os.environ['SPARK_LOCAL_IP'] = "172.18.0.1"
srvname = "172.18.0.1"
srvport = 9999
dbgport = 5680


def _log(dummy):
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.bind("tcp://0.0.0.0:5555")

    while True:
        #  Wait for next request from client
        message = socket.recv().decode('UTF-8')
        log.info(f'{message}')
        # time.sleep(0.01)


if __name__ == "__main__":
    """
    if len(sys.argv) != 3:
        print("Usage: network_wordcount.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)
    """

    my_handler = StreamHandler(sys.stdout)
    my_handler.push_application()
    log = Logger('Logbook')
    log.info('Hello, World!')

    tr = threading.Thread(target=_log, args=(1,), daemon=True)
    tr.start()

    conf = SparkConf()
    conf.setMaster("spark://spark-driver:7077")
    conf.setAppName("PythonStreamingNetworkWordCount")
    conf.set("spark.python.daemon.module", "remote_debug")
    conf.set("spark.files", "remote_debug.py,remote_code_to_debug.py")

    sc = SparkContext(conf=conf)

    # sc = SparkContext(master="spark://spark-driver:7077", appName="PythonStreamingNetworkWordCount")
    ssc = StreamingContext(sc, 60)

    lines = ssc.socketTextStream(srvname, srvport)
    """
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a + b)
    """
    counts = lines.flatMap(remote_code_to_debug.mysplit)\
                  .map(remote_code_to_debug.mymap)\
                  .reduceByKey(remote_code_to_debug.myreducebykey)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()

