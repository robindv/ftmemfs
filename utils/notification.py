from threading import Lock
import time
import traceback

import zmq

#import weasel.etc.config as config
from ftmemfs.utils.threads import *
#from ftmemfs.utils.logger import WeaselLogger

PROTOCOL_HEADERS = {'CLIENT': "\x01",
                    'WORKER': "\x02",
                    'RSMNG': "\x04",
                    'SHUTDOWNMEM': "\x06",
                    'SHUTDOWN': '\x08'
                    }

''' ZeroMq-based implementation of communication '''


def get_context():
    context = zmq.Context()
    return context


def tmp_socket(context, socket_type, identity, broker):
    """Helper function that returns a new configured socket
    connected to the server"""
    # print "broker {}".format(broker)
    tmp = context.socket(socket_type)
    if socket_type == zmq.REQ:
        tmp.setsockopt(zmq.IDENTITY, identity)
        tmp.connect("tcp://" + broker)
    elif socket_type == zmq.DEALER:
        tmp.setsockopt(zmq.IDENTITY, identity)
        if broker.split(":")[0] == "*":
            tmp.bind("tcp://" + broker)
        else:
            tmp.connect("tcp://" + broker)
    elif socket_type == zmq.ROUTER:
        tmp.bind("tcp://" + broker)
        tmp.setsockopt(zmq.ROUTER_MANDATORY, 1)
    return tmp


def make_request(identity, header, message):
    request = [identity,
               header,
               message]
    return request


def send_message(socket, message):
    socket.send_multipart(message)


def get_reply(socket):
    return socket.recv_multipart()


#log_name = 'notification'
#notification_logger = WeaselLogger(
#    log_name,
#    config.LOGDIR +
#    '/' +
#    log_name +
#    '.log')


class ZmqConnectionThread(Thread):
    def __init__(self, identity, socket_type, address, msg_process_callback):
        Thread.__init__(self)
        self.running = True
        self.requests = []
        self.lock = Lock()
        self.msg_process_callback = msg_process_callback
        self.context = get_context()
        self.frontend = tmp_socket(self.context, socket_type,
                                   identity, address)
        self.frontend.setsockopt(zmq.LINGER, 120000)
        self.poll = zmq.Poller()
        self.backup_socket = None
        if address.split(':')[0] != "*":
            self.backup_socket = tmp_socket(self.context, zmq.DEALER,
                                            identity, "*:" + address.split(':')[1])
            self.poll.register(self.backup_socket, zmq.POLLIN)
        self.poll.register(self.frontend, zmq.POLLIN)
        self.tqueue = ThreadPool(1, None)

    def change_addr(self, identity, socket_type, address):
        self.frontend = tmp_socket(self.context, socket_type,
                                   identity, address)
        self.frontend.setsockopt(zmq.LINGER, 120000)

        self.poll.register(self.frontend, zmq.POLLIN)

    def put_request_in_queue(self, request):
        self.lock.acquire()
        self.requests.append(request)
        self.lock.release()

    def get_request_from_queue(self):
        request = None
        self.lock.acquire()
        request = self.requests.pop()
        self.lock.release()
        return request

    def get_queue_len(self):
        qlen = 0
        self.lock.acquire()
        qlen = len(self.requests)
        self.lock.release()
        return qlen

    def stop(self):
        self.running = False

    def run(self):
        self.tqueue.start()

        failures = {}

        while self.running:
            try:
                ''' wait an interval max equal with the heartbeat period '''
                sockets = dict(self.poll.poll(200))
                for sock in sockets:
                    if sockets[sock] == zmq.POLLIN:
                        frames = sock.recv_multipart()
                        self.tqueue.add_task(self.msg_process_callback, frames)
                        # self.msg_process_callback(frames)
            except:
                traceback.print_exc()
                time.sleep(1)
            ''' here I send the requests to workers... '''
            tmp_queue = []
            while self.get_queue_len() > 0:
                request = self.get_request_from_queue()
                tmp_request = request[:]
                tmp_request.insert(1, '')
                try:
                    send_message(self.frontend, tmp_request)
                except Exception as e:

                    if e.strerror == "No route to host":
                        if request[0] not in failures:
                            failures[request[0]] = 1
                        else:
                            failures[request[0]] += 1

                        if failures[request[0]] < 5:
                            tmp_queue.append(request)
                        else:
                            print "Cannot reach {}, dropping message".format(request[0])
                    else:
                        traceback.print_exc()
                        tmp_queue.append(request)
            for req in tmp_queue:
                self.put_request_in_queue(req)
        if self.backup_socket != None:
            self.backup_socket.close()
        self.frontend.close()
        self.context.term()
        self.tqueue.wait_completion()
        return
