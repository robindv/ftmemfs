import SocketServer
import threading
import subprocess

from ftmemfs.tasks.general_tasks import FtmemfsTask

class RedisUsageServerTask(FtmemfsTask):

    def __repr__(self):
        return "RedisUsageServerTask"

    def execute(self, lm):
        print "Redis usage server started on port 2000"
        lm.redis_usage_server = RedisUsageTCPServer()
        lm.redis_usage_server.start()

class RedisUsage(SocketServer.BaseRequestHandler):

    def handle(self):
        try:
            self.request.sendall(subprocess.Popen("redis-cli info | grep used_memory:", shell=True,stdout=subprocess.PIPE).communicate()[0].strip()[12:]);
        except:
            print "whoops"
            self.request.sendall("")

class RedisUsageTCPServer(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.server = None

    def run(self):
        self.server = SocketServer.TCPServer(("0.0.0.0", 2000), RedisUsage, bind_and_activate=False)
        self.server.allow_reuse_address = True
        self.server.server_bind()
        self.server.server_activate()
        self.server.serve_forever()

    def stop(self):
        self.server.shutdown()
