from kazoo.client import KazooClient
import kazoo.exceptions
import threading
import os
import sys
import signal
import socket
import time
import ConfigParser
import redis
import logging
import threading
import ftmemfs.etc.config
import pickle
import traceback
import Queue

from ftmemfs.utils.hrw import *
from ftmemfs.utils.zookeeper_utils import *

from ftmemfs.tasks.redis_tasks import RedisUsageServerTask
from ftmemfs.tasks.general_tasks import *
from ftmemfs.tasks.fsck_tasks import FsckEnqueueTask

from ftmemfs.leader.leader import Leader

def file_content(file):
    f = open(file, 'r')
    c = f.read()
    f.close()
    return c.strip()

class ZkStateTask(FtmemfsTask):
    def __init__(self, state):
        self.state = state

    def __repr__(self):
        return "ZkStateTask({})".format(self.state)

    def execute(self, lm):
        print "Current state is: {}".format(self.state)

        if self.state == "CONNECTED":
            msg =  "Localmanager {} connected, ZK-host: {}".format(lm.ip, lm.zk.command("envi").split("\n")[2].split("=")[1].split(".")[0])
            print msg
            lm.add_log_entry("INFO", msg)

            if lm.zk_state == "SUSPENDED" and lm.potential_leader:
                print "We could become the leader now, check!"
                lm.queue.put((10, CheckLeaderTask(lm.zk.get("/leaders")[0])))

        if self.state == "LOST":
            print "Connection lost, stop!"

            sys.exit(0)

        lm.zk_state = self.state

class LocalManager(object):

    def signal_handler(self, signum, frame):
        '''Signal handler to make sure ctrl-c stops everything'''
        print "Got ctrl+c, stopping"

        self.zk.stop()

        if self.redis_host:
            self.redis_usage_server.stop()
            self.redis_usage_server.join()
        sys.exit(0)

    def __init__(self):
        '''Initialise localmanager'''

        self.zk_state = None
        self.ldr      = None


        # Perform the following steps:
        # - Register signal handler
        # - Read configuration and determine IP address
        # - Connect to zookeeper
        # - Check whether this node is already running or not in the services
        # - Create tasks according to the services
        # - Start the queue listener
        #
        signal.signal(signal.SIGINT, self.signal_handler)

        self.queue = Queue.PriorityQueue()

        self.config = ConfigParser.ConfigParser()
        self.config.read("{}/ftmemfs/config.ini".format(os.getenv("HOME")))
        self.config.read("{}/ftmemfs/config_platform.ini".format(os.getenv("HOME")))

        # TODO retrieve from interface
        self.ip = self.config.get("general","ip_prefix")
        self.ip += "{}".format(int(socket.gethostname()[4:]))

        # When locally running zookeeper, only connect to local zk
        zk_hosts = sys.argv[1]
        if "{}:2181".format(self.ip) in zk_hosts.split(','):
            zk_hosts = "{}:2181".format(self.ip)

        self.zk = KazooClient(zk_hosts, timeout = 2.0, connection_retry=kazoo.retry.KazooRetry(max_tries=5, max_delay=3600))
        #self.zk.add_listener(self.kazoo_listener)
        self.zk.add_listener(lambda s: self.queue.put((5, ZkStateTask(s))))
        self.zk.start()

        # self.fsck_lock   = threading.Lock()

        self.fscks_handled = []

        self.leader_lock = threading.Lock()
        self.my_leader_path = None
        self.redises = {}

        if not self.zk.exists("/services/{}".format(self.ip)):
            self.add_log_entry("ERROR", "Node {} not in zookeeper database".format(self.ip))
            raise Exception("Node {} not in zookeeper database".format(self.ip))

        if self.zk.exists("/online_hosts/{}".format(self.ip)):
            self.add_log_entry("ERROR", "Localmanager already running")
            raise Exception("Localmanager already running?!")

        self.zk.create("/online_hosts/{}".format(self.ip), ephemeral=True)
        self.potential_leader = self.zk.exists("/services/{}/zookeeper".format(self.ip)) is not None
        self.redis_host       = self.zk.exists("/services/{}/redis".format(self.ip)) is not None

        self.initial_leader = False

        # Leaders initially do not redis
        if self.potential_leader and is_zookeeper_leader(self.zk):
            self.initial_leader = True

            if self.redis_host:
                self.zk.delete("/services/{}/redis".format(self.ip))
                self.redis_host = False

        # TODO: check redis_host...
        self.redis_seq = None
        if self.redis_host or self.get_redis_seq() != None:
            self.redis_seq = self.check_redis(self.ip)
            print "Redis self seq: {}".format(self.redis_seq)
            self.queue.put((5, RedisUsageServerTask()))

        self.worker    = None
        self.is_worker = False
        self.leader    = None
        self.legacy    = self.zk.exists("/legacy")

        if self.zk.exists("/services/{}/worker".format(self.ip)):

            self.zk.create("/workers/{}".format(self.ip), "", ephemeral=True)
            if self.legacy:
                time.sleep(30)
                os.system("{}/mount_legacy.sh {}".format(self.config.get("general","scripts_dir"),self.config.get("worker","mount")))
            else:
                os.system("{}/mount_ftmemfs.sh {} /workers/{} {}".format(self.config.get("general","scripts_dir"),self.config.get("worker","mount"),self.ip, sys.argv[1]))
            os.system("mkdir -p " + ftmemfs.etc.config.LOGDIR)
            self.is_worker = True

        self.zk.ChildrenWatch("/leaders", lambda s : self.queue.put((10,CheckLeaderTask(s))))

        if not self.legacy and self.redis_seq is not None:
            self.zk.ChildrenWatch("/fsck", lambda c : self.queue.put((50, FsckEnqueueTask(c))))


    def queue_worker(self):
        '''Queue worker'''

        while True:
            task = self.queue.get()[1]
            print "Got from queue: {}".format(task)
            task.execute(self)
            self.queue.task_done()

    def add_log_entry(self, severity, message):
        '''Add a log to ZooKeeper for debugging/info purposes'''
        log = {"time": time.time(), "ip": self.ip, "severity": severity, "message": message }
        try:
            self.zk.create("/logs/log", value=pickle.dumps(log), sequence=True)
        except:
            print "Could not create log entry"

    def run(self):

        self.queue_t = threading.Thread(target = self.queue_worker)
        self.queue_t.daemon = True
        self.queue_t.start()

        while True:
             time.sleep(1)

             if self.redis_host and not self.is_redis_running():
                 self.add_log_entry("ERROR", "Redis unavailable, stopping localmanager")
                 break

            #  if self.worker:
            #      if not os.path.ismount('/local/rdevries/ftmemfs'):
            #          print "FTMEMFS crashed :-("
            #          break

        print "Stopping"

        try:
            self.zk.stop()
            if self.worker:
                self.worker.shutdown()
                self.worker.join()

            if self.redis_host:
                self.redis_usage_server.stop()
                self.redis_usage_server.join()
        except:
            pass

        sys.exit(0)

    def start_leader(self):
        '''This node is the leader, stop the worker node and start the
        coordination thread'''
        self.add_log_entry("INFO", "Leader")

        if self.is_worker:
            self.is_worker = False
            # Unmount fuse!
            self.zk.delete("/workers/{}".format(self.ip))

        thr = None

        if self.worker != None:
            print "Stopping worker ZMQ!"
            thr = self.worker.sched_client_thread
            thr.stop()

            self.worker.sched_client_thread = None
            time.sleep(2)

        if not self.initial_leader:
            print "Block FUSE!"
            self.zk.ensure_path("/fuse_block")

        self.zk.create("/services/{}/leader".format(self.ip), "", ephemeral=True)

        leader = Leader(self)
        self.ldr = leader

        leader.run()

        print "We should stop the current worker!"

        if self.worker != None:
            thr.join()
            self.worker.shutdown()
            self.worker.join()

    def is_redis_running(self):

        r = redis.StrictRedis()
        try:
            response = r.ping()
        except redis.ConnectionError:
            return False
        return True

    def get_redis_seq(self):
        '''Retrieve the redis sequence number from the redis server running
        locally.'''
        r = redis.StrictRedis()
        try:
            return r.get("ftmemfs_redis_id")
        except redis.ConnectionError:
            pass
        return None

    def set_redis_seq(self,id):
        '''Update the redis sequence number on the redis server running
        locally.'''
        r = redis.StrictRedis()
        try:
            r.set("ftmemfs_redis_id","{}".format(id))
        except redis.ConnectionError:
            pass

    def check_redis(self, ip):
        '''Check whether redis is running and start redis if it is not.'''
        # print "Check whether redis is running"

        if self.is_redis_running():
            # print "Redis already running"

            # This should not be necessary, testing purposes only
            for c in self.zk.get_children("/redis"):
                d = self.zk.get("/redis/" + c)[0]

                if d.split(',')[1] == self.ip and d.split(',')[0] != -1:
                    return int(c[5:])

            return
        # else:
            # print "Redis not already running"

        port = self.config.get("redis", "port")

        os.system("{}/start_redis.sh {} {} {} {}".format(
            self.config.get("general", "scripts_dir"),
            port,
            self.config.get("redis", "dir"),
            self.config.get("redis", "bin"),
            self.config.get("redis", "config_base")
            ))

        nodeset,_ = self.zk.get("/services/{}/redis".format(ip))
        seq = self.zk.create("/redis/redis", "{},{},{}".format(nodeset, ip, port), sequence=True)
        self.set_redis_seq(int(seq[12:]))

        return int(seq[12:])

if __name__ == "__main__":
    logging.basicConfig()

    if len(sys.argv) < 2:
        print "Invalid number of arguments"
        sys.exit()

    try:
        lm = LocalManager()
        lm.run()
    except Exception as e:
        print "Something went wrong: {}".format(e)
        raise
