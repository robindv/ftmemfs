import threading
import logging
import pickle
import redis
import socket
import traceback
import time
import os
from ftmemfs.tasks.fsck_coordination_tasks import *
import ftmemfs.leader.workflow
import ftmemfs.leader.scheduler
import SocketServer

def add_log_entry(leader, severity, message):
    log = {"time": time.time(), "ip": leader.ip, "severity": severity, "message": message }
    leader.zk.create("/logs/log", value=pickle.dumps(log), sequence=True)

class Leader(object):
    """The leader is the only instance that can make decisions"""

    def __init__(self, lm):

        self.ip = "10.149.0.{}".format(int(socket.gethostname()[4:]))
        self.zk                = lm.zk
        self.lock              = threading.Lock()
        self.workflow_lock     = threading.Lock()
        self.fscked_hosts      = []
        self.redises           = {}
        self.workflow          = None
        self.workflow_id       = None
        self.workers           = {}
        self.workflow_loading  = False
        self.workflow_started  = 0.0
        self.statsfile         = None
        self.statsinterval     = float(lm.config.get("general", "workflow_statistics_interval"))
        self.lm                = lm
        self.initial_leader    = lm.initial_leader
        self.scheduler         = ftmemfs.leader.scheduler.Scheduler(self)


        self.num_fscks         = 0


        self.watched_fscks = []
        self.fscks = {}

        # Load fscked_hosts
        self.zk.ChildrenWatch("/fsck", lambda c : lm.queue.put((20, FsckWatchTask(c))))


    def run(self):
        SchedulingTCPServer(self.scheduler).start()
        LeaderDebugTCPServer(self.scheduler).start()

        self.zk.ChildrenWatch("/workers", lambda w: self.scheduler.queue.put((5, "workers", w)))

        self.redis_watch()
        self.online_hosts_watch()
        self.suspicions_watch()
        self.workflow_watch()

        self.scheduler.server_thread.start()


    def set_workflow_state(self, workflow_id, state):

        add_log_entry(self, "INFO", "Workflow finished")

        sdict = pickle.loads(self.zk.get("/workflows/" + workflow_id)[0])
        sdict["finished"] = time.time()
        sdict["state"] = state

        try:
            self.zk.set("/workflows/"+ workflow_id, pickle.dumps(sdict))
        except:
            print ":-("


    def workflow_watch(self, info = None):
        # print "workflow watch!"

        self.workflow_lock.acquire()

        if self.scheduler.workflow_id != None:
            # print "Already executing {}".format(self.workflow_id)
            self.workflow_lock.release()
            return

        workflows = self.zk.get_children("/workflows", watch=self.workflow_watch)

        if len(workflows) == 0:
            # print "No workflows loaded"
            self.workflow_lock.release()
            return

        workflows.sort()

        for workflow_id in workflows:

            data, stat = self.zk.get("/workflows/" + workflow_id, watch=self.workflow_watch)
            if stat.dataLength == 0:
                print "Still uploading"
                continue

            workflow = pickle.loads(self.zk.get("/workflows/" + workflow_id)[0])

            if workflow["state"] == "finished" or workflow["state"] == "failed":
                continue

            if not self.workflow_loading:
                self.workflow_loading = True
                thread = WorkflowLoadThread(self, workflow, workflow_id)
                thread.start()

            break

        self.workflow_lock.release()


    def redis_watch(self, info = None):
        # print "Redis watch"

        self.lock.acquire()
        for server in self.zk.get_children("/redis", watch=self.redis_watch):
            info = self.zk.get("/redis/" + server)[0].split(',')
            self.redises[int(server[5:])] = {"class": info[0],"host": info[1], "port": info[2]}
        self.lock.release()


    def suspicions_watch(self, info = None):
        # print "suspicions watch"
        self.lock.acquire()

        suspicions = self.zk.get_children("/suspicions", watch=self.suspicions_watch)
        for suspicion in suspicions:
            suspicion = int(suspicion)

            if suspicion in self.fscked_hosts:
                continue

            r = self.redises[suspicion]

            if r["class"] == "-1" or self.zk.exists("/fsck/fsck{:010d}".format(suspicion)):
                print "Nice suspicion, already removed.."
                self.fscked_hosts.append(suspicion)
                try:
                    self.zk.delete("/suspicions/{}".format(suspicion))
                except:
                    print "Failed to delete /suspicions/{}".format(suspicion)
                    pass
                continue

            print "Testing {},{}".format(suspicion, r)

            try:
                test_redis = redis.StrictRedis(host=r["host"], port=r["port"])
                response = test_redis.ping()
                self.zk.delete("/suspicions/{}".format(suspicion))

            except redis.ConnectionError:
                self.fscked_hosts.append(suspicion)

                print "Creating fsck for {}".format(suspicion)
                self.scheduler.queue.put((5, "pause"))

                fsck_dict = {"state" : "started", "host" : r["host"], "port" : r["port"] , "redis_id" : suspicion, "participants" : self.get_other_redises_with_class(r) }

                transaction = self.zk.transaction()
                transaction.create("/fsck/fsck{:010d}".format(suspicion),pickle.dumps(fsck_dict))
                transaction.create("/fsck/fsck{:010d}/removed".format(suspicion))
                # TODO check?
                print transaction.commit()

            except:
                print "Something went wrong with deleting /suspicions/{}".format(suspicion)

        self.lock.release()

    def get_other_redises_with_class(self, our_redis):
        redises = []

        for r in self.zk.get_children("/redis"):
            redis = self.zk.get("/redis/" + r)[0].split(',')
            if redis[0] == our_redis["class"] and redis[1] != our_redis["host"]:
                redises.append(int(r[5:]))

        redises.sort()

        return redises

    def online_hosts_watch(self, info = None):
        self.lock.acquire()
        online_hosts = self.zk.get_children("/online_hosts", watch=self.online_hosts_watch)

        for key, r in self.redises.items():
            if r["host"] not in online_hosts and r["class"] != "-1":
                print "This host seem to be missing: {}".format(r)
                self.zk.ensure_path("/suspicions/{}".format(key))

        self.lock.release()


    def write_statsfile(self):

        if self.statsfile is None:
            return

        threading.Timer(self.statsinterval, self.write_statsfile).start()

        if self.scheduler.workflow is not None and self.statsfile is not None:
            self.statsfile.write("{};{}\n".format(time.time() - self.workflow_started, self.scheduler.num_todo()))
            self.statsfile.flush()


class WorkflowLoadThread(threading.Thread):

    def __init__(self, leader, workflow, workflow_id):
        threading.Thread.__init__(self)
        self.leader = leader
        self.workflow    = workflow
        self.workflow_id = workflow_id

    def run(self):

        add_log_entry(self.leader, "INFO", "Loading workflow " + self.workflow_id)
        recover = self.workflow["state"] == "running"

        workflow_obj = ftmemfs.leader.workflow.Workflow(self.leader.zk, self.workflow_id)

        if self.workflow["state"] == "submitted":
            self.workflow["started"] = time.time()

        self.leader.workflow_started = self.workflow["started"]

        if self.leader.statsfile is None:
            self.leader.statsfile = open("{}/stats_{}_{}_{}".format(os.getenv("HOME"), int(time.time()), socket.gethostname(), self.workflow_id), 'w')

        self.workflow["state"] = "running"
        self.leader.zk.set("/workflows/" + self.workflow_id, pickle.dumps(self.workflow))

        add_log_entry(self.leader, "INFO", "Start the execution of workflow {} (pc = {})".format(self.workflow_id, 0))
        self.leader.scheduler.queue.put( (10, "set_workflow", self.workflow_id, workflow_obj, recover))



class SchedulingTCPServer(threading.Thread):

    def __init__(self, scheduler):
        threading.Thread.__init__(self)
        self.scheduler = scheduler

    def run(self):
        server = SocketServer.TCPServer(("0.0.0.0", 2001), SchedulerTCP, bind_and_activate=False)
        server.RequestHandlerClass.set_scheduler(self.scheduler)
        server.allow_reuse_address = True
        server.server_bind()
        server.server_activate()
        server.serve_forever()

class LeaderDebugTCPServer(threading.Thread):

    def __init__(self, scheduler):
        threading.Thread.__init__(self)
        self.scheduler = scheduler

    def run(self):
        server = SocketServer.TCPServer(("0.0.0.0", 2002), SchedulerListsTCP, bind_and_activate=False)
        server.RequestHandlerClass.set_scheduler(self.scheduler)
        server.allow_reuse_address = True
        server.server_bind()
        server.server_activate()
        server.serve_forever()

class SchedulerTCP(SocketServer.BaseRequestHandler):
    @staticmethod
    def set_scheduler(scheduler):
        SchedulerTCP.scheduler = scheduler

    def handle(self):
        try:
            self.request.sendall(pickle.dumps(SchedulerTCP.scheduler.workers))
        except:
            self.request.sendall("")

class SchedulerListsTCP(SocketServer.BaseRequestHandler):
    @staticmethod
    def set_scheduler(scheduler):
        SchedulerListsTCP.scheduler = scheduler

    def handle(self):
        try:
            self.request.sendall(pickle.dumps(SchedulerListsTCP.scheduler.debug_dict()))
        except:
            self.request.sendall("")
