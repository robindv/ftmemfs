import Queue as PQ
import threading
import traceback
import pickle
from ftmemfs.utils.notification import *
import ftmemfs.etc.config as config # TODO obsolete by .ini file

def add_log_entry(leader, severity, message):
    log = {"time": time.time(), "ip": leader.ip, "severity": severity, "message": message }
    leader.zk.create("/logs/log", value=pickle.dumps(log), sequence=True)

class Scheduler(object):
    """The scheduler distributes tasks amongst nodes"""

    def __init__(self, leader):
        self.leader = leader
        self.queue = PQ.PriorityQueue()
        self.workflow = None
        self.workflow_id = None

        # We are not the leader that started this..
        self.recover = not leader.initial_leader

        self.queue_t = threading.Thread(target = self.queue_worker)
        self.queue_t.daemon = True
        self.queue_t.start()

        self.workers = {}
        self.paused = self.recover
        self.num_running_info = 0
        self.workflow = None
        self.workers_initialised = False

        self.disabled_workers = [leader.lm.ip]

        self.waiting      = set()
        self.runnable     = set()
        self.running      = set()
        self.not_running  = set()
        self.finished     = set()
        self.check        = set()
        self.pause_completion_check = set()

        self.failures     = {}


        self.prefetch_const = 2
        self.num_threads = int(leader.lm.config.get("worker", "nr_colocated_tasks"))

        self.server_thread = ZmqConnectionThread(
            'resourcemng',
            zmq.ROUTER,
            "*:" + str(config.ZMQ_SCHEDULER_PORT),
            self.scheduler_msg_callback)

    def num_todo(self):
        return len(self.waiting) + len(self.runnable)

    def queue_worker(self):
        '''Queue worker'''

        while True:
            # print "Queue size: {}".format(self.queue.qsize())

            task = self.queue.get()

            # print "Got from sched-queue: {}".format(task)
            if task[1] == "pause":
                print "Paused the scheduler"
                self.paused = True

                self.leader.zk.ensure_path("/fuse_block")

                # Copy the files to check
                for task in self.running:
                    if task not in self.check:
                        self.check.add(task)

            elif task[1] == "deleted_files":
                if self.workflow:
                    self.workflow.process_lost_files(task[2], self.finished)

            elif task[1] == "resume":
                self.do_resume()

            elif task[1] == "workers":
                self.do_workers(task[2])

            elif task[1] == "failed_tasks":
                if self.workflow or self.recover:
                    self.do_failed_tasks(task[2], task[3])

            elif task[1] == "completed_tasks":

                if self.workflow or self.recover:
                    self.do_completed_tasks(task[2], task[3])

            elif task[1] == "currently_running_or_waiting":

                self.do_currently_running_or_waiting(task[2], task[3])

            elif task[1] == "scheduling_request":
                if self.workflow and not self.paused:
                    self.do_scheduling_request(task[2])

            elif task[1] == "set_workflow":

                self.leader.workflow_lock.acquire()
                self.workflow_id = task[2]
                self.workflow = task[3]

                self.waiting      = set()
                self.runnable     = set()
                if not self.recover:
                    self.running  = set()
                    self.pause_completion_check = set()
                self.not_running  = set()
                self.finished     = set()
                self.check        = set()


                if task[4]:
                    self.finished = self.workflow.recover()

                self.runnable, self.waiting = self.workflow.det_runnable_tasks(self.finished)

                # Check with leader failure resuming stuff
                for task in self.runnable:
                    if task not in self.running:
                        self.not_running.add(task)

                self.leader.workflow_loading = False
                self.leader.write_statsfile()
                self.leader.workflow_lock.release()

            elif task[1] == "check_finished":
                self.do_check_finished()

            else:
                print "uh?!"

            self.queue.task_done()


    def do_resume(self):
        print "Resume?!"

        if self.recover and len(self.workers) != self.num_running_info:
            print "Not ready to resume!"

            self.queue.put((15, "resume"))
            return

        if self.recover:
            # Copy the files to check
            for task in self.running:
                if task not in self.check:
                    self.check.add(task)

        if self.recover and not self.workflow:
            print "Waiting for workflow to load!"
            self.queue.put((15, "resume"))
            return

        for task in self.pause_completion_check:
            print "Check task {}".format(task)
            if not self.workflow.task_is_complete(task):
                print "Task {} is incomplete".format(task)
            else:
                if task in self.finished:
                    print "Task {} is complete, insert to finished".format(task)
                    self.finished.add(task)

        self.runnable, self.waiting = self.workflow.det_runnable_tasks(self.finished)

        self.not_running.clear()
        for task in self.runnable:
            if task not in self.running:
                self.not_running.add(task)

        # Things will go wrong for a short period, that's okay
        self.failures = {}

        self.paused = False
        self.recover = False

        try:
            self.leader.zk.delete("/fuse_block")
            print "Unblocking FUSE!!! Continuing execution!"
        except:
            pass

    def do_check_finished(self):
        print "Checking if workflow has finished"

        if len(self.running) != 0:
            return

        if len(self.runnable) != 0:
            print "self.runnable is not empty, reschedule :/"

            for task in self.runnable:
                if self.workflow.task_is_complete(task):
                    print "Task {} is has been completed :/".format(task)
                    self.finished.add(task)
                else:
                    if task not in self.not_running:
                        self.not_running.add(task)
                        print "Task {} not completed, schedule!".format(task)
            return

        if self.workflow.is_finished(self.finished):
            print "Workflow finished"
            if self.leader.statsfile is not None:
                self.leader.statsfile.write("{};0\n".format(time.time() - self.leader.workflow_started))
                self.leader.statsfile.close()
                self.leader.statsfile = None

            self.leader.workflow_lock.acquire()
            self.leader.set_workflow_state(self.workflow_id, "finished")

            self.workflow = None
            self.workflow_id = None

            self.leader.workflow_lock.release()

            self.leader.workflow_watch()
        else:
            print "workflow not finished, should recalculate"
            # self.workflow.det_runnable_tasks()
            self.paused = True
            self.queue.put((15, "resume"))


    def do_scheduling_request(self, worker):
        '''Worker wants to execute a tasks, it says...'''

        if len(self.not_running) == 0:

            if len(self.running) == 0:
                self.queue.put((25, "check_finished"))
            return

        if worker in self.disabled_workers:
            return

        max_num_tasks = self.prefetch_const * self.num_threads
        num_workers = len(self.workers)

        if worker not in self.workers:
            self.workers[worker] = []

        if len(self.workers[worker]) < max_num_tasks:

            # determine the number of tasks to give
            num_to_schedule = max_num_tasks - len(self.workers[worker])

            if len(self.not_running) < num_workers * len(self.workers):
                num_to_schedule = min(num_to_schedule, len(self.not_running) // num_workers)

            num_to_schedule = max(1, num_to_schedule)
            num_to_schedule = min(num_to_schedule, len(self.not_running))

            tasks = []

            for i in range(num_to_schedule):
                task = self.not_running.pop()
                self.running.add(task)

                if task not in self.workers[worker]:
                    self.workers[worker].append(task)

                tasks.append(self.workflow.commands[task])

            self.server_thread.put_request_in_queue([worker, PROTOCOL_HEADERS['RSMNG'], 'task', pickle.dumps( {"tasks" : tasks })])


    def do_failed_tasks(self, worker, failed_tasks):

        require_recomputation = False

        for task in failed_tasks:
            print "Failing task {} from {}".format(task, worker)

            try:
                self.workers[worker].remove(task)
            except:
                print "Ooh noo :-("
                pass

            if task in self.running:
                self.running.remove(task)

            if task in self.runnable and task not in self.not_running:
                self.not_running.add(task)

            if task in self.check:
                print "Removing task {} from checklist".format(task)
                self.check.remove(task)
                continue

            # TODO check if this is true?!
            if self.paused:
                print "Failing task, but paused, so okay"
                continue


            add_log_entry(self.leader, "ERROR", "Worker: {} Failing task: {}, command: {}".format(worker, task, self.workflow.commands[task]))

            # Check parents!
            print "Checking if task can run:"
            if not self.workflow.task_can_run(task):
                require_recomputation = True

            if task in self.failures:
                self.failures[task] += 1

                # Check and fail?
                print "Total failures now: {}".format(self.failures[task])

                if self.failures[task] > 2:
                    print "stop execution!"
                    add_log_entry(self.leader, "ERROR", "Give up execution")
                    self.paused = True
                    self.leader.set_workflow_state(self.workflow_id, "failed")
                    self.leader.workflow_watch()
                    return
            else:
                self.failures[task] = 1


        print "Failed tasks :( {}".format(failed_tasks)

        if require_recomputation:
            print "We should recompute!!!"



    def do_completed_tasks(self, worker, completed_tasks):

        # print "Worker: {}, tasks: {}".format(worker, completed_tasks)

        for task in completed_tasks:
            try:
                self.workers[worker].remove(task)
            except:
                print "Ooh noo :-("
                pass

            if task in self.running:
                self.running.remove(task)

        if not self.workflow:
            return

        if self.paused or self.recover:
            print "Add tasks to check!"
            for task in completed_tasks:
                self.pause_completion_check.add(task)
            return

        for c in completed_tasks[:]:

            if c in self.check:

                print "task {} in checklist!".format(c)
                self.check.remove(c)

                if not self.workflow.task_is_complete(c):
                    print "task {} not complete!!".format(c)
                    completed_tasks.remove(c)

                    if c in self.runnable and c not in self.not_running:
                        print "Requeueing!"
                        self.not_running.add(c)

        # Move from runnable to finished
        for c in completed_tasks:
            if c not in self.finished:
                self.finished.add(c)
            if c in self.runnable:
                self.runnable.remove(c)

        # Calculate new runnable tasks
        new_runnable = self.workflow.next_iteration(self.finished, self.waiting, completed_tasks)
        for t in new_runnable:

            if t not in self.runnable:
                self.runnable.add(t)

            if t not in self.running and t not in self.not_running:
                self.not_running.add(t)

    def do_currently_running_or_waiting(self, worker, tasks):


        print "Node {} currently executing {} tasks".format(worker, len(tasks))
        self.num_running_info += 1

        for task in tasks:

            if worker not in self.workers:
                self.workers[worker] = []

            if task not in self.workers[worker]:
                self.workers[worker].append(task)

            if task not in self.running:
                self.running.add(task)

            if task not in self.check:
                self.check.add(task)

        if self.workers_initialised and len(self.workers) == self.num_running_info:
            self.queue.put((15, "resume"))


    def do_workers(self, zk_workers):

        for worker in zk_workers:
            if worker not in self.workers:
                self.workers[worker] = []
                print "Added {}, now {} workers".format(worker, len(self.workers))

        for worker in self.workers.keys():
            if worker in zk_workers:
                continue

            add_log_entry(self.leader, "WARNING", "Worker {} lost".format(worker))

            self.disabled_workers.append(worker)

            if self.workflow is not None:
                try:
                    print "Worker lost, redistributing.."
                    for task in self.workers[worker]:

                        if task in self.running:
                            self.running.remove(task)

                        if task in self.runnable and task not in self.not_running:
                            self.not_running.add(task)

                    del self.workers[worker]
                except:
                    print "mm :("
                    traceback.print_exc()

        self.workers_initialised = True

        if self.paused and len(self.workers) == self.num_running_info:
            print "Ask to resume! 417"
            self.queue.put((15, "resume"))

    def scheduler_msg_callback(self, message):

        # TODO: check this
        # if self.workflow == None:
        #     return

        try:
            if message[4] != "task":

                if message[4] == "currently_running_or_waiting":
                    data2               = pickle.loads(message[5])
                    self.queue.put((20, "currently_running_or_waiting", message[0], data2))
                    return

                if message[4] != "task_empty":
                    print message[4]
                return

            tmp_msg             = message[4:]
            data2               = pickle.loads(tmp_msg[2])

            computed_tasks      = data2['ran']
            failed_tasks        = data2['failed']
            worker_current_size = data2['qsize']

            if len(failed_tasks) > 0:
                self.queue.put((30, "failed_tasks", message[0], failed_tasks))

            if len(computed_tasks) > 0:
                self.queue.put((50, "completed_tasks", message[0], computed_tasks))

            if worker_current_size <= self.prefetch_const * self.num_threads:
                self.queue.put((60, "scheduling_request", message[0]))

            return

        except:
            print "Whoops whoop"
            traceback.print_exc()

    def debug_dict(self):

        return {'Running': self.running, 'Waiting': self.waiting,
                'Runnable': self.runnable ,'Not running': self.not_running,
                'Checklist': self.check}
