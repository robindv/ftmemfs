from subprocess import *
import hashlib
import os
import pickle
import psutil
import sys
import multiprocessing
import threading
import socket

from ftmemfs.utils.notification import *
from ftmemfs.utils.threads import *
from ftmemfs.utils.logger import WeaselLogger
from ftmemfs.worker.node_monitor import NodeMonitor

import ftmemfs.etc.config as config


class NodeScheduler(threading.Thread):
    def __init__(self, leader, lm):

        threading.Thread.__init__(self)

        interface = lm.config.get("general","interface")
        self.localmanager = lm

        self.cwd = "/local/rdevries/ftmemfs"

        f = os.popen("ip addr show {} | grep \"inet \" | cut -d' ' -f6 | cut -d/ -f1".format(interface))
        self.identity = f.read().strip()
        self.queue_data_lock = threading.Lock()

        # Make a random hash for the queue.
        self.random_hash = hashlib.sha1(b'/etc/hostname').hexdigest()

        print "Random hash" + self.random_hash

        self.task_data = {}
        self.queue_data = {}
        self.max_tasks_to_run_for_queue = {}

        # Will hold the timestamp when we first received a task.
        self.time_start_running = -1

        # Initially we run four tasks per CPU core. This number might
        # be changed by the monitor thread based on resource contention.
        if config.ADAPT_TASKS:
            self.max_tasks_to_run = multiprocessing.cpu_count() * 2
        else:
            self.max_tasks_to_run = int(lm.config.get("worker", "nr_colocated_tasks"))

        # Create the queue for the tasks.
        self.queue_data[self.random_hash] = {'qid': self.random_hash,
                                             'asked': 0, 'recv': 0,
                                             'tpool': ThreadPool(1, self.task_data)}
        tpool = self.queue_data[self.random_hash]['tpool']
        self.max_tasks_to_run_for_queue[self.random_hash] = self.max_tasks_to_run
        tpool.set_size(self.max_tasks_to_run)
        tpool.start()

        # Create the monitoring thread.
        self.monitor_thread = NodeMonitor(parent=self)

        # Create the thread that performs communication with
        # the scheduler.
        self.sched_client_thread = ZmqConnectionThread(
            self.identity,
            zmq.DEALER,
            leader + ":" + str(config.ZMQ_SCHEDULER_PORT),
            self.callback)
        self.running = True
        logfile = config.LOGDIR + "/local_scheduler.log"
        self.logger = WeaselLogger('local_scheduler', logfile)
        self.ntasks_to_ask = 1
        self.task_id = 1
        self.time_asked_first = time.time()
        self.running_task = 0
        self.nr_received_tasks = 0
        self.nran_tasks = []
        self.nfailed_tasks = []
        self.queues_asked_for = []
        self.current_ntasks = 1
        self.has_new_task = False
        self.first_task = False
        self.running_task_lock = threading.Lock()

        # Will hold a map of nr_colocated_tasks => [runtimes]
        self.task_runtimes = {}
        self.task_runtime_lock = threading.Lock()
        self.sleep_time = config.WAITTIME
        self.task_time = 1
        self.has_new_queue = False
        self.new_queues = []
        self.logger.info("NodeScheduler started...")
        self.nrunning_past_period = []

        # Will hold the last started executable.
        self.last_started = None

        self.currently_running_or_waiting = []

    def change_ip(self, leader):
        self.sched_client_thread.change_addr(self.identity,
        zmq.DEALER,leader + ":" + str(config.ZMQ_SCHEDULER_PORT))

        print "Send to leader!"
        self.sched_client_thread.put_request_in_queue(
            [self.identity, PROTOCOL_HEADERS['WORKER'], 'currently_running_or_waiting', pickle.dumps(self.currently_running_or_waiting)])

        # print "changing ip!, currently in our bag: {}".format()


    def run_task(self, arg):
        command_id = arg['id']
        command = arg['exec'] + ' ' + arg['params']
        qid = arg['qid']
        myid = threading.current_thread().ident

        # Tell the monitor thread we have started a task.
        self.monitor_thread.task_started(myid)

        # Increment the number of running tasks.
        self.running_task_lock.acquire()
        self.running_task += 1
        nr_colocated = self.running_task

        # The first time we run a task of a different type, we reset
        # the history on the monitor thread.
        if self.last_started is None:
            self.last_started = arg['exec']
        if self.last_started != arg['exec']:
            # print('Started new task type: ' + str(arg['exec']))
            sys.stdout.flush()
            self.monitor_thread.reset_known_points()
            self.last_started = arg['exec']
        self.running_task_lock.release()


        # print("Executing " + str(command))

        start_time = time.time()
        proc = psutil.Popen(command, shell=True,
                            stdout=PIPE, stderr=PIPE, cwd = self.cwd)
        self.task_data[myid]['lock'].acquire()
        self.task_data[myid]['proc'] = proc
        self.task_data[myid]['ctask'] = arg
        self.task_data[myid]['lock'].release()
        out, err = proc.communicate()
        return_code = proc.returncode
        failed = False
        if return_code != 0:
            # print('Was executing: ' + str(command))
            # print('Error when returning: ' + str(return_code))
            failed = True
            # print('Take a deep breath...')

            self.localmanager.add_log_entry("ERROR", "Failing task {} {} {}".format(command_id, out, err))

            time.sleep(1)
            sys.stdout.flush()
        else:
            if not self.localmanager.legacy:
                for output_file in arg['output_files']:
                    try:
                        self.localmanager.zk.create_async("/data/{}/written".format(output_file), "")
                    except:
                        print "Nope {}".format(output_file)

        end_time = time.time()

        # Record task running times.
        self.task_runtime_lock.acquire()
        running_time = end_time - start_time
        if nr_colocated not in self.task_runtimes:
            self.task_runtimes[nr_colocated] = []
        self.task_runtimes[nr_colocated].append(running_time)
        # print("Task %s ran in %s seconds (%s)" % (str(command_id), str(running_time), str(arg['exec'])))
        sys.stdout.flush()
        self.task_runtime_lock.release()

        # Tell the monitor thread we have finished a task.
        self.monitor_thread.task_finished(myid)

        self.task_data[myid]['lock'].acquire()
        self.task_data[myid]['ctask'] = None
        if self.task_data[myid]['task'].get(qid) == None:
            self.task_data[myid]['task'][qid] = []
            self.external_change = True
        self.task_data[myid]['task'][qid].append(
            [end_time - start_time, 100 * (end_time - start_time) / (end_time - start_time)])
        self.task_data[myid]['lock'].release()
        self.running_task_lock.acquire()
        self.running_task -= 1

        if failed is False:
            self.nran_tasks.append(command_id)
        else:
            self.nfailed_tasks.append(command_id)

        self.running_task_lock.release()

    def get_total_queue_size(self):
        queue_size = 0
        self.queue_data_lock.acquire()
        for qid in self.queue_data:
            queue_size = queue_size + self.queue_data[qid]['tpool'].tasks.qsize()
        self.queue_data_lock.release()
        return queue_size

    def get_tasks_to_ask(self):
        """
        Returns the number of tasks to ask from the scheduler. Tries to keep the queue size at
        least as long as the maximum allowed number of tasks at the moment.
        :return:
        """
        tasks_to_ask = {}
        self.queues_asked_for = []
        queue_size = self.get_total_queue_size()
        self.queue_data_lock.acquire()
        for qid in self.queue_data:
            tasks_to_ask[qid] = 0
            self.queue_data[qid]['asked'] = 0
            self.queue_data[qid]['recv'] = 0
            qsize = self.queue_data[qid]['tpool'].tasks.qsize()
            if qsize > 2 * self.max_tasks_to_run_for_queue[qid] and self.max_tasks_to_run_for_queue[qid] != -1:
                continue
            if qsize == 0:
                tasks_to_ask[qid] = self.max_tasks_to_run_for_queue[qid]
            else:
                if qsize > self.max_tasks_to_run_for_queue[qid] and self.max_tasks_to_run_for_queue[qid] != -1:
                    continue
                elif qsize < self.max_tasks_to_run_for_queue[qid]:
                    tasks_to_ask[qid] = self.max_tasks_to_run_for_queue[qid] - qsize
            self.queues_asked_for.append(qid)
            self.queue_data[qid]['asked'] = tasks_to_ask[qid]
        self.queue_data_lock.release()
        return tasks_to_ask, queue_size

    def wait_and_ask(self):
        while self.running:
            # check at 0.2 seconds
            time.sleep(0.2)

            self.running_task_lock.acquire()
            nrunning = self.running_task
            self.nrunning_past_period.append(nrunning)

            for task in self.nran_tasks:
                try:
                    self.currently_running_or_waiting.remove(task)
                except:
                    pass
            for task in self.nfailed_tasks:
                try:
                    self.currently_running_or_waiting.remove(task)
                except:
                    pass

            task_data_to_send  = {'ran': self.nran_tasks[:], 'failed': self.nfailed_tasks[:]}
            self.nran_tasks    = []
            self.nfailed_tasks = []
            self.running_task_lock.release()
            (tasks_to_ask, queue_size) = self.get_tasks_to_ask()
            task_data_to_send['qsize'] = queue_size * self.task_time
            pickled_data = pickle.dumps(task_data_to_send)
            if len(tasks_to_ask) > 0 and self.sched_client_thread != None:
                self.sched_client_thread.put_request_in_queue(
                    [self.identity, PROTOCOL_HEADERS['WORKER'], 'task', pickle.dumps(tasks_to_ask), pickled_data])
            else:
                pass

    def process_task(self, task):
        tmp = task.split(';')
        task_name = tmp[-1].split()[0].split('/')[-1]
        new_task = False
        return new_task

    def add_task_to_queues(self, tasks):

        for task in tasks['tasks']:
            self.running_task_lock.acquire()
            self.nr_received_tasks += 1
            self.running_task_lock.release()
            new_task = self.process_task(task['exec'])
            task_hash = hashlib.sha1(task['exec'].encode()).hexdigest()
            self.has_new_task |= new_task
            task['qid'] = task_hash
            self.queue_data[self.random_hash]['tpool'].add_task(self.run_task, task)

    def get_latest_task_type(self):
        """
        Return the latest started task type.
        :return:
        """
        self.running_task_lock.acquire()
        latest = self.last_started
        self.running_task_lock.release()
        return latest

    def running_identical_tasks(self):
        """
        Returns whether or not the worker is currently only running tasks of the same type.
        :return:
        """
        current = None
        task_threads = self.task_data.keys()
        try:
            for task_thread in task_threads:
                if (task_thread not in self.task_data) or ('lock' not in self.task_data[task_thread]):
                    continue
                self.task_data[task_thread]['lock'].acquire()
                if 'ctask' in self.task_data[task_thread] and self.task_data[task_thread]['ctask'] is not None:
                    if current is None:
                        current = self.task_data[task_thread]['ctask']['exec']
                    elif current != self.task_data[task_thread]['ctask']['exec']:
                        self.task_data[task_thread]['lock'].release()
                        return False
                self.task_data[task_thread]['lock'].release()
            return True
        except Exception, e:
            print('Got exception while trying to determine identical tasks')
            print(e)
            sys.stdout.flush()

    def optimal_nr_tasks(self, colocated):
        """
        Determines which of the given number of colocated tasks has the highest
        throughput.
        :param colocated:
        :return:
        """
        self.task_runtime_lock.acquire()
        best = 0
        nr = 0
        for nr_tasks in colocated:
            if nr_tasks in self.task_runtimes:
                runtimes = self.task_runtimes[nr_tasks]
                avg_runtime = sum(runtimes) / float(len(runtimes))
                tasks_per_second = nr_tasks / avg_runtime
                if best == 0 or tasks_per_second > best:
                    best = tasks_per_second
                    nr = nr_tasks
        self.task_runtime_lock.release()
        return nr

    def get_task_runtimes(self):
        """
        Returns recorded task runtimes.
        :return:
        """
        self.task_runtime_lock.acquire()
        runtimes = self.task_runtimes.copy()
        self.task_runtime_lock.release()
        return runtimes

    def clear_runtimes(self):
        """
        Clears current task runtimes.
        :return:
        """
        self.task_runtime_lock.acquire()
        self.task_runtimes = {}
        self.task_runtime_lock.release()

    def get_current_max_tasks(self):
        """
        Returns the current maximum number of tasks to run in parallel.
        :return:
        """
        self.queue_data_lock.acquire()
        size = self.max_tasks_to_run
        self.queue_data_lock.release()
        return size

    def set_current_max_tasks(self, nr_max_tasks):
        """
        Sets the current maximum number of tasks to run in parallel.
        :param nr_max_tasks:
        :return:
        """
        self.queue_data_lock.acquire()
        tpool = self.queue_data[self.random_hash]['tpool']
        tpool.resize(nr_max_tasks, [])
        self.max_tasks_to_run_for_queue[self.random_hash] = nr_max_tasks
        self.max_tasks_to_run = nr_max_tasks
        self.queue_data_lock.release()

    def callback(self, frames):
        """
        Callback for messages from the scheduler.
        """
        command = frames[2]

        data = None
        if len(frames) > 3:
            data = frames[3]
        if command == 'shutdown':
            self.shutdown()
        elif command == 'task':
            # Record the time of the first tasks.
            if self.time_start_running == -1:
                self.time_start_running = time.time()
            tasks = pickle.loads(data)

            for task in tasks['tasks']:
                self.currently_running_or_waiting.append(task['id'])

            self.add_task_to_queues(tasks)
        elif command == 'empty':
            for qid in self.queue_data:
                self.empty_queue(self.queue_data[qid]['tpool'])
        elif command == 'resource':
            self.monitor_thread.got_resource_reply(data)
        elif command == 'get_bandwidth':
            self.monitor_thread.send_bandwidth_stats(True)
        elif command == 'statistics':
            # We only send statistics if we are still running the same executable.
            print('Scheduler wants to have statistics')
            executable = pickle.loads(frames[3])
            self.running_task_lock.acquire()
            if executable == self.last_started:
                result = self.monitor_thread.get_bandwidth_and_runtime_history()
            else:
                result = False
            self.running_task_lock.release()
            self.sched_client_thread.put_request_in_queue(
                    [self.identity, PROTOCOL_HEADERS['WORKER'], 'statistics', 'worker_statistics', pickle.dumps(result)])
        else:
            print "No callback for this message! {}".format(frames)

    def empty_queue(self, queue):
        while not queue.empty():
            try:
                queue.get(False)
            except Empty:
                continue
            queue.task_done()

    def shutdown(self):
        self.running = False

    def check_empty_queues(self):
        queues_empty = True
        self.queue_data_lock.acquire()
        for qid in self.queue_data:
            queues_empty = queues_empty & self.queue_data[
                qid]['tpool'].tasks.empty()
        self.queue_data_lock.release()
        return queues_empty

    def send_resource_request(self, message_type, statistics):
        """
        Sends a resource request to the scheduler with the given type
        and usage statistics.
        :param message_type:
        :param statistics:
        :return:
        """
        self.sched_client_thread.put_request_in_queue(
                    [self.identity, PROTOCOL_HEADERS['WORKER'], 'resource', message_type, pickle.dumps(statistics)])

    def get_nr_running_tasks(self):
        """
        Retrieve the current number of running tasks.
        :return:
        """
        self.running_task_lock.acquire()
        nr_running = self.running_task
        self.running_task_lock.release()
        return nr_running

    def get_start_time(self):
        """
        Returns the timestamp of when we started running tasks.
        :return:
        """
        return self.time_start_running

    def run(self):
        # Start the thread that receives from the server.
        self.sched_client_thread.start()

        # Start the monitoring thread.
        self.monitor_thread.start()

        # Create and start the thread that asks for tasks.
        finishing_tasks_thread = Thread(target=self.wait_and_ask)
        finishing_tasks_thread.start()
        while self.running:
            try:
                ''' if queue is empty and no other tasks are running: ask for task to the scheduler '''
                ''' else if tasks are running check the utilization and ask for more/less '''
                self.running_task_lock.acquire()
                nrunning = self.running_task
                self.running_task_lock.release()
                if self.check_empty_queues() and nrunning == 0 and self.sched_client_thread is not None:
                    ''' I have finished all my tasks, ask for random task from the resource mng '''
                    ''' For now just ask for one task, will change this later'''
                    self.sched_client_thread.put_request_in_queue(
                        [self.identity, PROTOCOL_HEADERS['WORKER'], 'task_empty', "1"])
                time.sleep(self.sleep_time)
            except KeyboardInterrupt:
                self.shutdown()

        # The finishing_tasks_thread is stopped by self.shutdown().
        finishing_tasks_thread.join()

        # Wait for the tasks to finish.
        for qid in self.queue_data:
            self.queue_data[qid]['tpool'].wait_completion()

        # Stop the receiving and monitoring thread.
        if self.sched_client_thread is not None:
            self.sched_client_thread.stop()
            self.sched_client_thread.join()

        self.monitor_thread.stop()
        self.monitor_thread.join()
