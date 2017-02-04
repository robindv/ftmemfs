import signal


def init_worker():
	signal.signal(signal.SIGINT, signal.SIG_IGN)


import threading
from Queue import Queue
from threading import Thread


class Worker(Thread):
	"""Thread executing tasks from a given tasks queue"""

	def __init__(self, tasks, shared_dict, workers_map, workers_map_lock, \
				 deleted_workers, lock):
		Thread.__init__(self)
		self.tasks = tasks
		self.daemon = True
		self.dying = False
		self.deleted_workers = deleted_workers
		self.shared_dict = shared_dict
		self.workers_map = workers_map
		self.workers_map_lock = workers_map_lock
		self.lock = lock
		self.myid = None
		self.start()

	def remove_myself(self):
		self.lock.acquire()
		self.deleted_workers.append(self.myid)
		self.lock.release()

	def run(self):
		myid = threading.current_thread().ident
		self.workers_map_lock.acquire()
		self.workers_map[myid] = self
		self.workers_map_lock.release()
		self.myid = myid
		if self.shared_dict is not None:
			self.lock.acquire()
			if not self.shared_dict.get(myid):
				self.shared_dict[myid] = {}
				self.shared_dict[myid]['lock'] = threading.Lock()
				self.shared_dict[myid]['task'] = {}
			self.lock.release()
		while True:
			if self.dying:
				self.remove_myself()
				return
			func, args, kargs = self.tasks.get()
			try:
				func(*args, **kargs)
			except Exception as e:
				print e
			finally:
				self.tasks.task_done()


class ThreadPool:
	"""Pool of threads consuming tasks from a queue """

	def __init__(self, num_threads, shared_dict):
		self.tasks = Queue()
		self.num_threads = num_threads
		self.workers = []
		self.workers_map = {}
		self.deleted_workers = []
		self.shared_dict = shared_dict
		self.dict_lock = threading.Lock()
		self.resize_lock = threading.Lock()
		self.workers_map_lock = threading.Lock()

	def set_size(self, nthreads):
		self.num_threads = nthreads

	def get_size(self):
		return self.num_threads

	def start(self):
		for _ in range(self.num_threads):
			new_worker = Worker(
				self.tasks,
				self.shared_dict,
				self.workers_map,
				self.workers_map_lock,
				self.deleted_workers,
				self.dict_lock)
			self.workers.append(new_worker)

	def resize(self, new_nthreads, threads_to_stop):
		if new_nthreads < 0 or new_nthreads == self.num_threads:
			return
		self.resize_lock.acquire()
		while new_nthreads > self.num_threads:
			new_worker = Worker(
				self.tasks,
				self.shared_dict,
				self.workers_map,
				self.workers_map_lock,
				self.deleted_workers,
				self.dict_lock)
			self.workers.append(new_worker)
			self.num_threads += 1
		if len(threads_to_stop) == 0:
			self.workers_map_lock.acquire()
			while new_nthreads < self.num_threads:
				self.workers[0].dying = True
				self.num_threads -= 1
				del self.workers[0]
			self.workers_map_lock.release()
		else:
			self.workers_map_lock.acquire()
			for tid in threads_to_stop:
				worker = self.workers_map[tid]
				self.workers.remove(worker)
				try:
					del self.workers_map[tid]
				except:
					print "Cannot find thread ", tid
			self.workers_map_lock.release()
		self.resize_lock.release()

	def add_task(self, func, *args, **kargs):
		"""Add a task to the queue"""
		self.tasks.put((func, args, kargs))

	def get_tasks(self, ntasks):
		commands = []
		for i in range(1, ntasks):
			try:
				(func, args, kargs) = self.tasks.get(False)
				commands.append((func, args, kargs))
				self.tasks.task_done()
			except:
				continue
		return commands

	def wait_completion(self):
		"""Wait for completion of all the tasks in the queue"""
		self.tasks.join()

	def finish(self):
		for worker in self.workers:
			worker.dying = True
			worker.join()
