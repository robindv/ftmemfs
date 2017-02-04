import time
import threading
import os
import psutil
import multiprocessing
import sys
import pickle
import math
from ftmemfs.utils import simple_logger
from subprocess import *

import ftmemfs.etc.config as config


class NodeMonitor(threading.Thread):
	"""
	Continously monitors CPU and bandwidth usage.
	"""

	def __init__(self, parent=None):
		threading.Thread.__init__(self)
		self.running = True
		self.parent = parent

		# Holds the most recent bandwidth values.
		self.bandwidth_out_values = []
		self.bandwidth_in_values = []

		# Holds the most recent CPU values.
		self.cpu_values = []

		# Wether or not a resource request is being sent to the scheduler.
		self.sent_resource_request = False
		self.sent_resource_request_lock = threading.Lock()
		self.statistics = {
			'cpu': {
				'current': 0,
				'max': config.MAX_CPU_PERCENT,
				'reached_limit': False
			},
			'bandwidth': {
				'out': {
					'current': 0,
					'max': 1000,
					'reached_limit': False
				},
				'in': {
					'current': 0,
					'max': 1000,
					'reached_limit': False
				}
			}
		}

		# Indicates if the worker can report bandwidth fully or under utilized.
		# These flags are reset to True
		# when the running task type changes and can be set to False when the scheduler
		# did not change bandwidth in response to a request.
		self.allowed_to_report_bandwidth_lock = threading.Lock()
		self.allowed_to_report_bandwidth_fully = True
		self.allowed_to_report_bandwidth_under = True

		# Indicates whether or not we saw bandwidth contention during the current workflow phase
		self.saw_bandwidth_contention_lock = threading.Lock()
		self.saw_bandwidth_contention = False

		# Holds the adaptation history.
		self.adaptation_history_lock = threading.Lock()
		self.adaptation_history = []

		# Holds a map of nr_colocated => (avg bw out, avg bw int)
		self.bandwidth_history_lock = threading.Lock()
		self.bandwidth_history = {}

		# Holds number of tasks for which contention was not detected.
		self.good_points = []

		# Holds number of tasks for which contention was detected.
		self.bad_points = []
		self.points_lock = threading.Lock()

		# Used to indicate we have hit the final plateau in terms of resource utilization.
		self.hit_last_plateau = False

		# Holds the last maximum bandwidth.
		self.last_max_bandwidth = None

		# Create the loggers for the readings.
		bandwidth_out_file_name = config.LOGDIR + '/bandwidth_out_measurements.log'
		self.bandwidth_out_logger = simple_logger.SimpleLogger(bandwidth_out_file_name)
		bandwidth_out_abs_file_name = config.LOGDIR + '/bandwidth_out_abs.log'
		self.bandwidth_out_abs_logger = simple_logger.SimpleLogger(bandwidth_out_abs_file_name)

		bandwidth_in_file_name = config.LOGDIR + '/bandwidth_in_measurements.log'
		self.bandwidth_in_logger = simple_logger.SimpleLogger(bandwidth_in_file_name)
		bandwidth_in_abs_file_name = config.LOGDIR + '/bandwidth_in_abs.log'
		self.bandwidth_in_abs_logger = simple_logger.SimpleLogger(bandwidth_in_abs_file_name)

		cpu_file_name = config.LOGDIR + '/cpu_measurements.log'
		self.cpu_logger = simple_logger.SimpleLogger(cpu_file_name)

		task_adaptation_file_name = config.LOGDIR + '/task_adaptation.log'
		self.task_adaptation_logger = simple_logger.SimpleLogger(task_adaptation_file_name)

		sweet_spot_file_name = config.LOGDIR + '/sweet_spot.log'
		self.sweet_spot_logger = simple_logger.SimpleLogger(sweet_spot_file_name)

		# In case we adapt, log the first number of tasks (with a timestamp of 0).
		if config.ADAPT_TASKS:
			init_tasks_log = '0,' + str(self.parent.get_current_max_tasks())
			self.task_adaptation_logger.log(init_tasks_log)

		# To determine when to gather measurements, we keep track of when a certain
		# thread starts and stops a task.
		self.task_started_lock = threading.Lock()
		self.task_started_by_thread = None
		self.task_finished_by_thread = None

	def run(self):
		"""
		Gets the CPU and bandwidth usage and stores it.
		"""
		while self.running:

			# Get the current bandwidth limit (if it exists). We always want to keep track of the
			# average bandwidth usage, even if we do not have to monitor at this time.
			current_max_bandwidth = 1000
			current_nr_tasks = self.parent.get_nr_running_tasks()
			max_nr_tasks = self.parent.get_current_max_tasks()

			# Only record the measurement if we're currently running enough tasks and they are of the
			# same type.
			if current_nr_tasks != max_nr_tasks or not self.parent.running_identical_tasks():
				continue

			if os.path.isfile(config.MAX_BANDWIDTH_FILE):
				with open(config.MAX_BANDWIDTH_FILE, 'r') as current_bandwidth_file:
					max_str = current_bandwidth_file.readline().strip()
					if max_str == '':
						continue
					current_max_bandwidth = float(max_str)

					# Record the last known maximum bandwidth.
					if self.last_max_bandwidth is None:
						self.last_max_bandwidth = current_max_bandwidth
					elif self.last_max_bandwidth != current_max_bandwidth:

						# If we received more bandwidth, clear good/bad points.
						if self.last_max_bandwidth < current_max_bandwidth:
							print('Clearing good/bad points because of new higher max bandwidth')
							sys.stdout.flush()
							self.points_lock.acquire()
							self.good_points = []
							self.bad_points = []
							self.points_lock.release()
						self.last_max_bandwidth = current_max_bandwidth

				# Get the most recent bandwidth out values.
				last_bandwidth_out = float(psutil.Popen('tail -1 ' + config.BANDWIDTH_OUT_FILE, shell=True, stdout=PIPE, stderr=PIPE).communicate()[0].replace("\n", ""))
				self.bandwidth_out_logger.log((last_bandwidth_out / current_max_bandwidth) * 100)
				self.bandwidth_out_abs_logger.log(last_bandwidth_out)

				# We want to collect enough bandwidth values.
				self.bandwidth_out_values.append(last_bandwidth_out)

				# Get the most recent bandwidth in values.
				last_bandwidth_in = float(psutil.Popen('tail -1 ' + config.BANDWIDTH_IN_FILE, shell=True, stdout=PIPE, stderr=PIPE).communicate()[0].replace("\n", ""))
				self.bandwidth_in_logger.log((last_bandwidth_in / current_max_bandwidth) * 100)
				self.bandwidth_in_abs_logger.log(last_bandwidth_in)

				# We want to collect enough bandwidth values.
				self.bandwidth_in_values.append(last_bandwidth_in)

			# We want to collect enough CPU values.
			last_cpu = psutil.cpu_percent()
			self.cpu_logger.log(last_cpu)
			self.cpu_values.append(last_cpu)

			# See if we have enough measurements.
			if config.ADAPT_TASKS and self.gathered_enough_statistics():

				# Gather all statistics.
				self.statistics = {
					'cpu': {
						'current': self.combine_readings(self.cpu_values),
						'max': config.MAX_CPU_PERCENT
					},
					'bandwidth': {
						'out': {
							'current': self.combine_readings(self.bandwidth_out_values),
							'max': current_max_bandwidth
						},
						'in': {
							'current': self.combine_readings(self.bandwidth_in_values),
							'max': current_max_bandwidth
						}
					}
				}

				# Add the statistics to the history.
				self.adaptation_history_lock.acquire()
				self.adaptation_history.append((current_nr_tasks, self.statistics.copy()))
				self.adaptation_history_lock.release()

				# Save the bandwidth history.
				self.bandwidth_history_lock.acquire()
				if current_nr_tasks not in self.bandwidth_history:
					self.bandwidth_history[current_nr_tasks] = {'out': [], 'in': []}
				self.bandwidth_history[current_nr_tasks]['out'].append(self.statistics['bandwidth']['out']['current'])
				self.bandwidth_history[current_nr_tasks]['in'].append(self.statistics['bandwidth']['in']['current'])
				self.bandwidth_history_lock.release()

				# Adapt the tasks.
				finished_adapting_tasks = self.adapt_nr_tasks(current_nr_tasks, self.statistics)

				# Only take bandwidth action if adaptation is finished and we can report bandwidth.
				if finished_adapting_tasks and self.can_report_bandwidth() and config.ADAPT_BANDWIDTH:
					self.send_bandwidth_stats()

			# Sleep.
			time.sleep(config.MONITOR_PERIOD)

	def send_bandwidth_stats(self, force=False):
		"""
		Collect stats and maybe send them back to the scheduler.
		:return:
		"""
		# Forcibly gather statistics
		if force:
			self.statistics = {
					'cpu': {
						'current': self.combine_readings(self.cpu_values),
						'max': config.MAX_CPU_PERCENT
					},
					'bandwidth': {
						'out': {
							'current': self.combine_readings(self.bandwidth_out_values),
							'max': self.last_max_bandwidth
						},
						'in': {
							'current': self.combine_readings(self.bandwidth_in_values),
							'max': self.last_max_bandwidth
						}
					}
				}
			print(self.statistics)
			print(str(len(self.bandwidth_in_values)))
		# Not only send the statistics, but also the history.
		data = {
			'executable': self.parent.get_latest_task_type(),
			'statistics': self.statistics,
			'history': self.get_bandwidth_and_runtime_history()
		}

		self.allowed_to_report_bandwidth_lock.acquire()
		can_report_fully = (force or self.allowed_to_report_bandwidth_fully)
		can_report_under = (force or self.allowed_to_report_bandwidth_under)
		self.allowed_to_report_bandwidth_lock.release()

		# Bandwidth is fully utilized, try to get more.
		if (self.reached_bandwidth_upper_limit(self.statistics) or self.might_benefit_from_more_bandwidth(self.statistics)) and can_report_fully:
			print('-- Network fully utilized, getting more bandwidth --')
			self.send_resource_request('fully_utilized', data)

		# Bandwidth is under utilized.
		elif self.reached_bandwidth_lower_limit(self.statistics) and can_report_under:
			print('-- Network under utilized, scaling down bandwidth --')
			self.send_resource_request('under_utilized', data)

	def get_bandwidth_and_runtime_history(self):
		"""
		Collects all bandwidth and runtime information to send when the bandwidth is fully utilized.
		:return:
		"""
		self.bandwidth_history_lock.acquire()
		bandwidth_history = self.bandwidth_history.copy()
		self.bandwidth_history_lock.release()

		runtime_history = self.parent.get_task_runtimes()
		result = {}
		for colocate in runtime_history:
			if colocate in bandwidth_history:
				avg_runtime = sum(runtime_history[colocate]) / float(len(runtime_history[colocate]))
				avg_bw_out = sum(bandwidth_history[colocate]['out']) / float(len(bandwidth_history[colocate]['out']))
				avg_bw_in = sum(bandwidth_history[colocate]['in']) / float(len(bandwidth_history[colocate]['in']))
				result[colocate] = [avg_runtime, avg_bw_out, avg_bw_in]
		return result

	def combine_readings(self, readings):
		"""
		Combines the given readings into one value based on the strategy
		in the config file.
		:param readings:
		:return:
		"""
		# The average strategy.
		if config.MEASURE_STRATEGY == 'Average':
			return sum(readings) / float(len(readings))

		# The maximum strategy.
		elif config.MEASURE_STRATEGY == 'Max':
			sorted_readings = readings[:]
			sorted_readings.sort()
			start_index = len(sorted_readings) - config.NR_MEASUREMENTS_FOR_MAX
			return sum(sorted_readings[start_index: len(sorted_readings)]) / float(config.NR_MEASUREMENTS_FOR_MAX)

		# The median strategy.
		else:
			sorted_readings = readings[:]
			sorted_readings.sort()
			if len(sorted_readings) % 2 == 0:
				middle_1 = sorted_readings[(len(sorted_readings) / 2)]
				middle_2 = sorted_readings[(len(sorted_readings) / 2) - 1]
				return (middle_1 + middle_2) / 2.0
			else:
				return sorted_readings[(len(sorted_readings) / 2)]

	def adapt_nr_tasks(self, current_nr_tasks, new_statistics):
		"""
		Tries to increase or decrease the number of tasks to run in parallel.
		We want to utilize as much CPU or bandwidth as possible without there
		being thrashing.
		:return: Whether or not we are done with adaptation at the moment.
		"""
		# There is contention, we have to decrease the number of tasks.
		if self.reached_cpu_limit(new_statistics) or self.reached_bandwidth_upper_limit(new_statistics):
			print('Reached contention with nr tasks: ' + str(current_nr_tasks))
			print(new_statistics)
			sys.stdout.flush()

			# Record that we did see bandwidth contention
			if self.reached_bandwidth_upper_limit(new_statistics):
				self.saw_bandwidth_contention_lock.acquire()
				self.saw_bandwidth_contention = True
				self.saw_bandwidth_contention_lock.release()

			self.points_lock.acquire()
			if current_nr_tasks not in self.bad_points:
				self.bad_points.append(current_nr_tasks)
			if current_nr_tasks in self.good_points:
				self.good_points.remove(current_nr_tasks)

			# Due to the plateau logic, there could still be good points higher than this
			# contention point, remove those.
			self.good_points = [point for point in self.good_points if point < current_nr_tasks]

			self.points_lock.release()

			self.hit_last_plateau = False
			new_nr_tasks = self.decrease_nr_tasks(current_nr_tasks)

		else:

			# Add to the good points and remove from the bad.
			self.points_lock.acquire()
			if current_nr_tasks not in self.good_points:
				self.good_points.append(current_nr_tasks)
			if current_nr_tasks in self.bad_points:
				self.bad_points.remove(current_nr_tasks)
			self.points_lock.release()

			# If the current number of tasks is 'good' and a higher number of tasks
			# is 'bad', we reached the contention value. We have also reached the
			# contention value if we previously hit a plateau.
			if self.hit_last_plateau or self.increase_point_is_bad_point(current_nr_tasks):
				self.sweet_spot_logger.log(str(current_nr_tasks), True, self.parent.get_start_time())
				return True

			# Have we maybe hit a plateau?
			plateau_nr = self.reached_plateau()
			if plateau_nr > 0:
				# Set the new number of tasks.
				print('Setting plateau max: ' + str(plateau_nr))
				sys.stdout.flush()
				self.sweet_spot_logger.log(str(plateau_nr), True, self.parent.get_start_time())
				self.task_adaptation_logger.log(str(plateau_nr), True, self.parent.get_start_time())

				# Reset gathered measurements.
				self.cpu_values = []
				self.bandwidth_out_values = []
				self.bandwidth_in_values = []

				# Set the max
				self.parent.set_current_max_tasks(plateau_nr)
				return True

			# We can still increase the number of tasks
			new_nr_tasks = self.increase_nr_tasks(current_nr_tasks)

		# Set the new number of tasks.
		print('Setting new max: ' + str(new_nr_tasks))
		sys.stdout.flush()
		self.task_adaptation_logger.log(str(new_nr_tasks), True, self.parent.get_start_time())
		self.parent.set_current_max_tasks(new_nr_tasks)

		# Reset gathered measurements.
		self.cpu_values = []
		self.bandwidth_out_values = []
		self.bandwidth_in_values = []

		# Indicate that we are not finished yet.
		return False

	def increase_point_is_bad_point(self, nr_tasks):
		"""
		Determines whether or not increasing the given nr of tasks leads to a bad point.
		:param nr_tasks:
		:return:
		"""
		self.points_lock.acquire()
		if len(self.bad_points) < 1:
			self.points_lock.release()
			return False
		min_bad_point = min(self.bad_points)
		self.points_lock.release()
		point_range = int(math.floor(min_bad_point / (2.0 * multiprocessing.cpu_count())))
		increased = self.increase_nr_tasks(nr_tasks)
		return increased >= (min_bad_point - point_range - 1)

	def reset_known_points(self):
		"""
		Clears the known points.
		:return:
		"""
		#print('resetting known points and clearing runtimes')
		#sys.stdout.flush()
		self.points_lock.acquire()
		self.good_points = []
		self.bad_points = []
		self.points_lock.release()

		if config.ADAPT_TASKS:
			self.parent.set_current_max_tasks(multiprocessing.cpu_count() * 2)

		# Reset bandwidth contention flag.
		self.saw_bandwidth_contention_lock.acquire()
		self.saw_bandwidth_contention = False
		self.saw_bandwidth_contention_lock.release()

		# Clear the adaptation history.
		self.adaptation_history_lock.acquire()
		self.adaptation_history = []
		self.adaptation_history_lock.release()

		# Also clear currently known runtimes.
		self.parent.clear_runtimes()

		# Reset bandwidth history
		self.bandwidth_history_lock.acquire()
		self.bandwidth_history = {}
		self.bandwidth_history_lock.release()

		# We can report bandwidth again.
		self.allowed_to_report_bandwidth_lock.acquire()
		self.allowed_to_report_bandwidth_fully = True
		self.allowed_to_report_bandwidth_under = True
		self.allowed_to_report_bandwidth_lock.release()

		# Reset plateau logic.
		self.hit_last_plateau = False

	def reached_plateau(self):
		"""
		Looks in the adaptation history to see if a plateau has been reached. This can
		be the case when there is no contention for CPU or bandwidth, but resource usage
		has not increased after increasing the number of tasks. If a plateau has been
		reached, this method returns the new nr of tasks, otherwise it returns 0.
		:return:
		"""
		# Plateau has to be at least a certain number of values.
		self.adaptation_history_lock.acquire()
		if len(self.adaptation_history) < 3:
			self.adaptation_history_lock.release()
			return 0
		index = 1
		lowest_nr_tasks = 0

		cpu_ratios = []
		bandwidth_out_ratios = []
		bandwidth_in_ratios = []
		nr_colocated = []
		while index < 3:
			# Compare two latest readings. If the difference in either CPU or network
			# utilization is high enough, we have not reached a plateau.
			current = self.adaptation_history[-1 * index]
			prev = self.adaptation_history[-1 * (index + 1)]

			# We can only reach a plateau if we are increasing tasks.
			current_nr = current[0]
			prev_nr = prev[0]
			if current_nr <= prev_nr:
				self.adaptation_history_lock.release()
				return 0

			# Collect values.
			nr_colocated.append(current_nr)
			current_stats = current[1]
			prev_stats = prev[1]
			current_cpu = current_stats['cpu']['current']
			current_bandwidth_out = current_stats['bandwidth']['out']['current']
			current_bandwidth_in = current_stats['bandwidth']['in']['current']
			cpu_diff = abs(current_cpu - prev_stats['cpu']['current'])
			bandwidth_diff_out = abs(current_bandwidth_out - prev_stats['bandwidth']['out']['current'])
			bandwidth_diff_in = abs(current_bandwidth_in - prev_stats['bandwidth']['in']['current'])

			cpu_ratio = cpu_diff / float(current_cpu + 0.0001)
			bandwidth_ratio_out = bandwidth_diff_out / float(current_bandwidth_out + 0.0001)
			bandwidth_ratio_in = bandwidth_diff_in / float(current_bandwidth_in + 0.0001)
			cpu_ratios.append(cpu_ratio)
			bandwidth_out_ratios.append(bandwidth_ratio_out)
			bandwidth_in_ratios.append(bandwidth_ratio_in)

			lowest_nr_tasks = prev_nr
			index += 1

		# See if we have reached a plateau.
		cpu_ratio_avg = sum(cpu_ratios) / float(len(cpu_ratios))
		bandwidth_out_ratio_avg = sum(bandwidth_out_ratios) / float(len(bandwidth_out_ratios))
		bandwidth_in_ratio_avg = sum(bandwidth_in_ratios) / float(len(bandwidth_in_ratios))
		highest_nr_tasks = self.adaptation_history[-1][0]
		task_ratio = (highest_nr_tasks - lowest_nr_tasks) / float(highest_nr_tasks)

		# If the task ratio is 0, we have a plateau by definition (shouldnt happen)
		self.adaptation_history_lock.release()
		best = self.parent.optimal_nr_tasks(nr_colocated)
		if task_ratio == 0:
			self.hit_last_plateau = True
			if best == 0:
				return lowest_nr_tasks
			return best

		# Do we have a plateau?
		if not (cpu_ratio_avg / task_ratio) <= 1 or not (bandwidth_out_ratio_avg / task_ratio) <= 1 or not (bandwidth_in_ratio_avg / task_ratio) <= 1:
			return 0

		# Yes we have!
		self.hit_last_plateau = True
		if best == 0:
			return lowest_nr_tasks
		return best

	def increase_nr_tasks(self, current_nr_tasks):
		"""
		Returns the new increased number of tasks.
		:param current_nr_tasks:
		:return:
		"""
		self.points_lock.acquire()
		if len(self.bad_points) < 1:
			self.points_lock.release()
			return current_nr_tasks * 2
		max_good_point = max(self.good_points)
		min_bad_point = min(self.bad_points)
		self.points_lock.release()

		# We can't do better than the maximum good point.
		if (max_good_point + 1) == min_bad_point:
			return max_good_point
		else:
			# Go halfway in between the biggest good point and smallest bad point.
			return (min_bad_point + max_good_point) / 2

	def decrease_nr_tasks(self, current_nr_tasks):
		"""
		Returns the new decreased number of tasks.
		:param current_nr_tasks:
		:return:
		"""
		self.points_lock.acquire()
		if len(self.good_points) < 1:
			new_nr_tasks = current_nr_tasks / 2
			while new_nr_tasks in self.bad_points:
				new_nr_tasks /= 2
			if new_nr_tasks <= 0:
				new_nr_tasks = 1
			self.points_lock.release()
			return new_nr_tasks
		max_good_point = max(self.good_points)
		min_bad_point = min(self.bad_points)
		self.points_lock.release()

		# We can't do better than the maximum good point.
		if (max_good_point + 1) == min_bad_point:
			return max_good_point
		else:
			# Go halfway in between the biggest good point and smallest bad point.
			return (min_bad_point + max_good_point) / 2

	def can_report_bandwidth(self):
		"""
		Wether or not the worker can report under or over utilization of the network to the
		scheduler. There are a few requirements for this:
		- There have to be running tasks
		- There must be no outstanding bandwidth request to the scheduler.
		:return:
		"""
		self.sent_resource_request_lock.acquire()
		self.allowed_to_report_bandwidth_lock.acquire()
		can_report = (self.parent.get_nr_running_tasks() > 0 and not self.sent_resource_request)
		self.allowed_to_report_bandwidth_lock.release()
		self.sent_resource_request_lock.release()
		return can_report

	def send_resource_request(self, message_type, statistics):
		"""
		Forwards the resource request to the parent and makes sure the Thread stops monitoring
		until it has heard back from the scheduler.
		"""
		self.sent_resource_request_lock.acquire()
		self.sent_resource_request = True
		self.parent.send_resource_request(message_type, statistics)
		self.sent_resource_request_lock.release()

	def got_resource_reply(self, data):
		"""
		The scheduler sent back a reply to the resource request.
		"""
		self.sent_resource_request_lock.acquire()
		self.sent_resource_request = False
		self.sent_resource_request_lock.release()

		self.allowed_to_report_bandwidth_lock.acquire()
		can_report = pickle.loads(data)
		if 'under_utilized' in can_report:
			self.allowed_to_report_bandwidth_under = can_report['under_utilized']
		if 'fully_utilized' in can_report:
			self.allowed_to_report_bandwidth_fully = can_report['fully_utilized']
		sys.stdout.flush()
		self.allowed_to_report_bandwidth_lock.release()

	def reached_cpu_limit(self, statistics):
		"""
		Determines if the CPU or bandwidth is contended for.
		"""
		return statistics['cpu']['current'] >= float(config.MAX_CPU_PERCENT)

	def reached_bandwidth_upper_limit(self, statistics):
		"""
		Determines if the upper bandwidth limit is reached. This happens if either
		the incoming or the outgoing bandwidth reaches the current maximum.
		"""
		current_bandwidth_out = statistics['bandwidth']['out']['current']
		current_bandwidth_in = statistics['bandwidth']['in']['current']
		if current_bandwidth_out > current_bandwidth_in:
			current_bandwidth = current_bandwidth_out
		else:
			current_bandwidth = current_bandwidth_in

		# Bandwidth in/out limits are the same.
		max_bandwidth = statistics['bandwidth']['out']['max']
		return (current_bandwidth / max_bandwidth) * 100 >= float(config.MAX_BANDWIDTH_PERCENT)

	def reached_bandwidth_lower_limit(self, statistics):
		"""
		Determines if the lower bandwidth limit is reached. This happens if both the incoming
		and the outgoing bandwidth reach the lower limit, or alternatively put: the largest
		of the two reaches the lower limit.
		"""
		current_bandwidth_out = statistics['bandwidth']['out']['current']
		current_bandwidth_in = statistics['bandwidth']['in']['current']
		if current_bandwidth_out > current_bandwidth_in:
			current_bandwidth = current_bandwidth_out
		else:
			current_bandwidth = current_bandwidth_in
		# Bandwidth in/out limits are the same.
		max_bandwidth = statistics['bandwidth']['out']['max']

		bandwidth_values = config.AVAILABLE_BANDWIDTH_VALUES.keys()
		bandwidth_values.sort()

		# We say that there is under utilization when the bandwidth drops below the first purchaseble value
		# that is below the current maximum bandwidth.
		for idx, bandwidth in enumerate(bandwidth_values):
			if max_bandwidth == bandwidth:
				if idx >= 1:
					lower = bandwidth_values[idx - 1]
					return current_bandwidth < lower

		return False

	def might_benefit_from_more_bandwidth(self, statistics):
		"""
		Determines, given the statistics and whether or not we saw bandwidth contention when
		adapting tasks, if we can benefit from more bandwidth.
		"""
		current_bandwidth_out = statistics['bandwidth']['out']['current']
		current_bandwidth_in = statistics['bandwidth']['in']['current']
		current_cpu = statistics['cpu']['current']
		if current_bandwidth_out > current_bandwidth_in:
			current_bandwidth = current_bandwidth_out
		else:
			current_bandwidth = current_bandwidth_in

		self.saw_bandwidth_contention_lock.acquire()
		bandwidth_contention = self.saw_bandwidth_contention
		self.saw_bandwidth_contention_lock.release()

		# Bandwidth in/out limits are the same.
		max_bandwidth = statistics['bandwidth']['out']['max']
		max_cpu = statistics['cpu']['max']
		bandwidth_ratio = current_bandwidth / float(max_bandwidth)
		cpu_ratio = current_cpu / float(max_cpu)

		can_benefit = (bandwidth_contention and (bandwidth_ratio > cpu_ratio))
		return can_benefit

	def task_started(self, thread_id):
		"""
		Indicates that the given thread started a task.
		"""
		self.task_started_lock.acquire()
		if self.task_started_by_thread is None:
			self.task_started_by_thread = thread_id
		self.task_started_lock.release()

	def task_finished(self, thread_id):
		"""
		Indicates that the given thread finished a task.
		"""
		self.task_started_lock.acquire()
		if self.task_started_by_thread == thread_id:
			self.task_finished_by_thread = thread_id
		self.task_started_lock.release()

	def gathered_enough_statistics(self):
		"""
		Returns whether or not enough statistics have been gathered to act on them.
		"""
		self.task_started_lock.acquire()
		result = (self.task_started_by_thread is not None and (self.task_started_by_thread == self.task_finished_by_thread))
		if result:
			self.task_started_by_thread = None
			self.task_finished_by_thread = None
		self.task_started_lock.release()
		return result

	def stop(self):
		"""
		Stops the monitoring thread.
		"""
		self.running = False
