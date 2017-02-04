"""
   General configuration
"""
import sys

sys.setrecursionlimit(4000)

FTMEMFS_MAX_CHUNK_SIZE = 1024*1024

''' Where to save a log for everything '''
LOGDIR = '/local/rdevries/logs'

''' Path to the file used for MemFS configuration. Will be generated at runtime. '''
MEMFS_CONFIG_FILE = '/home/odr700/elastic_memfs/config'

''' Whether or not to start MemFS when starting the scheduler. '''
START_MEMFS = False

''' Beta value to use when creating and assigning MemFS partitions. '''
MEMFS_BETA = 4

''' DEBUG, INFO, ERROR '''
LOG_LEVEL = 'DEBUG'

''' Scheduler address, is also set by run_weasel.sh '''
SCHEDULER = ''

''' Dictionary of worker addresses to instance type, also set by run_weasel.sh '''
WORKERS = {}

''' Scheduler ZMQ port '''
ZMQ_SCHEDULER_PORT = 5556

''' How many seconds to sleep between 2 scheduling periods '''
WAITTIME = 4

''' Whether or not to adapt tasks dynamically '''
ADAPT_TASKS = False

''' Whether or not to adapt bandwidth dynamically '''
ADAPT_BANDWIDTH = False

''' The number of colocated tasks if SHOULD_ADAPT == False '''
NR_COLOCATED_TASKS = 16 # 8 on DAS4

''' How many seconds to sleep between 2 utilization readings (make it under 1 sec for more accurate readings)'''
MONITOR_PERIOD = 0.2

''' How much CPU usage to allow before considering the CPU fully utilized '''
MAX_CPU_PERCENT = 95

''' How much bandwidth to allow before considering the network fully utilized. '''
MAX_BANDWIDTH_PERCENT = 90

''' The threshold for when to consider the network under utilized '''
MIN_BANDWIDTH_PERCENT = 50

''' How many measurements to collect per task type before going to task adaptation '''
NR_MEASUREMENTS_PER_TASK_TYPE = 30

''' How many measurements to consider for the Max strategy. '''
NR_MEASUREMENTS_FOR_MAX = 10

''' The strategy to use when combining readings. Can be one of the following: Max, Average, Median '''
MEASURE_STRATEGY = 'Max'

''' The strategy to use when purchasing more bandwidth. Can be one of the following: Performance, Cost '''
MAX_BANDWIDTH_STRATEGY = 'Cost'

''' Key-value pairs of bandwidth values (in Mbit/s) to their respective cost. '''
AVAILABLE_BANDWIDTH_VALUES = {50: 10, 100: 20, 200: 40, 400: 50}

''' The bandwidth billing period in seconds. '''
BANDWIDTH_BILLING_PERIOD = 60

''' Path to the file that contains the (current) bandwidth limit in Mbit/s '''
MAX_BANDWIDTH_FILE = '/var/log/current_bandwidth'

''' Path to the file that contains (current) egress bandwidth usage in Mbit/s. Every line must contain one value. '''
BANDWIDTH_OUT_FILE = '/local/rdevries/psutil_bandwidth.out'

''' Path to the file that contains (current) egress bandwidth usage in Mbit/s. Every line must contain one value. '''
BANDWIDTH_IN_FILE = '/local/rdevries/psutil_bandwidth.in'

''' The path to the bandwidth control binary. See https://github.com/ovedanner/bandwidth-throttler '''
BANDWIDTH_CONTROL_BINARY = 'python /home/rdevries/bandwidth-throttler/shape_traffic_client.py'

''' The port on which the bandwidth control binary listens for bandwidth requests. '''
BANDWIDTH_CONTROL_PORT = 5555
