from kazoo.client import KazooClient
import kazoo.exceptions
import cmd
import ConfigParser
import os
import subprocess
import threading
import random
import xml.etree.ElementTree as ET
import pickle
import struct
import logging
import time
import socket
from multiprocessing import Pool
import collections
import sys
import redis as pyredis
import utils.hrw
import xxhash
import datetime


class FTCli(cmd.Cmd):

    def emptyline(self):
        return

    def do_exit(self, line):
        return True


    def inject_failure(self, hosts):
        print "injecting failures to hosts: "
        print hosts

        if platform == "das4":
            work =  ["ssh {} \"ps aux | grep ^{} | egrep -v 'ps aux|-bash|sshd|sleep|drmaa_slurm_script|grep|awk|tail' | awk '{{ print \$2 }}' | xargs kill\" &> /dev/null".format(host,os.getlogin()) for host in hosts]
        else:
            work =  ["ssh {} \"ps aux | grep ^{} | egrep -v 'ps aux|-bash|sh|sshd|sleep|drmaa_slurm_script|grep|awk|tail' | awk '{{ print \$2 }}' | xargs kill -9\" &> /dev/null".format(host,os.getlogin()) for host in hosts]


        pool = Pool(processes = 8)
        outputs = pool.map(do_work, work)
        pool.terminate()

    def do_inject(self, line):

        # This is only an example, you can put more interesting failure behaviour in here ;-)
        time = get_integer("After how many seconds should the hosts be killed?", 5, 3600)
        hosts = ['10.149.0.35']

        t = threading.Timer(time, self.inject_failure, args=[hosts])
        t.start()

    def do_zkcmd(self, line):

        try:
            print zk.command(line)
        except:
            print "Something went wrong"

    def do_file_sizes(self, line):

        try:
            children = zk.get_children("/data/{}".format(line))

            total = 0

            for child in children:
                # print "/{}/{}".format(line, child)

                zinode = utils.hrw.get_zinode(zk, "{}/{}".format(line, child))
                total += zinode['size']

            print total

        except kazoo.exceptions.NoNodeError:
            print "ZNode {} not found".format(line)



    def do_memory_usage(self, line):

        hosts = zk.get_children("/services")
        hosts.sort()

        num_workers = 0
        zookeeper_hosts = []
        redis_hosts = []

        for host in hosts:
             if zk.exists("/services/{}/redis".format(host)):
                 redis_hosts.append(host)

             if zk.exists("/services/{}/zookeeper".format(host)):

                 if zk.exists("/services/{}/leader".format(host)):
                     continue

                 zookeeper_hosts.append(host)

        work = ["ssh {} 'cat /proc/`pidof redis-server`/status | grep VmRSS'".format(host) for host in redis_hosts ]

        pool = Pool(processes = 8)
        outputs = pool.map(do_work, work)
        pool.terminate()

        redis_usage = 0
        for output in outputs:
             redis_usage += int(output[0].split("\t")[1].strip().split(" kB")[0])
        print "Total redis usage: {} kB using {} nodes".format(redis_usage, len(redis_hosts))

        work = ["ssh {} 'cat /proc/`pidof java`/status | grep VmRSS'".format(host) for host in zookeeper_hosts ]

        pool = Pool(processes = 8)
        outputs = pool.map(do_work, work)
        pool.terminate()

        print "ZooKeeper:"
        for output in outputs:
             print output[0].strip()


    def do_unblock(self, line):
        try:
            zk.delete("/fuse_block")
        except:
            pass

    def do_block(self, line):
        zk.create("/fuse_block")

    def do_write_machinefile(self, line):
        f = open('/home/rdevries/machinefile.txt','w')
        children = zk.get_children("/services")
        children.sort()
        for child in children:
            f.write("{}\n".format(child))
        f.close()

    def do_redis_toggle(self, line):

        children = zk.get_children("/redis")
        children.sort()

        if len(children) == 0:
            print "No children"
            return

        value = zk.get("/redis/" + children[0])[0].split(',')
        value[0] = "0" if value[0] == "-1" else "-1"
        zk.set("/redis/" + children[0], ','.join(value))
        print "Nodeclass now {} for {}".format(value[0], value[1])


    def do_predict_leader(self, line):

        children = zk.get_children("/services")
        children.sort()

        for host in children:
            if not zk.exists("/services/{}/zookeeper".format(host)):
                continue

            print "\n{}: ".format(host)
            cmd = 'ssh {} \'echo "stat" | nc 127.0.0.1 2181 | grep "Mode: "\''.format(host)
            os.system(cmd)


    def do_legacy_clear_fs(self, line):

        if not zk.exists("/legacy"):
            print "Legacy not enabled"
            return

        for redis_id in zk.get_children("/redis"):

            redis = zk.get("/redis/" + redis_id)[0].split(',')

            if redis[0] == "-1":
                continue

            try:
                r = pyredis.StrictRedis(host=redis[1],port=redis[2])
                r.flushall()
            except:
                continue



    def do_update_services(self, line):

        print "Give list of hosts"
        hosts = raw_input().strip().split(',')

        print "Which service do you want to enable [redis/zookeeper/worker] ?"
        service = raw_input().strip()

        if service not in ['redis','zookeeper','worker']:
            print "invalid service"

        for host in hosts:

            if not zk.exists("/services/{}".format(host.strip())):
                print "Host {} does not exist".format(host.strip())
                continue

            if service == "redis":
                if not zk.exists("/services/{}/redis".format(host.strip())):
                    zk.create("/services/{}/redis".format(host.strip()), str(0), makepath=True)
            else:
                zk.ensure_path("/services/{}/{}".format(host.strip(),service))



    def do_ls(self, line):
        if len(line.strip()) == 0:
            print "Please give directory"
            return

        try:
            children = zk.get_children(line)
            children.sort()
            print "\n".join(children)
            print "({} files/directories)".format(len(children))
        except kazoo.exceptions.NoNodeError:
            print "ZNode {} not found".format(line)

    def do_get(self, line):
        if len(line.strip()) == 0:
            print "Please give path"
            return

        try:
            print zk.get(line)
        except kazoo.exceptions.NoNodeError:
            print "ZNode {} not found".format(line)

    def do_cancel(self, line):
        # pass

        if zk.exists("/reservations"):
            for child in zk.get_children("/reservations"):
                no = zk.get("/reservations/" + child )[0].strip()
                os.system("preserve -c {}".format(no))

        no = file_content(config.get('general','reservation_file'))
        os.system("preserve -c {}".format(no))

        return True

    def do_randomise_weights(self, line):
        nodeclasses = zk.get("/nodeclasses")[0].split(',')

        num_classes = int(zk.get("/nodeclasses")[0].split(',')[0])

        new = [str(num_classes)]
        for i in range(num_classes):
            weight = random.randint(0, 10000000)
            print "New weight {} for class {}".format(i, weight)
            new.append(str(weight))

        zk.set("/nodeclasses", ','.join(new))

    def do_add_nodes(self, line):

        num_hosts = get_integer("How many hosts do you want to add?", 1, 64)

        reserve_nodes(config.get('general','temp_reservation_file'), num_hosts, False)
        hosts = file_content(config.get('general','temp_hosts_file')).strip().split('\n')
        for host in hosts:
            try:
                zk.create("/services/{}/redis".format(host), str(0), makepath=True)
                zk.create("/services/{}/worker".format(host), str(0), makepath=True)
            except:
                print "Something went wrong for host {}".format(host)

        reservation_no = file_content(config.get('general','temp_reservation_file'))

        zk.create("/reservations/reservation", reservation_no, sequence = True, makepath=True)

    def do_fscks(self, line):

        fscks = zk.get_children("/fsck")
        if len(fscks) == 0:
            print "No fscks"
            return

        fscks.sort()
        print "{: <16}{: <16}{: <16}{: <16}".format("Fsck ID", "Host", "State", "Nodes")

        for fsck_id in fscks:
            fsck = pickle.loads(zk.get("/fsck/{}".format(fsck_id))[0])
            print "{: <16}{: <16}{: <16}{: <16}".format(fsck_id,fsck["host"],fsck["state"], ','.join(map(str,fsck["participants"])))


    def do_fsck_info(self, line):
        '''Give some info about the fsck'''
        try:
            fpath = "/fsck/fsck{:010d}".format(int(line))
            f = pickle.loads(zk.get(fpath)[0])
        except:
            print "Fsck {} not found or invalid".format(line)
            return

        print "\n{: <15}{: <10}".format("State:", f["state"])
        print "{: <15}{: <10}".format("Redis_id:", f["redis_id"])
        print "{: <15}{: <10}".format("Host:", f["host"])
        print "{: <15}{: <10}".format("Deleted files:", f["deleted_files"])

        print "\nParticipants:"

        for p in f["participants"]:
            try:
                pinfo = zk.get("{}/redis_{}".format(fpath, p))[0]
                print "{: <15}{: <10}".format(p, pinfo)
            except:
                print "{: <15}{: <10}".format(p, "-")
        print ""

    def do_available_workflows(self, line):

        print "\n".join(os.listdir("{}/workflows/".format(os.getenv('HOME'))))

    def do_workflows(self, line):

        workflows = zk.get_children("/workflows")

        if len(workflows) == 0:
            print "No workflows added yet"
            return

        workflows.sort()

        print "{: <20}{: <24}{: <12}{: <16}{: <16}{: <26}{: <16}{: <32}".format("Workflow ID", "Name", "Num tasks", "State", "Start", "(timestamp)", "End", "Duration")

        for workflow in workflows:
            data, stat = zk.get("/workflows/" + workflow)

            # try:
            wdict    = pickle.loads(data)
            start    = datetime.datetime.fromtimestamp(wdict["started"]).strftime("%d/%m %H:%M:%S") if wdict["started"] != 0 else "---"
            start_t  = "{:10.10f}".format(wdict["started"]) if wdict["started"] != 0 else  "---"
            end      = datetime.datetime.fromtimestamp(wdict["finished"]).strftime("%d/%m %H:%M:%S") if wdict["state"] == "finished" else "---"
            duration = "---"
            total_f  = ""

            if wdict["state"] == "finished":
                total_f = wdict["finished"] - wdict["started"]
                total = int(total_f)
                msecs = total_f - total
                hours = total // 3600
                mins  = (total - hours * 3600) // 60
                secs  = total % 60 + msecs
                duration = "{:02d}:{:02d}:{:07.4f}".format(hours, mins, secs)

            print "{: <20}{: <24}{: <12}{: <16}{: <16}{: <26}{: <16}{: <16}{: <16}".format(workflow, wdict["name"], wdict["num_tasks"], wdict["state"], start, start_t, end, duration,total_f)

            # except:
                # print "{}\t{}".format(workflow, "corrupted")

    def do_usage(self, line):

        usage = {}

        for nodeclass in zk.get("/nodeclasses")[0].split(',')[1:]:
            usage[nodeclass] = {"total" : float(0), "nodes": {} }

        for child in zk.get_children("/redis"):

            child = zk.get("/redis/" + child)[0].split(',')

            # Disabled
            if child[0] == "-1":
                continue

            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((child[1], 2000))
                u = s.recv(200)
                s.close()
            except:
                continue

            try:
                usage[child[0]]["total"] += float(u) / (1024*1024*1024)
                usage[child[0]]["nodes"][child[1]] = float(u) / (1024*1024*1024)
            except ValueError:
                usage[child[0]]["nodes"][child[1]] = "err"

        for nodeclass in usage:

            print "Nodeclass {}, total usage: {:.3f} GB".format(nodeclass, usage[nodeclass]["total"])

            for k, v in collections.OrderedDict(sorted(usage[nodeclass]["nodes"].items())).iteritems():
                try:
                    print "      {: <12} usage: {:.3f} GB".format(k, v)
                except ValueError:
                    print "      {: <12} error".format(k, v)


    def do_scheduling(self, line):

        # try:
        leaders = zk.get_children("/leaders")
        leaders.sort()

        if len(leaders) == 0:
            print "No leader"
            return

        leader = zk.get("/leaders/" + leaders[0])[0]

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((leader, 2001))
            workers = s.recv(200000)
            s.close()
            workers = pickle.loads(workers)
        except socket.error:
            print "Couldn't connect to scheduler {}".format(leader)
            return
        except Exception as ex:
            print "Something went wrong {}".format(ex)
            return

        print "{: <16}{: <12}{}".format("Worker", "Num tasks", "Tasks")

        i = 0
        for worker, tasks in workers.iteritems():
            print "{: <16}{: <12}{}".format(worker, len(tasks), ",".join(tasks))
            i = i+1

        print "({} workers)".format(i)

    def do_debug_scheduler(self, line):
        # try:
        leaders = zk.get_children("/leaders")
        leaders.sort()

        if len(leaders) == 0:
            print "No leader"
            return

        leader = zk.get("/leaders/" + leaders[0])[0]

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((leader, 2002))
            tasks = s.recv(2000000)
            s.close()
            tasks = pickle.loads(tasks)
        except socket.error:
            print "Couldn't connect to scheduler {}".format(leader)
            return
        except Exception as ex:
            print "Something went wrong {}".format(ex)
            return

        print "{: <16}{: <4}{}".format("Category", "#", "Tasks")
        for cat in tasks:
            print "{: <16}{: <4}{: <12}".format(cat, len(tasks[cat]), ",".join([ "{}".format(t) for t in tasks[cat]]))


    def do_iotester_read(self, line):

        self.do_add_workflow("prepare_read1.dax")

        for i in range(10):
            self.do_add_workflow("iotester_read1_1M.dax")

        for i in range(10):
            self.do_add_workflow("iotester_read1_10M.dax")

        for i in range(10):
            self.do_add_workflow("iotester_read1_100M.dax")

        self.do_add_workflow("prepare_readn.dax")

        for i in range(10):
            self.do_add_workflow("iotester_readn_1M.dax")

        for i in range(10):
            self.do_add_workflow("iotester_readn_10M.dax")

        for i in range(10):
            self.do_add_workflow("iotester_readn_100M.dax")

    def do_iotester_write(self, line):

        self.do_add_workflow("sleep_15000.dax")

        for i in range(5):
            self.do_add_workflow("iotester_write1_1M.dax")
            self.do_add_workflow("clear_write1_1M.dax")

        for i in range(5):
            self.do_add_workflow("iotester_write1_10M.dax")
            self.do_add_workflow("clear_write1_10M.dax")

        for i in range(5):
            self.do_add_workflow("iotester_write1_100M.dax")
            self.do_add_workflow("clear_write1_100M.dax")

        for i in range(5):
            self.do_add_workflow("touch_write1_1M.dax")
            self.do_add_workflow("iotester_write1p_1M.dax")
            self.do_add_workflow("clear_write1_1M.dax")

        for i in range(5):
            self.do_add_workflow("touch_write1_10M.dax")
            self.do_add_workflow("iotester_write1p_10M.dax")
            self.do_add_workflow("clear_write1_10M.dax")

        for i in range(5):
            self.do_add_workflow("touch_write1_100M.dax")
            self.do_add_workflow("iotester_write1p_100M.dax")
            self.do_add_workflow("clear_write1_100M.dax")

    def do_add_workflow(self, line):
        """add_workflow [file] \nAdd a workflow to the queue"""
        if line.strip() == "":
            print "Please provide the workflow file as an argument"
            return

        filename = os.path.abspath("{}/{}".format(config.get("general","workflows_dir"),line))
        if not os.path.isfile(filename):
            print "Could not find file {}".format(filename)

        try:
            dag = ET.parse(filename)
            root = dag.getroot()
        except:
            print "{} is not a valid XML-file.".format(filename)
            return

        num_tasks = sum(e.tag == "{http://pegasus.isi.edu/schema/DAX}job" for e in root.getchildren() )
        if num_tasks == 0:
            print "{} does not contain any tasks.".format(filename)
            return

        wdict = {"state" : "submitted", "num_tasks" : num_tasks, "name" : root.attrib["name"], "started": 0, "finished" : 0}
        zkid = zk.create("/workflows/workflow", "", sequence = True)
        print "Added workflow {} as {} containing {} tasks.".format(filename, zkid, num_tasks)

        handle = open(filename, 'r')
        while True:
            part = handle.read(900000)

            if part == "":
                break
            zk.create(zkid + "/part", part, sequence = True)
        handle.close()

        # Update to indicate all parts are there
        zk.set(zkid, pickle.dumps(wdict))


    def do_reset_workflow(self, line):

        try:
            workflow =  "workflow{:010d}".format(int(line))
            info = pickle.loads(zk.get("/workflows/" + workflow)[0])

            if info["state"] == "finished" or info["state"] == "failed":
                info["state"] = "submitted"
                zk.set("/workflows/" + workflow, pickle.dumps(info))

                print "Workflow state is submitted again.."
            else:
                print "Still busy"
        except:
            print "Workflow not found"

    def do_continue_workflow(self, line):

        try:
            workflow =  "workflow{:010d}".format(int(line))
            info = pickle.loads(zk.get("/workflows/" + workflow)[0])

            if info["state"] == "failed":
                info["state"] = "retry"
                zk.set("/workflows/" + workflow, pickle.dumps(info))

                print "Workflow state is running again.."
            else:
                print "Still busy"
        except:
            print "Workflow not found"

    def do_rm(self, filename):

        try:
            zk.delete(filename)
            print "Deleted succesfully"
        except kazoo.exceptions.NoNodeError:
            print "Znode does not exist"
        except kazoo.exceptions.NotEmptyError:
            print "Znode has children"

    def do_clear(self, line):

        if not get_yes_or_no("Do you want to clear the entire filesystem?"):
            return

        zk.ensure_path("/fuse_block")

        for redis_id in zk.get_children("/redis"):

            redis = zk.get("/redis/" + redis_id)[0].split(',')

            if redis[0] == "-1":
                continue

            try:
                r = pyredis.StrictRedis(host=redis[1],port=redis[2])
                r.flushall()
            except:
                continue

            for child in zk.get_children("/redis/" + redis_id):
                try:
                    zk.delete("/redis/{}/{}".format(redis_id, child))
                except:
                    continue

        for filename in zk.get_children("/data"):
            zk.delete("/data/" + filename, recursive = True)

        zk.delete("/fuse_block")

    def do_restart(self, line):

        # TODO, add all reservations

        redis = []
        zookeeper = []
        workers = []
        hosts = []
        leaders = []

        for host in zk.get_children("/services"):
            if zk.exists("/services/{}/zookeeper".format(host)):
                zookeeper.append(host)

            if zk.exists("/services/{}/worker".format(host)):
                workers.append(host)

            if zk.exists("/services/{}/redis".format(host)):
                redis.append(host)

            hosts.append(host)

        global zk_hosts
        zk_hosts = ','.join(["{}:2181".format(host) for host in zookeeper])

        zk.stop()

        if platform == "das4":
            work =  ["ssh {} \"ps aux | grep ^{} | egrep -v 'ps aux|-bash|sshd|sleep|drmaa_slurm_script|grep|awk|tail' | awk '{{ print \$2 }}' | xargs kill\" &> /dev/null".format(host,os.getlogin()) for host in hosts]
        else:
            work =  ["ssh {} \"ps aux | grep ^{} | egrep -v 'ps aux|-bash|sh|sshd|sleep|drmaa_slurm_script|grep|awk|tail' | awk '{{ print \$2 }}' | xargs kill\" &> /dev/null".format(host,os.getlogin()) for host in hosts]


        pool = Pool(processes = 8)
        outputs = pool.map(do_work, work)
        pool.terminate()

        print "Zzzz"
        time.sleep(4)

        start_zookeeper(zookeeper)
        zk.set_hosts(zk_hosts)
        zk.start()
        initialise_zookeeper()

        global latest_log
        latest_log = len(zk.get_children("/logs", watch=logs_watch))

        zk.create("/services")

        for host in hosts:
            try:
                zk.create("/services/{}".format(host))
                if host in zookeeper:
                    zk.create("/services/{}/zookeeper".format(host))
                if host in workers:
                    zk.create("/services/{}/worker".format(host))
                if host in redis:
                    zk.create("/services/{}/redis".format(host), str(0))
            except:
                print "Something went wrong for host {}".format(host)


    def do_legacy(self, line):

        if zk.exists("/legacy"):
            if get_yes_or_no("Do you want to disable legacy mode?"):
                zk.delete("/legacy")
            return

        if not get_yes_or_no("Do you want to enable legacy mode?"):
            return

        print "Enabling!"

        redises = []

        for host in zk.get_children("/services"):
            if zk.exists("/services/{}/redis".format(host)):
                redises.append(host)

        f = open(config.get('legacy','redis_table'),'w')
        f.write("{} 1\n".format(len(redises)))
        f.write("0 {}\n".format(len(redises)-1))
        f.write("0\n")
        for host in redises:
            f.write("{}:{}\n".format(host,config.get('redis','port')))
        f.close()

        zk.ensure_path("/legacy")


    def do_create(self, line):

        zk.ensure_path(line)

    def do_file_info(self, line):

        try:
            zinode = utils.hrw.get_zinode(zk, line)
            print zinode
        # hashes = utils.hrw.get_hashes(zk)
        except:
            print "This is not a valid entry"

    def do_start_localmanagers(self, line):
        print "starting local managers?"

        to_start = []
        for host in zk.get_children("/services"):
            if not zk.exists("/online_hosts/{}".format(host)):
                to_start.append(host)

        start_localmanagers(to_start)

    def do_redises(self, line):
        redises = zk.get_children("/redis")
        if len(redises) == 0:
            print "No redis servers"
            return

        redises.sort()
        print "{: <20}{: <12}{: <16}{: <8}".format("Redis ID", "Nodeclass", "Host", "Port")

        for redis_id in redises:
            redis = zk.get("/redis/{}".format(redis_id))[0].split(',')
            print "{: <20}{: <12}{: <16}{: <8}".format(redis_id,redis[0], redis[1], redis[2])

    def do_logs(self, line):
        logs = zk.get_children("/logs")
        if len(logs) == 0:
            print "No logs"
            return

        logs.sort()
        print "{: <10}{: <16}{: <16}{: <26}{: <40}".format("Severity", "Host", "Time","Timestamp", "Message")

        for log_id in logs:
            log = zk.get("/logs/{}".format(log_id))[0]

            if log[0] == '(':
                log = pickle.loads(log)
            else:
                s =  log.split(';')
                log = {"severity" : s[0], "ip": s[1], "time": float(s[2]), "message": s[3]}

            print "{: <10}{: <16}{: <16}{: <26}{: <40}".format(log["severity"],log["ip"],datetime.datetime.fromtimestamp(log["time"]).strftime("%d/%m %H:%M:%S"),"{:10.10f}".format(log["time"]),log["message"])


    def do_services(self, line):

        print "{: <16}{: <12}{: <8}{: <8}{: <8}{: <8}".format("Host","Zookeeper","Redis","Worker","Leader","Online")

        hosts = zk.get_children("/services")
        hosts.sort()

        num_workers = 0

        for host in hosts:

            print "{: <16}{: <12}{: <8}{: <8}{: <8}{: <8}".format(host,
            "X" if zk.exists("/services/{}/zookeeper".format(host)) else "",
            "X" if zk.exists("/services/{}/redis".format(host)) else "",
            "X" if zk.exists("/services/{}/worker".format(host)) else "",
            "X" if zk.exists("/services/{}/leader".format(host)) else "",
            "X" if zk.exists("/online_hosts/{}".format(host)) else "")

            if zk.exists("/services/{}/worker".format(host)):
                num_workers += 1

        print "Workers: {}".format(num_workers)


def do_work(work):
    proc = subprocess.Popen(work, stdout=subprocess.PIPE, shell=True)
    (output, err) = proc.communicate()
    #print output
    return output, err

def file_content(filen):
    f = open(filen, 'r')
    c = f.read()
    f.close()
    return c

def reservation_exists(reservation_file):

    if os.path.isfile(reservation_file):
        reservation_no = file_content(reservation_file).strip()

        if not os.system("preserve -list | grep {}".format(reservation_no)):
            print "Using existing reservation {}".format(reservation_no)
            return True
        else:
            print "Reservation {} not there anymore".format(reservation_no)

    return False

def reserve_nodes(output_file, num_nodes, ssd_nodes):

    res_time = get_integer("For how long do you want to make a reservation (minutes)?", 1, 1439)

    print "Reserving {} nodes".format(num_nodes)
    os.system("{} {} {} {} {} {}".format(
                                config.get("general", "reservation_script"),
                                num_nodes,
                                output_file,
                                config.get('general','temp_hosts_file'),
                                "1" if ssd_nodes else "0",
                                datetime.timedelta(minutes=res_time)
                                ))

    return file_content(config.get('general','temp_hosts_file')).strip().split('\n')


def start_zookeeper(hosts):
    print "Starting zookeeper"

    zookeeper_conf = config.get('zookeeper','config_file')
    client_port = config.get('zookeeper','clientPort')

    f = open(config.get('zookeeper','config_file'),'w')
    f.write("tickTime={}\n".format(config.get('zookeeper','tickTime')))
    f.write("dataDir={}\n".format(config.get('zookeeper','dataDir')))
    f.write("clientPort={}\n".format(client_port))
    f.write("initLimit={}\n".format(config.get('zookeeper','initLimit')))
    f.write("syncLimit={}\n".format(config.get('zookeeper','syncLimit')))
    f.write("maxClientCnxns={}\n".format(config.get('zookeeper','maxClientCnxns')))
    for host in hosts:
        f.write("server.{}={}:2888:3888\n".format(host.split('.')[3], host))
    f.close()

    f = open(config.get('zookeeper','hosts_file'),'w')
    f.write(",".join(["{}:{}".format(host,client_port) for host in hosts]))
    f.close()

    work = ["{}/ftmemfs/scripts/start_zookeeper.sh {} {} {} {}".format(
            os.getenv('HOME'),
            host,
            config.get('zookeeper','directory'),
            config.get('zookeeper','target_dir'),
            config.get('zookeeper','config_file'))
            for host in hosts]

    pool = Pool(processes = 8)
    outputs = pool.map(do_work, work)
    pool.terminate()

    print "Waiting..."

    time.sleep(3)

def initialise_zookeeper():
    print "Initialising zookeeper"

    if zk.exists("/data"):
        print "Already initialised"
        return

    zk.ensure_path("/leaders")
    zk.create("/data", b"d")
    zk.create("/online_hosts")
    zk.create("/redis")
    zk.create("/nodeclasses",b"1,0")
    zk.create("/fsck")
    zk.create("/suspicions")
    zk.create("/workers")
    zk.create("/logs")
    zk.ensure_path("/workflows");


def start_localmanagers(hosts):

    start_hosts = []

    if not get_yes_or_no("Do you want to start all local managers?"):
        for host in hosts:
            if get_yes_or_no("Do you want to start {}?".format(host)):
                start_hosts.append(host)
    else:
        start_hosts = hosts

    print "Starting localmanagers at: {}".format(", ".join(start_hosts))

    work = ["{}/ftmemfs/scripts/start_localmanager.sh {} {}".format(
            os.getenv('HOME'),
            host,
            zk_hosts)
            for host in start_hosts]

    pool = Pool(processes = int(config.get('worker','nr_colocated_tasks')))
    outputs = pool.map(do_work, work)
    pool.terminate()

def logs_watch(data):

    children = zk.get_children("/logs", watch=logs_watch)

    print ""

    global latest_log

    for i in range(latest_log, len(children)):

        log = zk.get("/logs/log{:010d}".format(i))[0]

        if log[0] == '(':
            log = pickle.loads(log)
        else:
            s =  log.split(';')
            log = {"severity" : s[0], "ip": s[1], "time": float(s[2]), "message": s[3]}

        print "[{}][{}][{}] {}".format(log["severity"],log["ip"],time.strftime("%d/%m %H:%M:%S", time.localtime(log["time"])),log["message"])


    latest_log = len(children)

    sys.stdout.write("(Cmd) ")
    sys.stdout.flush()

def get_integer(question, min, max):

    while True:
        try:
            num = int(raw_input("{} [{}-{}] ".format(question, min, max)))

            if num >= min and num <= max:
                return num
        except:
            pass

        print "Invalid input"

def get_yes_or_no(question):

    while True:
        try:
            answer = raw_input("{} [yes/no] ".format(question)).strip()

            if answer == "y" or answer == "yes":
                return True

            if answer == "n" or answer == "no":
                return False

        except:
            pass

        print "Invalid input"


def get_services():
    pass

if __name__ == "__main__":
    logging.basicConfig()

    global zk
    global zk_hosts
    global latest_log

    config = ConfigParser.ConfigParser()
    config.read("{}/ftmemfs/config.ini".format(os.getenv("HOME")))
    config.read("{}/ftmemfs/config_platform.ini".format(os.getenv("HOME")))

    try:
        platform = config.get("general", "platform")
    except ConfigParser.NoOptionError:
        print "Platform missing, please create config_platform.ini"
        sys.exit(0)

    print "Platform: {}".format(platform)

    reservation_file = config.get('general','reservation_file')

    # Check if the reservation is still valid
    if not reservation_exists(reservation_file):

        num_zookeeper_hosts = get_integer("How many zookeeper hosts do you want?", 1, 8)
        hosts = reserve_nodes(reservation_file, num_zookeeper_hosts, True)

        start_zookeeper(hosts)

        zk_hosts = file_content(config.get('zookeeper','hosts_file'))
        zk = KazooClient(zk_hosts)
        zk.start()

        initialise_zookeeper()

        for host in hosts:
            zk.ensure_path("/services/{}/zookeeper".format(host))
            zk.ensure_path("/services/{}/worker".format(host))
            zk.create("/services/{}/redis".format(host), str(0))

    else:
         zk_hosts = file_content(config.get('zookeeper','hosts_file'))
         zk = KazooClient(zk_hosts)
         zk.start()



    latest_log = len(zk.get_children("/logs", watch=logs_watch))

    FTCli().cmdloop()

    zk.stop()
