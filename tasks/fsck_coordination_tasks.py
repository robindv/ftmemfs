import pickle
from ftmemfs.tasks.general_tasks import FtmemfsTask
from ftmemfs.utils.hrw import *
import redis
import traceback
import threading
import kazoo.exceptions

class FsckWatchTask(FtmemfsTask):

    def __init__(self, children):
        self.children = children

    def __repr__(self):
        return "FsckWatchTask({})".format(self.children)

    def execute(self, lm):

        for child in self.children:

            if child in lm.ldr.watched_fscks:
                continue

            f = pickle.loads(lm.zk.get("/fsck/{}".format(child))[0])

            if f["state"] == "finished":
                continue

            lm.ldr.fscks[child] = {"zk_dict" : f, "finished": []}
            lm.ldr.watched_fscks.append(child)

            if f["state"] == "cleaning":
                lm.queue.put((50, FsckFinishTask(child)))
            else:
                lm.queue.put((20, FsckDataWatchTask(child)))


class FsckDataWatchTask(FtmemfsTask):

    def __init__(self, fsck):
        self.fsck = fsck

    def __repr__(self):
        return "FsckDataWatchTask({})".format(self.fsck)

    def execute(self, lm):

        f_state = lm.ldr.fscks[self.fsck]

        done = True

        for p in f_state["zk_dict"]["participants"]:
            if p not in f_state["finished"]:

                # Okay, fscked host, ignore
                if lm.zk.exists("/fsck/fsck{:010d}".format(p)):
                    continue

                try:
                    state = lm.zk.get("/fsck/{}/redis_{}".format(self.fsck, p))[0]
                    if state == "finished":
                        f_state["finished"].append(p)
                    else:
                        # print "{} State is: {}".format(p, state)
                        done = False
                        break
                except kazoo.exceptions.NoNodeError:
                    # print "node /fsck/{}/redis_{} doesnt exist".format(self.fsck, p)
                    done = False
                    break



        if not done:
            # print "Not yet finished"
            # Because watches were not working here, we check every 0.5 second ;-)
            threading.Timer(0.5, lambda : lm.queue.put((20,  FsckDataWatchTask(self.fsck)))).start()
            return


        f_state["zk_dict"]["state"] = "cleaning"
        lm.zk.set("/fsck/{}".format(self.fsck),pickle.dumps(f_state["zk_dict"]))
        
        lm.queue.put((50, FsckFinishTask(self.fsck)))



class FsckFinishTask(FtmemfsTask):

    def __init__(self, fsck_id):
        self.fsck_id = fsck_id

    def __repr__(self):
        return "FsckFinishTask({})".format(self.fsck_id)

    def execute(self, lm):

        fsck = lm.ldr.fscks[self.fsck_id]["zk_dict"]

        deleted_files = []

        # Remove files from data segment
        for raw_filename in lm.zk.get_children("/redis/redis{:010d}".format(fsck['redis_id'])):
            filename = raw_filename.replace("$_$",'/')

            deleted_files.append(filename)

            try:
                if lm.zk.exists("/data/{}/written".format(filename)):
                    lm.zk.delete("/data/{}/written".format(filename))
                lm.zk.delete("/data/{}".format(filename))
            except:
                traceback.print_exc()
                pass

            try:
                lm.zk.delete("/redis/redis{:010d}/{}".format(fsck['redis_id'], raw_filename))
            except:
                traceback.print_exc()
                pass

        for child in lm.zk.get_children("/fsck/{}/removed".format(self.fsck_id)):
            children = lm.zk.get_children("/fsck/{}/removed/{}".format(self.fsck_id, child))
            load_s = ""
            for i in range(len(children)):
                load_s += lm.zk.get("/fsck/{}/removed/{}/removed{:010d}".format(self.fsck_id, child, i))[0]
            deleted_files.extend(pickle.loads(load_s))


        fsck["state"] = "finished"
        fsck["deleted_files"] = len(deleted_files)


        # Update redis class
        lm.zk.set("/redis/redis{:010d}".format(fsck['redis_id']), "{},{},{}".format(-1, fsck["host"], fsck["port"]))

        # Store final state of fsck
        lm.zk.set("/fsck/{}".format(self.fsck_id),pickle.dumps(fsck))

        # Process deleted files
        lm.ldr.scheduler.queue.put((10, "deleted_files", deleted_files))

        # print "Check if there is another ongoing fsck?"
        for f in lm.ldr.fscks:
            # print f
            # print lm.ldr.fscks
            if lm.ldr.fscks[f]["zk_dict"]["state"] != "finished":
                print "{} is still ongoing".format(f)
                return

        # No fscks, unblock
        lm.ldr.scheduler.queue.put((15, "resume"))
