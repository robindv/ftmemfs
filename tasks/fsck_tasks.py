import pickle
import ftmemfs.etc.config
from ftmemfs.tasks.general_tasks import FtmemfsTask
from ftmemfs.utils.hrw import *
import redis
import traceback
from kazoo.exceptions import *

class FsckEnqueueTask(FtmemfsTask):
    def __init__(self, children):
        self.children = children

    def __repr__(self):
        return "FsckEnqueueTask({})".format(self.children)

    def execute(self, lm):
        for fsck in self.children:
            if fsck in lm.fscks_handled:
                continue

            f = pickle.loads(lm.zk.get("/fsck/{}".format(fsck))[0])

            if f["state"] != "finished" and lm.redis_seq in f["participants"]:
                lm.queue.put((100, FsckTask(fsck)))

            lm.fscks_handled.append(fsck)


class FsckTask(FtmemfsTask):

    def __init__(self, id):
        self.id = id
        self.redises = {}

    def __repr__(self):
        return "FsckTask({})".format(self.id)

    def execute(self, lm):

        print "Doing fsck! {}".format(self.id)

        fsck = pickle.loads(lm.zk.get("/fsck/{}".format(self.id))[0])

        if lm.zk.exists("/fsck/{}/redis_{}".format(self.id, lm.redis_seq)):
            state = lm.zk.get("/fsck/{}/redis_{}".format(self.id, lm.redis_seq))[0]

            if state == "finished":
                print "done!"
                return
            else:

                if state == "started":
                    todo = [1,2]
                else:
                    todo = [2]

        else:
            # Create
            lm.zk.create("/fsck/{}/redis_{}".format(self.id, lm.redis_seq), "started")

            todo = [1,2]

        if 1 in todo:
            to_remove = self.step_1(lm)
            # print to_remove

            transaction = lm.zk.transaction()
            transaction.set_data("/fsck/{}/redis_{}".format(self.id, lm.redis_seq), "cleaning")
            if len(to_remove) != 0:
                transaction.create("/fsck/{}/removed/redis_{}".format(self.id, lm.redis_seq))

                to_store = pickle.dumps(to_remove)
                for i in range(0, len(to_store), 900000):
                    transaction.create("/fsck/{}/removed/redis_{}/removed".format(self.id, lm.redis_seq), to_store[i:i+900000], sequence=True)
            else:
                print "nothing to remove"

            # print transaction.commit()
            # TODO check transaction!?

        # Strangely enough, we are recovering or something like that? Whatever, continue..
        else:
            to_remove = []
            if lm.zk.exists("/fsck/{}/removed/redis_{}".format(self.id, lm.redis_seq)):
                # TODO: split

                children = lm.zk.get_children("/fsck/{}/removed/redis_{}".format(self.id, lm.redis_seq))

                load_s = ""
                for i in range(len(children)):
                    load_s += lm.zk.get("/fsck/{}/removed/redis_{}/removed{:010d}".format(self.id, lm.redis_seq, i))[0]
                to_remove = pickle.loads(load_s)

        self.step_2(lm, to_remove)

        lm.zk.set("/fsck/{}/redis_{}".format(self.id, lm.redis_seq), "finished")

        return


    def step_1(self, lm):
        '''Find the files that have to be removed, and remove the keys
           from the redis server, but do not remove it from the reverse index
           yet. '''

        max_chunk_size = ftmemfs.etc.config.FTMEMFS_MAX_CHUNK_SIZE
        hashes =  get_hashes(lm.zk)
        remove_files = []

        our_redis = lm.zk.get("/redis/redis{:010d}".format(lm.redis_seq))[0].split(',')
        target_redis_seq = int(self.id[5:])
        target_redis = lm.zk.get("/redis/redis{:010d}".format(target_redis_seq))[0].split(',')


        # print "our_redis: {}".format(our_redis)
        # print "target_redis: {}".format(target_redis)

        # Connect to our own redis server
        self.redises[lm.redis_seq] = redis.StrictRedis(host=our_redis[1], port=our_redis[2])

        ### Step 1
        # Get a list of all files that are present on the target_redis
        for raw_filename in lm.zk.get_children("/redis/redis{:010d}".format(target_redis_seq)):
            # Fix directory separator
            filename =  raw_filename.replace("$_$",'/')
            # print "checking if {} is present on our redis".format(filename)

            for key in self.redises[lm.redis_seq].keys("{}___chunk-*".format(filename)):
                # print "*removing key* {}".format(key)
                try:
                    self.redises[lm.redis_seq].delete(key)
                except:
                    print "whoops"

        # print "\n\n\nStep 2 - check own reverse index\n\n\n"

        ### Step 2
        # Check our own reverse index
        for raw_filename in lm.zk.get_children("/redis/redis{:010d}".format(lm.redis_seq)):
            filename =  raw_filename.replace("$_$",'/')

            # print "Checking {}".format(filename)

            zinode = get_zinode(lm.zk, filename)

            do_delete = False

            if lm.zk.exists("/data/{}/written".format(filename)):
                # // does division with floor, double minus makes it a ceil :-)
                num_nodes = -(-zinode["size"] // max_chunk_size)
                # print "File should consist of {} nodes".format(num_nodes)
                for i in range(num_nodes):
                    if get_server_for_chunk(filename, i, zinode, hashes) == target_redis_seq:
                        do_delete = True
            else:
                # File currently written, we should remove because we do not know how big it is
                if target_redis_seq in zinode["servers"]:
                    do_delete = True

            if not do_delete:
                continue

            # print "File {} should be deleted".format(filename)
            remove_files.append(filename)

        return remove_files


    def step_2(self, lm, do_remove):

        # print "cleaning!"

        for filename in do_remove:

            zinode = get_zinode(lm.zk, filename)
            raw_filename = filename.replace('/',"$_$")

            # Already deleted
            if zinode is not None:
                target_redis_seq = int(self.id[5:])
                self.remove_segments(lm, raw_filename, zinode, target_redis_seq)
                while True:
                    try:
                        lm.zk.delete("/data/" + filename, recursive=True)
                        break
                    except NotEmptyError:
                        # print "Could not delete {}, not empty".format(filename)
                        continue
                    except:
                        break
            try:
                lm.zk.delete("/redis/redis{:010d}/{}".format(lm.redis_seq, raw_filename))
            except:
                pass



    def remove_segments(self, lm, filename, zinode, target_redis_seq):
        '''Remove all segments of a given file'''

        # print "trying to remove file {}".format(filename)

        for server in zinode["servers"]:
            try:
                if server == target_redis_seq:
                    continue

                if server not in self.redises:
                    rinfo = lm.zk.get("/redis/redis{:010d}".format(server))[0].split(',')

                    # Disabled host
                    if rinfo[0] == -1:
                        continue

                    self.redises[server] = redis.StrictRedis(host=rinfo[1], port=rinfo[2])

                for key in self.redises[server].keys("{}___chunk-*".format(filename)):
                    # print "*removing key* {} on server {}".format(key, server)
                    self.redises[server].delete(key)
                    continue
            except:
                print "Something went wrong, do we care?"
                traceback.print_exc()
                # TODO, probably we do..
