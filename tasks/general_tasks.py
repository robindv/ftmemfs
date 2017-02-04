import traceback
from ftmemfs.utils.zookeeper_utils import *
from ftmemfs.worker.node_scheduler import NodeScheduler

class FtmemfsTask(object):

    def execute(self, lm):
        print "Not implemented task!"


class CheckLeaderTask(FtmemfsTask):

    def __init__(self, leaders):
        self.leaders = leaders

    def __repr__(self):
        return "CheckLeaderTask({})".format(self.leaders)

    def execute(self, lm):
        lm.leader_lock.acquire()

        if lm.ldr is not None:
            print "We are the leader, skipping!"
            lm.leader_lock.release()
            return

        leaders = self.leaders

        if lm.potential_leader and is_zookeeper_leader(lm.zk):
            lm.add_log_entry("INFO", "Become leader, announce")

            print "We are the leader now!"

            lm.my_leader_path = lm.zk.create("/leaders/leader", lm.ip, sequence=True, ephemeral=True)
            lm.leader = lm.ip

            # TODO make this another task
            lm.start_leader()

            lm.leader_lock.release()
            return

        if len(leaders) != 0:
            leaders.sort()
            try:
                new_leader = lm.zk.get("/leaders/" + leaders[0])[0]

                # New leader
                if lm.leader != new_leader:
                    lm.leader = new_leader

                    if lm.is_worker:
                        if lm.worker == None:
                            lm.worker = NodeScheduler(lm.leader, lm)
                            print "Starting worker!"
                            lm.worker.start()
                        else:
                            print "We should change the ip of the leader, it is now {}".format(lm.leader)
                            lm.worker.change_ip(lm.leader)
            except Exception as e:
                print e
                traceback.print_exc()
        else:
            print "No clue who the leader is for now.."

        lm.leader_lock.release()
