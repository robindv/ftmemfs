import xml.etree.cElementTree as ET
import time

class Workflow(object):

    def __init__(self, zk, workflow_id):

        print "Loading workflow {}".format(workflow_id)

        parts = zk.get_children("/workflows/" + workflow_id)
        parts.sort()

        xml_string = ""

        for part in parts:
            xml_string += zk.get("/workflows/{}/{}".format(workflow_id, part))[0]

        # print "XML string filled!"

        self.zk = zk
        self.adag              = ET.fromstring(xml_string)

        # print "DAG readed!"
        self.tasks             = set()
        self.name              = self.adag.attrib["name"]
        self.output_files      = {}
        self.input_files       = {}
        self.task_output_files = {}
        self.parents           = {}
        self.children          = {}
        self.commands          = {}

        for child in self.adag.getchildren():
            if "job" in child.tag:
                task_id = child.attrib["id"]
                self.tasks.add(task_id)

                self.parents[task_id]  = set()
                self.children[task_id] = set()

                self.commands[task_id] = self.task_to_dict(task_id, child)

                for filen in child.getchildren():
                    if "uses" in filen.tag:

                        if filen.attrib["link"] == "output":
                            self.output_files[filen.attrib["name"]] = task_id

                        elif filen.attrib["link"] == "input":

                            if filen.attrib["name"] in self.input_files:
                                self.input_files[filen.attrib["name"]].add(task_id)
                            else:
                                self.input_files[filen.attrib["name"]] = set([task_id])
            else:
                for parent in child.getchildren():
                    self.parents[child.attrib["ref"]].add(parent.attrib["ref"])
                    self.children[parent.attrib["ref"]].add(child.attrib["ref"])

        self.num_tasks    = len(self.tasks)
        self.output_tasks = set()
        for parent, children in self.children.iteritems():
            if len(children) == 0:
                self.output_tasks.add(parent)


    def task_can_run(self, task):

        for parent in self.parents[task]:
            if not self.task_is_complete(parent):
                print "Parent {} not complete!".format(parent)
                return False

        return True

    def task_is_complete(self, task):

        d = self.commands[task]

        for output_file in d["output_files"]:
            if not self.zk.exists("/data/{}/written"):
                print "output file {} of task {} doesnt exist, not completed".format(output_file, task)
                return False

        return True

    def recover(self):
        print "Recovering from previous execution"

        files = set()
        finished = set()

        for filename in self.output_files:
            if self.zk.exists("/data/{}/written".format(filename)):
                files.add(filename)

        for child in self.adag.getchildren():
            if "job" in child.tag:
                complete = True
                for filen in child.getchildren():
                    if "uses" in filen.tag:
                        if filen.attrib["link"] == "output":
                            if filen.attrib["name"] not in files:
                                complete = False
                if complete:
                    finished.add(child.attrib["id"])

        print "{} tasks already finished".format(len(finished))
        return finished

    def is_finished(self, finished):

        unfinished_tasks = [task for task in self.output_tasks if task not in finished]

        return len(unfinished_tasks) == 0

    def task_to_dict(self, task_id, job):
        '''Get the dictionary containg the command belonging to the given
           task id.'''

        answer = {'id': task_id, 'argument': "", "params": ""}

        answer["qid"] = 1 # find out what this is
        answer["exec"] = job.attrib["name"]

        arg = job.find("{http://pegasus.isi.edu/schema/DAX}argument")

        params = []

        if arg is not None:
            for elem in arg.iter():
                if elem.text != None:
                    params.append(elem.text)
                if elem.tag == "{http://pegasus.isi.edu/schema/DAX}file":
                    params.append(elem.attrib["name"])
                if elem.tail != None and elem.tag != "{http://pegasus.isi.edu/schema/DAX}argument":
                    params.append(elem.tail)

        stdout = job.find("{http://pegasus.isi.edu/schema/DAX}stdout")
        if stdout is not None:
            params.append(' > ' + stdout.attrib["name"])

        answer["params"] = ''.join(params)

        output_files = []

        # get output files
        for filen in job.getchildren():
            if "uses" in filen.tag:
                if filen.attrib["link"] == "output":
                    output_files.append(filen.attrib["name"])
        answer["output_files"] = output_files

        return answer

    def det_runnable_tasks(self, finished):
        '''Determine which tasks can run, aiming for the potential tasks using
        an array of already finished tasks '''

        print "Determine runnable tasks"

        runnable = set()
        waiting  = set()
        potential     = self.output_tasks.copy()

        i = 0

        while len(potential) > 0:

            task = potential.pop()

            if task in finished:
                continue

            parents_finished = True
            for parent in self.parents[task]:
                if parent not in finished:
                    parents_finished = False
                    if parent not in potential and parent not in waiting:
                        potential.add(parent)

            if parents_finished:
                if task not in runnable:
                    runnable.add(task)
            else:
                waiting.add(task)

        print "Determine runnable tasks - done"
        return runnable, waiting

    def next_iteration(self, finished, waiting, just_finished):
        '''Given finished tasks, update the runnable list'''

        new_runnable = []

        for task in just_finished:
            for child in self.children[task]:
                parents_finished = True
                for parent in self.parents[child]:
                    if parent not in finished:
                        parents_finished = False
                        break
                if parents_finished and child in waiting:
                    # self.runnable.add(child)
                    waiting.remove(child)
                    new_runnable.append(child)

        return new_runnable

    def process_lost_files(self, lost_files, finished):
        print "Process {} lost files!".format(len(lost_files))



        for filename in lost_files:
            if filename in self.output_files:
                task = self.output_files[filename]
                if task in finished:
                    finished.remove(task)

                    print "Removed {} from finished".format(task)
