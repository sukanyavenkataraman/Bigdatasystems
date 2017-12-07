'''
F3: A scheduler for F2 based on weighted minmax
Weights are based on a function of user resource quota and % containers allotted
Baselines for comparison are shortest job first, min max, weighted min max based on other weights
'''

import sys

MAX_CONTAINERS = 10
CONTAINER_SIZE = 1


class job:
    def __init__(self,
                  eachjob,
                  schedule_time=0
                  ):
        job_variables = eachjob.strip().split(',')

        self.jobID = job_variables[0]
        self.schedule_time = schedule_time
        self.container_per_partition = float(job_variables[2])
        self.total_partitions = int(job_variables[1])
        self.partitions_per_container = CONTAINER_SIZE / self.container_per_partition
        self.job_container_quota = float(job_variables[3])
        self.estimated_time_per_partition = int(job_variables[4])
        self.job_weight = 0
        self.parallelism_at_time = []


class scheduler:
    def __init__(self,
                 filename):
        '''

        :param filename:
        '''

        self.filename = filename
        self.ready_queue = []
        self.resource_at_time = [MAX_CONTAINERS] * 1000
        self.decisions = []
        self.next_time = 0

    def schedule_decision(self,
                          algo=0,
                          time=0):

        # Shorted job first
        if algo == 0:
            sorted_queue = sorted(self.ready_queue, key=lambda x: x.estimated_time_per_partition * x.total_partitions)

        ready_queue_new = []
        index = 0
        min_next_time = sys.maxint
        print 'before ', time

        for job in self.ready_queue:
            print job.jobID, job.total_partitions

        while index < len(sorted_queue):
            while sorted_queue[index].total_partitions > 0 and \
                    self.resource_at_time[time] > 0:

                sorted_queue[index].total_partitions -= sorted_queue[index].partitions_per_container
                self.resource_at_time[time] -= 1

                if time + sorted_queue[index].estimated_time_per_partition < min_next_time:
                    min_next_time = time + sorted_queue[index].estimated_time_per_partition

                for i in range(time, time + sorted_queue[index].estimated_time_per_partition):
                    self.resource_at_time[i] -= 1
                    if self.resource_at_time[i] < 0:
                        print 'ERROR: resource', self.resource_at_time[i],' at time', i, 'below 0'

            if sorted_queue[index].total_partitions > 0:
                ready_queue_new.append(sorted_queue[index])

            index += 1

        self.ready_queue = ready_queue_new
        print 'after ', time

        for job in self.ready_queue:
            print job.jobID, job.total_partitions

        if min_next_time < sys.maxint:
            self.next_time = min_next_time
            return True

        return False

    def run(self):

        with open(self.filename, 'r') as f:
            alljobs = f.readlines()

            # <time><tab><list of jobs to be scheduled at time time>
            for i in range(len(alljobs)):
                parts = alljobs[i].strip().split('\t')

                time = int(parts[0])

                while self.next_time < time:
                    if self.schedule_decision(0, self.next_time) == False:
                        break
                print 'Out of potential infinite loop 1'

                jobs_at_time = parts[1].strip().split(';')
                for eachjob in jobs_at_time:
                    print eachjob
                    self.ready_queue.append(job(eachjob, time))

                if self.next_time == time:
                    self.schedule_decision(0, time)

            if self.ready_queue == []:
                print 'Done scheduling. Total run time is'
            else:
                while self.ready_queue != []:
                    self.schedule_decision(0, self.next_time)

                print 'Out of potential infinite loop 2'

        f.close()


sch = scheduler('test.txt')
sch.run()
