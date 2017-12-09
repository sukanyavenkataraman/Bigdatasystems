'''
F3: A scheduler for F2 based on weighted minmax
Weights are based on a function of user resource quota and % containers allotted
Baselines for comparison are shortest job first, min max, weighted min max based on other weights
'''

import sys
import math
import matplotlib.pyplot as plt

MAX_CONTAINERS = 5
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
    self.partitions_per_container = math.floor(CONTAINER_SIZE / self.container_per_partition)
    self.job_container_quota = float(job_variables[3])
    self.estimated_time_per_partition = int(job_variables[4])
    self.job_weight = 0
    self.parallelism_at_time = []
    self.containers_allotted = 0

class jobInfo:
  def __init__(self,
               jobID,
               startTime):
    self.jobID = jobID
    self.containers_allotted = 0
    self.start_time = startTime
    self.end_time = startTime


class scheduler:
  def __init__(self,
      filename):
    '''

    :param filename:
    '''

    self.filename = filename
    self.ready_queue = []
    self.containers_at_time = [MAX_CONTAINERS] * 1000
    self.decisions = []
    self.next_time = 0
    self.all_jobs = {}


  def schedule_decision(self,
      algo=0,
      time=0):

    if algo == 1:
      return self.schedule_max_min(time)
    if algo == 2:
      return self.schedule_weighted_max_min(time)

    # Shortest job first
    if algo == 0:
      sorted_queue = sorted(self.ready_queue, key=lambda
          x: x.estimated_time_per_partition * x.total_partitions)

    ready_queue_new = []
    index = 0
    min_next_time = sys.maxint
    print 'before ', time

    for job in self.ready_queue:
      print job.jobID, job.total_partitions

    while index < len(sorted_queue):
      while sorted_queue[index].total_partitions > 0 and \
              self.containers_at_time[time] > 0:

        sorted_queue[index].total_partitions -= sorted_queue[
          index].partitions_per_container
        sorted_queue[index].containers_allotted += 1
        self.all_jobs[sorted_queue[index].jobID].containers_allotted += 1
        self.all_jobs[sorted_queue[index].jobID].end_time = time + sorted_queue[index].estimated_time_per_partition

        self.containers_at_time[time] -= 1

        if time + sorted_queue[
          index].estimated_time_per_partition < min_next_time:
          min_next_time = time + sorted_queue[
            index].estimated_time_per_partition

        for i in range(time + 1,
                       time + sorted_queue[index].estimated_time_per_partition):
          self.containers_at_time[i] -= 1
          if self.containers_at_time[i] < 0:
            print 'ERROR: resource', self.containers_at_time[
              i], ' at time', i, 'below 0'

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

  def normalize_allocation(self):
    sum_all = 0
    for i in self.ready_queue:
      sum_all += i.job_container_quota
    print sum_all

    for j in self.ready_queue:
      j.job_weight = (j.job_container_quota* 1.0/sum_all)
      print j.jobID, " " ,j.job_weight


  def schedule_weighted_max_min(self, time=0):
    # Containers allotted
    ready_queue_new = []

    self.normalize_allocation()


    min_next_time = sys.maxint
    print 'before ', time

    for job in self.ready_queue:
      print "jobId, partitions_left, partitions_per_container"
      print job.jobID, job.total_partitions, job.partitions_per_container

    queue_length = len(self.ready_queue)

    while queue_length > 0 and self.containers_at_time[time] > 0:

      # Containers allotted

      job = self.ready_queue.pop(0)

      print math.ceil(job.total_partitions/job.partitions_per_container), math.floor(job.job_weight * self.containers_at_time[time])
      job.containers_allotted += min(math.ceil(job.total_partitions/job.partitions_per_container), math.floor(job.job_weight * self.containers_at_time[time]))
      job.total_partitions -= job.containers_allotted * job.partitions_per_container
      print "Allotting 1 container to job %s: new size: %s" % (
        job.jobID, job.containers_allotted)

      estimated_time = job.estimated_time_per_partition
      min_next_time = min(time + estimated_time, min_next_time)

      for i in range(time + 1, time + estimated_time):
        self.containers_at_time[i] -= min(job.total_partitions/job.partitions_per_container, math.floor(job.job_weight * self.containers_at_time[time]))
        if self.containers_at_time[i] < 0:
          print 'ERROR: resource', self.containers_at_time[i], ' at time', i, 'below 0'

      if job.total_partitions > 0:
        self.ready_queue.append(job)
      queue_length -= 1
    print 'after ', time

    for job in self.ready_queue:
      print job.jobID, job.total_partitions

    if min_next_time < sys.maxint:
      self.next_time = min_next_time
      return True

    return False

  def schedule_max_min(self, time=0):

    # Containers allotted

    min_next_time = sys.maxint
    print 'before ', time

    for job in self.ready_queue:
      print "jobId, partitions_left, partitions_per_container"
      print job.jobID, job.total_partitions, job.partitions_per_container

    while len(self.ready_queue) > 0 and self.containers_at_time[time] > 0:

      # Containers allotted
      sorted_queue = sorted(self.ready_queue,
                            key=lambda x: x.containers_allotted)

      job = sorted_queue[0]

      if job.total_partitions == 0:
        print "No more partitions left to schedule for job %s" % job.jobID
        self.ready_queue.pop(0)
        continue

      job.total_partitions = max(0, job.total_partitions - job.partitions_per_container)
      job.containers_allotted += 1
      print "Allotting 1 container to job %s: new size: %s" % (
        job.jobID, job.containers_allotted)
      self.containers_at_time[time] -= 1

      estimated_time = job.estimated_time_per_partition
      min_next_time = min(time + estimated_time, min_next_time)

      for i in range(time + 1, time + estimated_time):
        self.containers_at_time[i] -= 1
        if self.containers_at_time[i] < 0:
          print 'ERROR: resource', self.containers_at_time[
            i], ' at time', i, 'below 0'

      if job.total_partitions > 0:
        self.ready_queue.append(job)

      self.ready_queue.pop(0)


    print 'after ', time

    for job in self.ready_queue:
      print job.jobID, job.total_partitions

    if min_next_time < sys.maxint:
      self.next_time = min_next_time
      return True

    return False

  def printJCT(self):
    avg = 0
    oct = 0

    for key, value in self.all_jobs.iteritems():
      time = value.end_time - value.start_time
      print key, time
      avg += time
      oct = max(oct, time)

    print 'Average job completion time : ', 1.0*avg/len(self.all_jobs)
    print 'Overall completion time : ', oct

  def printCE(self):
    usage = []
    for i in range(len(self.containers_at_time)):
        usage.append(100.0*(MAX_CONTAINERS-self.containers_at_time[i])/MAX_CONTAINERS)

    plt.plot(usage)
    plt.ylabel('Cluster usage percentage')
    plt.axis([0, 10, 0, 110])
    plt.show()

  def run(self, algo_id=0):

    with open(self.filename, 'r') as f:
      alljobs = f.readlines()

      # <time><tab><list of jobs to be scheduled at time time>
      for i in range(len(alljobs)):
        parts = alljobs[i].strip().split('-')

        time = int(parts[0])

        while self.next_time < time:
          if self.schedule_decision(algo_id, self.next_time) == False:
            break
        print 'Out of potential infinite loop 1'

        jobs_at_time = parts[1].strip().split(';')
        for eachjob in jobs_at_time:
          print eachjob
          if eachjob[0] not in self.all_jobs:
            self.all_jobs[eachjob[0]] = jobInfo(eachjob[0], time)

          self.ready_queue.append(job(eachjob, time))

        if self.next_time == time:
          self.schedule_decision(algo_id, time)

      if self.ready_queue == []:
        print 'Done scheduling. Total run time is'
      else:
        while self.ready_queue != []:
          self.schedule_decision(algo_id, self.next_time)

        print 'Out of potential infinite loop 2'

    f.close()


sch1 = scheduler('test.txt')
sch2 = scheduler('test.txt')
sch3 = scheduler('test.txt')

sch1.run(algo_id=0)
sch1.printJCT()
sch1.printCE()

# print "==================="
# sch2.run(algo_id=1)
# print "==================="
# sch3.run(algo_id=2)
print "==================="
# sch.run(algo_id=1)

