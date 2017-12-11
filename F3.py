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
               startTime,
               containersRequired,
               jobQuota):
    self.jobID = jobID
    self.containers_allotted = 0
    self.containers_required = containersRequired
    self.start_time = startTime
    self.end_time = startTime
    self.job_quota = jobQuota


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
      self.schedule_max_min(time)
    if algo == 2:
      self.schedule_weighted_max_min(time)
    if algo == 3:
      self.schedule_weighted_max_min(time, algo=3)

    # Shortest job first
    #if algo == 0:
    sorted_queue = sorted(self.ready_queue, key=lambda
          x: x.estimated_time_per_partition * x.total_partitions)

    ready_queue_new = []
    index = 0
    min_next_time = sys.maxint
    #print 'before ', time, ' containers available ', self.containers_at_time[time]

    #for job in self.ready_queue:
      #print job.jobID, job.total_partitions

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
    #print 'after ', time, ' containers available ', self.containers_at_time[time]

    #for job in self.ready_queue:
      #print job.jobID, job.total_partitions

    if min_next_time < sys.maxint:
      self.next_time = min_next_time
      return True

    return False

  def normalize_allocation(self):
    sum_all = 0
    for i in self.ready_queue:
      sum_all += self.all_jobs[i.jobID].job_quota

    for j in self.ready_queue:
      j.job_weight = (j.job_container_quota * 1.0/sum_all)

  def normalize_allocation_ours(self):
      sum_all = 0
      for i in self.ready_queue:
          sum_all += i.job_container_quota + 1.0*self.all_jobs[i.jobID].containers_allotted/self.all_jobs[i.jobID].containers_required

      for j in self.ready_queue:
          j.job_weight = ((j.job_container_quota + 1.0*self.all_jobs[j.jobID].containers_allotted/self.all_jobs[i.jobID].containers_required) / sum_all)

  def schedule_weighted_max_min(self,
                                time=0,
                                algo=2):
    # Containers allotted

    if algo == 2:
        self.normalize_allocation()
    if algo == 3:
        self.normalize_allocation_ours()

    min_next_time = sys.maxint
    #print 'before ', time, ' containers available ', self.containers_at_time[time]

    #for job in self.ready_queue:
      #print "jobId, partitions_left, partitions_per_container"
      #print job.jobID, job.total_partitions, job.partitions_per_container

    queue_length = len(self.ready_queue)
    curr_containers = self.containers_at_time[time]

    while queue_length > 0 and curr_containers > 0:

      # Containers allotted

      #print ' %r containers available at time %r' %(self.containers_at_time[time], time)
      job = self.ready_queue.pop(0)

      allocation = min(math.ceil(job.total_partitions/job.partitions_per_container), math.floor(job.job_weight * self.containers_at_time[time]))
      job.containers_allotted += allocation
      curr_containers -= allocation

      self.all_jobs[job.jobID].containers_allotted += allocation
      job.total_partitions -= allocation * job.partitions_per_container

      #print "Allotting %r containers to job %s: new size: %s" % (
       # allocation, job.jobID, job.containers_allotted)

      estimated_time = job.estimated_time_per_partition
      self.all_jobs[job.jobID].end_time = time + estimated_time
      min_next_time = min(time + estimated_time, min_next_time)

      for i in range(time + 1, time + estimated_time):
        self.containers_at_time[i] -= allocation
        if self.containers_at_time[i] < 0:
          print 'ERROR: resource', self.containers_at_time[i], ' at time', i, 'below 0'

      if job.total_partitions > 0:
        self.ready_queue.append(job)

      queue_length -= 1
    self.containers_at_time[time] = curr_containers

    #print 'after ', time, ' containers available ', self.containers_at_time[time]

    #for job in self.ready_queue:
      #print job.jobID, job.total_partitions

    if min_next_time < sys.maxint:
      self.next_time = min_next_time
      return True

    return False

  def schedule_max_min(self, time=0):

    # Containers allotted

    min_next_time = sys.maxint
    #print 'scheduler begin time', time, ' containers available ', self.containers_at_time[time]

    #for job in self.ready_queue:
      #print "jobId, partitions_left, partitions_per_container"
      #print job.jobID, job.total_partitions, job.partitions_per_container

    while len(self.ready_queue) > 0 and self.containers_at_time[time] > 0:

      # Containers allotted
      sorted_queue = sorted(self.ready_queue,
                            key=lambda x: self.all_jobs[x.jobID].containers_allotted)

      job = sorted_queue[0]

      if job.total_partitions == 0:
        print "No more partitions left to schedule for job %s" % job.jobID
        self.ready_queue.pop(0)
        continue

      job.total_partitions = max(0, job.total_partitions - job.partitions_per_container)
      job.containers_allotted += 1

      jobId = job.jobID

      self.all_jobs[jobId].containers_allotted += 1
      self.all_jobs[jobId].end_time = time + job.estimated_time_per_partition

      #print "Allotting 1 container to job %s: new size: %s" % (
      #  job.jobID, job.containers_allotted)
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

    #print 'after ', time, ' containers available ', self.containers_at_time[time]

    #for job in self.ready_queue:
     # print job.jobID, job.total_partitions

    if min_next_time < sys.maxint:
      self.next_time = min_next_time
      return True

    return False

  def printJCT(self):
    avg = 0
    oct = 0

    for key, value in self.all_jobs.iteritems():
      time = value.end_time - value.start_time
      print ' Job ',key, ' completed in ',time ,' seconds.'
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
    plt.axis([0, 100, 0, 110])
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
        #print 'Out of potential infinite loop 1'

        jobs_at_time = parts[1].strip().split(';')
        for eachjob in jobs_at_time:
          eachjob_parts = eachjob.split(',')
          if eachjob_parts[0] not in self.all_jobs:
            self.all_jobs[eachjob_parts[0]] = jobInfo(eachjob_parts[0], time, math.ceil(float(eachjob_parts[1])*float(eachjob_parts[2])), float(eachjob_parts[3]))

          else:
            self.all_jobs[eachjob_parts[0]].containers_required += math.ceil(float(eachjob_parts[1])*float(eachjob_parts[2]))

          self.ready_queue.append(job(eachjob, time))

        if self.next_time == time:
          self.schedule_decision(algo_id, time)

      if self.ready_queue == []:
        print 'Done scheduling. Total run time is'
      else:
        while self.ready_queue != []:
          self.schedule_decision(algo_id, self.next_time)

        #print 'Out of potential infinite loop 2'

    f.close()

sch1 = scheduler('test.txt')
sch2 = scheduler('test.txt')
sch3 = scheduler('test.txt')
sch4 = scheduler('test.txt')

sch1.run(algo_id=0)
sch1.printJCT()
sch1.printCE()
print "==================="
sch2.run(algo_id=1)
sch2.printJCT()
sch2.printCE()
print "==================="
sch3.run(algo_id=2)
sch3.printJCT()
sch3.printCE()
print "==================="
sch4.run(algo_id=3)
sch4.printJCT()
sch4.printCE()
print "==================="
