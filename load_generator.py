import sys
import random


# Usage: python load_generator.py output_file_path max_jobs max_ready_jobs_at_t max_execution_time_per_partition max_partitions_per_vertex max_ready_time max_time_delta_between_lines
# max_ready_time refers to the time entry of the last line in the load file
# max_jobs refers to total number of jobs in the load
# max_time_delta_between_lines controls how close/separated successive lines are
# Sample Usage: python load_generator.py foo.txt 10 5 5 5 100 5

def main(args):
  filename = args[1]
  max_jobs = int(args[2])
  max_ready_jobs_at_t = int(args[3])
  max_execution_time_per_partition = int(args[4])
  max_partitions_per_vertex = int(args[5])
  max_ready_time = int(args[6])
  max_time_delta_between_lines = int(args[7])

  file = open(filename, 'w')

  job_count = random.randint(max_jobs / 2, max_jobs)

  quota_per_job = {}

  # Assign job quotas
  for i in range(0, job_count):
    quota_per_job[i] = 1.0 * random.randint(1, 10) / 10.0

  curr_time = 1
  #

  while curr_time <= max_ready_time - max_time_delta_between_lines:
    print "current time: ", curr_time
    line = generate_line(curr_time, max_ready_jobs_at_t,
                         max_partitions_per_vertex,
                         max_execution_time_per_partition, job_count,
                         quota_per_job)
    file.write(line)

    curr_time = curr_time + random.randint(1, max_time_delta_between_lines)
    file.write("\n")

  file.close()


def generate_job(used_job_ids, max_partitions_per_vertex,
    max_execution_time_per_partition, job_count, quota_per_job):
  print "generate job: ", used_job_ids, job_count

  while True:

    job_id = random.randint(0, job_count - 1)
    if job_id in used_job_ids:
      # print "cannot find job id"
      # print used_job_ids
      # print job_id
      # print job_count
      continue
    else:
      used_job_ids.append(job_id)
      break

  partitions = str(random.randint(1, max_partitions_per_vertex))
  container_per_partition = str(1.0 * random.randint(1, 10) / 10.0)
  quota = str(quota_per_job[job_id])
  execution_time = str(random.randint(1, max_execution_time_per_partition))

  return ','.join(
      [chr(job_id + ord('A')), partitions, container_per_partition, quota,
       execution_time])


def generate_line(time, max_ready_jobs, max_partitions_per_vertex,
    max_execution_time_per_partition, job_count, quota_per_job):
  time_str = str(time) + '-'
  jobs = random.randint(1, max_ready_jobs)

  used_job_ids = []

  while jobs > 0:
    print "generating jobs at time", time, jobs
    time_str += generate_job(used_job_ids, max_partitions_per_vertex,
                             max_execution_time_per_partition, job_count,
                             quota_per_job)
    jobs -= 1
    if jobs > 0:
      time_str += ";"

  return time_str


if __name__ == '__main__':
  main(sys.argv)
