import sys
import random
import math
from datetime import datetime


# Usage: python load_generator.py output_file_path num_jobs max_execution_time_per_partition max_partitions_per_vertex max_ready_time max_vertices_per_job max_dependencies_per_vertex
# Sample Usage: python load_generator_dependencies.py foobar.txt 5 5 5 20 5 3

def main(args):
  random.seed(datetime.now())
  filename = args[1]
  num_jobs = int(args[2])
  max_execution_time_per_partition = int(args[3])
  max_partitions_per_vertex = int(args[4])
  max_ready_time = int(args[5]) - 1
  max_vertices_per_job = int(args[6])
  max_dependencies_per_vertex = int(args[7])

  file = open(filename, 'w')

  vertices_per_job = {}
  start_time_per_job = {}

  quota_per_job = {}

  curr_time = 0

  for i in range(0, num_jobs):
    quota_per_job[i] = 1.0 * random.randint(1, 10) / 10.0
    vertices_per_job[i] = random.randint(1, max_vertices_per_job)
    start_time_per_job[i] = random.randint(0, max_ready_time)

    print "current time: ", curr_time
    line = generate_dag(i,
                        curr_time,
                        max_partitions_per_vertex,
                        max_execution_time_per_partition,
                        quota_per_job[i],
                        vertices_per_job[i],
                        max_dependencies_per_vertex)
    file.write(line)

    delta = random.randint(1, math.floor(max_ready_time / num_jobs))
    curr_time = curr_time + delta
    file.write("\n")

  file.close()


def generate_vertex(job_id, vertex_id, max_partitions_per_vertex,
    max_execution_time_per_partition, quota, max_dependencies_per_vertex):
  partitions = str(random.randint(1, max_partitions_per_vertex))
  container_per_partition = str(1.0 * random.randint(1, 10) / 10.0)
  quota = str(quota)
  execution_time = str(random.randint(1, max_execution_time_per_partition))
  num_dependencies = min(vertex_id,
                         random.randint(1, max_dependencies_per_vertex))
  parent_vertices = []

  while len(parent_vertices) < num_dependencies:
    parent_vertex_id = random.randint(0, vertex_id - 1)
    if parent_vertex_id in parent_vertices:
      continue
    else:
      parent_vertices.append(parent_vertex_id)

  if num_dependencies is 0:
    parent_vertices_str = "None"
  else:
    parent_vertices_str = ':'.join(
        map(lambda x: str(x), sorted(parent_vertices)))

  return ','.join(
      [chr(job_id + ord('A')), partitions, container_per_partition, quota,
       execution_time, str(vertex_id), parent_vertices_str])


def generate_dag(job_id, time, max_partitions_per_vertex,
    max_execution_time_per_partition, quota, vertices_in_job,
    max_dependencies_per_vertex):
  time_str = str(time) + '-'

  counter = 0

  while counter < vertices_in_job:
    print "generating vertex id %s for job %s" % (job_id, counter)
    time_str += generate_vertex(job_id, counter, max_partitions_per_vertex,
                                max_execution_time_per_partition, quota,
                                max_dependencies_per_vertex)
    counter += 1
    if counter != vertices_in_job:
      time_str += ";"

  return time_str


if __name__ == '__main__':
  main(sys.argv)
