# HDFS-Stress
## Building

    make

## Running
### Seeing latency

    hadoop jar StressTool.jar StressTool latency

(save to file):

	hadoop jar StressTool.jar StressTool latency 2>/dev/null | tee file.txt

### Stress on One Machine
To stress with 100 writing threads:

    hadoop jar StressTool.jar StressTool stress mkdirs 100
    hadoop jar StressTool.jar StressTool stress chmods 500

### Stress from Datanodes (MapReduce Job)
Requires an input directory containing multiple parts (try teragen to make data), which will distribute the job to multiple machines (hopefully, no guarantees). Output directory is needed but not used.

	hadoop jar StressTool.jar StressToolMR /path/to/input /path/to/output <num_threads>

Example:

	/apache/hadoop/bin/hadoop jar StressTool.jar StressToolMR /tmp/qos-testing/10000_rows_0 /tmp/qos-testing/out/0 100