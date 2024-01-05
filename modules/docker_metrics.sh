#!/bin/bash

cid=$1
if [ -z "$cid" ] ; then
	echo "[*] USAGE: docker-metrics.sh <container-id>"
	exit 1
fi

docker_stats=$(sudo curl -s --unix-socket /var/run/docker.sock "http://localhost/v1.41/containers/$cid/stats?stream=false")
rc=$?
if [ $rc -ne 0 ] ; then
	echo "[*] ERROR: Curl command exited with status code = $rc"
	exit 2
fi

#echo $docker_stats
jq -e .memory_stats.usage <<< "$docker_stats" &> /dev/null
rc1=$?
jq -e .cpu_stats.cpu_usage <<< "$docker_stats" &> /dev/null
rc2=$?
#echo $rc1 $rc2
if [ $rc1 -ne 0 ] || [ $rc2 -ne 0 ] ; then
	echo "[*] ERROR: Could not find container metrics (for $cid)"
	exit 3
fi

# helper function
function byte_to_mb(){
	local byte
	local b2mb
	local mb

	byte=$1
	b2mb=1048576
	if [ "$byte" != "null" ] ; then
		mb=$(python3 -c "print('{:.2f}'.format( $byte / $b2mb ))")
	else
		mb=0
	fi
	echo $mb
}

# Current timestamp (unix timestamp format)
echo "timestamp" $(date +%s)

### MEMORY STATISTICS
# used memory
memory_stats_usage=$(echo "$docker_stats" | jq -e '.memory_stats.usage')
memory_stats_stats_cache=$(echo "$docker_stats" | jq -e '.memory_stats.stats.cache')
used_memory=$(($memory_stats_usage - $memory_stats_stats_cache))
used_memory_mb=$(byte_to_mb $used_memory)
echo "docker_used_memory_mb $used_memory_mb"

# available memory
available_memory=$(echo "$docker_stats" | jq '.memory_stats.limit')
available_memory_mb=$(byte_to_mb $available_memory)
echo "docker_available_memory_mb $available_memory_mb"

# memory usage in %
has_mem=$(jq .memory_stats <<< "$docker_stats")
mem_usage_pct=$(python3 -c "print('{:.4f}'.format(($used_memory / $available_memory) * 100.0))")
echo "docker_memory_usage_pct $mem_usage_pct"

### CPU STATISTICS
# cpu delta
cpu_now=$(echo "$docker_stats" | jq '.cpu_stats.cpu_usage.total_usage')
cpu_last=$(echo "$docker_stats" | jq '.precpu_stats.cpu_usage.total_usage')
cpu_delta=$(($cpu_now - $cpu_last))
#echo "cpu delta: $cpu_delta"

# system cpu delta
sys_cpu_now=$(echo "$docker_stats" | jq '.cpu_stats.system_cpu_usage')
sys_cpu_last=$(echo "$docker_stats" | jq '.precpu_stats.system_cpu_usage')
sys_cpu_delta=$(($sys_cpu_now - $sys_cpu_last))
#echo "sys cpu delta: $sys_cpu_delta"

# number cpus
num_cpu=$(echo "$docker_stats" | jq -e '.cpu_stats.online_cpus')
if [ $? -ne 0 ] ; then
	num_cpu=$(echo "$docker_stats" | jq -e '.cpu_stats.cpu_usage.percpu_usage | length')
fi
echo "docker_num_cpus $num_cpu"

# cpu usage in %
cpu_usage_pct=$(python3 -c "print('{:.4f}'.format(($cpu_delta / $sys_cpu_delta) * $num_cpu * 100.0))")
echo "docker_cpu_usage_pct $cpu_usage_pct"

### IO STATISTICS
# io service bytes (read+write)
echo docker_io_service_bytes_read $(echo "$docker_stats" | jq '.blkio_stats.io_service_bytes_recursive[] | select(.op == "Read") | .value' | awk '{sum+=$0} END{print sum}')
echo docker_io_service_bytes_write $(echo "$docker_stats" | jq '.blkio_stats.io_service_bytes_recursive[] | select(.op == "Write") | .value' | awk '{sum+=$0} END{print sum}')
echo docker_io_service_bytes_total $(echo "$docker_stats" | jq '.blkio_stats.io_service_bytes_recursive[] | select(.op == "Total") | .value' | awk '{sum+=$0} END{print sum}')

# io serviced (read+write)
echo docker_io_serviced_read $(echo "$docker_stats" | jq '.blkio_stats.io_serviced_recursive[] | select(.op == "Read") | .value' | awk '{sum+=$0} END{print sum}')
echo docker_io_serviced_write $(echo "$docker_stats" | jq '.blkio_stats.io_serviced_recursive[] | select(.op == "Write") | .value' | awk '{sum+=$0} END{print sum}')
echo docker_io_serviced_total $(echo "$docker_stats" | jq '.blkio_stats.io_serviced_recursive[] | select(.op == "Total") | .value' | awk '{sum+=$0} END{print sum}')

# io time
echo docker_io_time $(echo "$docker_stats" | jq '.blkio_stats.io_time_recursive[0] | .value' | awk '{sum+=$0} END{print sum}')

exit 0
