#!/usr/bin/env python3

"""
    Author      : J.R.Fechner
    Description : This script extracts resource statistics from the influx-db (and stores them as csv-files)
                  => Please replace authentication variables ("org" and "token") with your own values! (*** important ***)
"""

# required modules
from influxdb_client import InfluxDBClient
import os
import sys
import re

################################################ EXTRACT STATS FROM INFLUX-DB #######################################################

def run(bucket_name):
    # config variables
    token = "4QDi6FI3cKHptyQ7bVeMJRgqNQi17auqH1vNO5m7Lu1tuOy0uqPOExC9d2GJqHTI2dXcLW3PoObinY9ZpjzdLw=="
    org = "bachelor"

    # connect to db
    print("[*] Connecting to DB ...")
    client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)
    print("[*] Successfully connected to DB!")

    # execute query
    print("[*] Executing query ...")
    query_api = client.query_api()
    query = """
        from(bucket: "{bucket_name}")
        |> range(start: 0)
    """.format(bucket_name=bucket_name)
    result = query_api.query_csv(query=query)

    metrics = {}
    header_line = None
    expected_field_list = {
        "timestamp": "timestamp",
        "docker_cpu_usage_pct": "cpu_usage_in_pct",
        "docker_available_memory_mb": "memory_limit_in_mb",
        "docker_used_memory_mb": "memory_usage_in_mb",
        "docker_memory_usage_pct": "memory_usage_in_pct",
        "docker_io_service_bytes_read": "io_read_bytes",
        "docker_io_service_bytes_write": "io_write_bytes",
        "max_cpus": "max_cpus",
        "max_mem": "max_mem",
        "max_disk": "max_disk"
    }
    TID_NAME_MAP = {}
    for csv_line in result:
        if not len(csv_line) == 0 and not csv_line[0].startswith("#"):
            if header_line == None:
                header_line = csv_line
            else:
                time, value, field, measurement, name, pid, tid = csv_line[5:]

                if field not in expected_field_list: continue
                else: field = expected_field_list[field]

                if tid not in metrics: metrics[tid] = {}
                if field not in metrics[tid]: metrics[tid][field] = []
                if "time" not in metrics[tid]: metrics[tid]["time"] = []
                metrics[tid][field].append(value)
                metrics[tid]["time"].append(time)
                TID_NAME_MAP[tid] = re.sub("\W","_",name)

    path = bucket_name + "/metrics"
    if not os.path.exists(bucket_name):
        os.makedirs(bucket_name)
        os.makedirs(path)

    for tid in metrics:
        fname = path+"/task_"+str(tid)+"_"+TID_NAME_MAP[tid]+".csv"
        print("[*] Writing file: "+str(fname))
        with open(fname,"w") as f:
            header_line = " ".join(list(metrics[tid].keys()))+"\n"
            f.write(header_line)
            value_arrays = list(metrics[tid].values())
            #print([len(arr) for arr in value_arrays])
            for i in range(len(value_arrays[0])):
                data_line = " ".join([str(arr[i]) if len(arr) > i else "?" for arr in value_arrays])+"\n"
                f.write(data_line)
    print("[*] Done with DB-export!")

################################################ MAIN STARTS HERE #######################################################

if __name__ == "__main__":
    if len(sys.argv) == 2:
        bucket_name = sys.argv[1]
        run(bucket_name)
        exit(0)
    else:
        print("[*] USAGE: extract_data_from_db.py <bucket-path>")
        exit(1)
