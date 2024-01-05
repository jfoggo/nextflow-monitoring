#!/usr/bin/env python3

"""
    Author      : J.R.Fechner
    Description : This script analyzes resource statistics for tasks (and generates monitoring plots)
"""

# required modules
import sys
from jinja2 import Template
import extract_data_from_db
import timeseries_clustering

TEMPLATE_PATH = "./monitoring_report_template.html"
REPORT_NAME = "monitoring_report.html"

################################################ MAIN STARTS HERE #######################################################
def run(wf_name,bucket_path):
    extract_data_from_db.run(bucket_path)
    task_plots = timeseries_clustering.run(wf_name,bucket_path)
    tasks = []
    for task_name in task_plots:
        task = {"name":task_name.replace(".csv","")}
        for resource in task_plots[task_name]:
            task[resource+"_plot"] = "../../"+task_plots[task_name][resource]
        tasks.append(task)
    print(tasks)
    tasks.sort(key=lambda d:int(d["name"].split("_",2)[1]))
    with open(TEMPLATE_PATH,"r") as f: html_template = f.read()
    tm = Template(html_template)
    html_content = tm.render(tasks=tasks,wf_name=wf_name)
    output_path = "{path}/{fname}".format(path=bucket_path,fname=REPORT_NAME)
    with open(output_path,"w") as f: f.write(html_content)
    print("[*] HTML Report generated: "+output_path)

################################################ MAIN STARTS HERE #######################################################

if __name__ == "__main__":
    if len(sys.argv) == 3:
        wf_name, bucket_name = sys.argv[1:3]
        run(wf_name,bucket_name)
        exit(0)
    else:
        print("[*] USAGE: create_monitoring_report.py <wf-name> <bucket-path> <report-path>")
        exit(5) # Test this
