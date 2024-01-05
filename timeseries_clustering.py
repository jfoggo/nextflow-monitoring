#!/usr/bin/env python3

"""
    Author      : J.R.Fechner
    Description : This script analyzes resource statistics for tasks (and generates monitoring plots)
"""

# required modules
import os
import re
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

################################################ CLUSTERING ALGORITHM #######################################################
WINDOW_SIZE = 5
EPS = 0.005
EPS_STEP = 0.001
CLUSTER_NUM = 50

def algo(ts,all_slices,eps,cluster_num,eps_step=0.1,debug=False):
    cur_iter = 0
    if len(all_slices) == 1: return all_slices                              # If only 1 slice provide => done
    while True:                                                             # Loop forever ... (until slices cannot be joined anymore)
        n = len(all_slices)
        cur_iter += 1
        if debug: print("==> Iteration {i}: total_slices={l}".format(i=cur_iter,l=len(all_slices)))
        new_slices = []                                                     # Save all processed slices in here ...
        for i in range(n-1):                                                # Loop all slices (i,i+1),...,(n-1,n)
            slice1,slice2 = all_slices[i:i+2]
            mean1,mean2 = slice1.mean(),slice2.mean()                       # calculate mean of slices
            if debug: print("S{i1} != S{i2}\t|\t{r:.2f} > {m2:.2f} : ".format(i1=i,i2=i+1,m1=mean1,m2=mean2,e=eps,r=mean1*eps + mean1,),end="")
            eps_offset = mean1 * eps
            if (mean1 + eps_offset < mean2) or (mean1 - eps_offset > mean2):    # check "not similiar"
                if debug: print("YES (separate slices)")
                new_slices.append(slice1)
                if i == n-2: new_slices.append(slice2)
            else:                                                               # otherwise "similiar"
                if debug: print("NO (merge slices)")
                merged_slice = np.concatenate((slice1,slice2))
                new_slices.append(merged_slice)
                new_slices += all_slices[i+2:]
                break                                                           # break out of for loop
        if len(all_slices) == len(new_slices):
            if debug: print("--> No change since last iteration... stopping now!")
            break
        else:
            #print("all == new\t|\t{a} == {n}".format(a=len(all_slices),n=len(new_slices)))
            all_slices = new_slices

    if len(all_slices) > cluster_num:                                           # check "cluster num" already reached
        if debug: print("len(all_slices) = {len} > {k} = cluster_num".format(
            len=len(all_slices),
            k=cluster_num
        ))
        new_eps = eps + eps_step                                                # increase epsilon
        return algo(ts,all_slices,new_eps,cluster_num,eps_step,debug)           # repeat process with larger epsilon
    else:
        return all_slices                                                       # otherwise done

def create_slices(ts,window_size):                 # Sliding-Window (with step=window_size)
    i = 0
    c = 0
    while i < ts.shape[0]:
        window = ts[i:i+window_size]
        c += 1
        i += window_size
        yield window

################################################ ERROR MEASUREMENTS #######################################################
def RMSE(p,a): # Root Mean Squared Error
    return ((a - p)**2).mean() ** 0.5

def GMRAE(p,a): # Geometric Mean Relative Absolute Error
    res = 0
    n = len(p)
    a_mean = a.mean()
    for j in range(n):
        ej = a[j] - p[j]
        if np.abs(a[j] - a_mean) != 0 and np.abs(ej) != 0:
            res += np.log(np.abs(ej) / np.abs(a[j] - a_mean))
    return np.exp(res / n)

################################################ HELPER FUNCTIONS #######################################################

def run(dataset_name,bucket_path):
    task_instances,task_names = load_dataset(bucket_path)
    print("[*] Loading dataset '{name}' => anz={anz}".format(name=dataset_name,anz=len(task_instances)))
    dataset_obj = {
        "path": bucket_path,
        "task_names": task_names,
        "dataset": task_instances
    }
    return process_dataset(dataset_name,dataset_obj,False)

def load_dataset(bucket_path):
    metrics_path = "{path}/metrics".format(path=bucket_path)
    task_instances,task_names = [],[]
    for fname in os.listdir(metrics_path):
        if not (fname.startswith("task_") and fname.endswith(".csv")): continue
        file_path = "{path}/{file}".format(path=metrics_path,file=fname)
        df = pd.read_csv(file_path,sep=' ')
        if len(df.timestamp) < 5: continue                                      # SKIP tasks with less than 5 metrics (short living tasks)
        expected_headers = ["time","timestamp","io_read_bytes","io_write_bytes","cpu_usage_in_pct","memory_usage_in_mb"]
        if sum([attr not in df for attr in expected_headers]) > 0: continue     # SKIP tasks where some metrics are missing (BUG: short living tasks may have no io_* attr)
        df["time"] = pd.to_datetime(df.time)
        df["seconds"] = df["timestamp"] - df["timestamp"].min()
        df["io_bytes"] = df["io_read_bytes"] + df["io_write_bytes"]             # Combine read+write bytes into single stats (total io = read + write io)
        shifted = df["io_bytes"].shift()                                        # Convert counter into "values over time"
        shifted[0] = 0
        try:
            df["io_bytes"] = df["io_bytes"] - shifted
        except Exception as e:
            print("[*] Could not parse IO-bytes column ...",e)
            continue
        task_instances.append(df)
        task_names.append(fname)
    return task_instances,task_names

def process_dataset(dataset_name,dataset_obj,show_plots=True):
    dataset = dataset_obj["dataset"]
    task_names = dataset_obj["task_names"]
    dataset_path = dataset_obj["path"]

    plot_path = "{path}/plots".format(path=dataset_path)
    if not os.path.isdir(plot_path):
        os.mkdir(plot_path)

    c = 0
    LABELS = {
        "y": {"cpu":"cpu_usage_in_pct","mem":"memory_usage_in_mb","io":"io_bytes"},
        "ylabel": {"cpu":"CPU Usage (in %)","mem":"Memory Usage (in MB)","io":"IO-Usage (in B)"},
    }
    generated_plots = {}
    for resource in ["cpu","mem","io"]:
        for task_instance,task_file_name in zip(dataset,task_names):
            if task_file_name not in generated_plots: generated_plots[task_file_name] = {}
            task_id,task_name,task_input_name = None,None,None
            match1 = re.match(r"task_(\d+)_(.+)__(.+)_\.csv",task_file_name)
            match2 = re.match(r"task_(\d+)_(.+)\.csv",task_file_name)
            if match1: task_id,task_name,task_input_name = match1.groups()
            elif match2:
                task_id,task_name = match2.groups()
                task_input_name = "#NO-INPUT#"
            if None not in [task_id,task_name,task_input_name]:
                fig,axes = plt.subplots(ncols=3,sharex=True,sharey=True,figsize=(30,10))
                fig.suptitle("Task {id}: {name} ({inp})".format(id=task_id,name=task_name,inp=task_input_name))
                task_instance.plot.line(
                    ax=axes[0],                             title="Input Time-Series",
                    y=LABELS["y"][resource],                x="seconds",
                    ylabel=LABELS["ylabel"][resource],      xlabel="Time (in sec)"
                )
                time_series = task_instance[LABELS["y"][resource]].to_numpy()
                initial_slices = list(create_slices(time_series,window_size=WINDOW_SIZE))
                try:
                    clusters = algo(time_series,initial_slices,EPS,CLUSTER_NUM,EPS_STEP)
                except Exception as e:
                    print("[*] An error occured while executing the algorithm:",e)
                    continue
                cluster_data = pd.DataFrame({"seconds":task_instance.seconds.to_numpy()})
                cdf = pd.DataFrame({"seconds":task_instance.seconds.to_numpy()})
                full_len = len(task_instance[LABELS["y"][resource]])
                mean = np.zeros(full_len)
                idx = 0
                for i,center in enumerate(clusters):
                    arr = np.full(full_len,np.nan)
                    w = len(center)
                    if i+1 < len(clusters): arr[idx:idx+w+1] = np.append(center,clusters[i+1][:1])
                    else: arr[idx:idx+w] = center
                    mean[idx:idx+w] = center.mean()
                    cdf["cluster-"+str(i)] = arr
                    idx += w
                cluster_data["cluster_means (k={k})".format(k=len(clusters))] = mean
                cluster_data["timeseries_mean"] = np.full(full_len,task_instance[LABELS["y"][resource]].mean())
                cdf.plot.line(
                    ax=axes[1],                        title="Clustered Input Time-Series",
                    x="seconds",
                    ylabel=LABELS["ylabel"][resource], xlabel="Time (in sec)"
                ).get_legend().remove()
                cluster_data.plot.line(
                    ax=axes[2],                         title="Clustering Result VS Global Mean",
                    x="seconds",
                    ylabel=LABELS["ylabel"][resource],  xlabel="Time (in sec)"
                )

                for ax in axes:
                    ax.set_xlim(left=0)
                    ax.set_ylim(bottom=0)

                image_path = "{path}/{name}_{metric}_k{cluster_num}.png".format(path=plot_path,name=task_file_name.replace(".csv",""),metric=resource,cluster_num=len(clusters))
                plt.savefig(image_path)
                generated_plots[task_file_name][resource] = image_path
                s1 = mean
                s2 = np.full(full_len,task_instance[LABELS["y"][resource]].mean())

                gmrae_c = GMRAE(s1,time_series)
                gmrae_m = GMRAE(s2,time_series)
                gmrae_diff = (gmrae_m - gmrae_c) / gmrae_m
                mrse_c = RMSE(s1,time_series)
                mrse_m = RMSE(s2,time_series)
                mrse_diff = (mrse_m - mrse_c) / mrse_m

                print("Task-ID:",task_id)
                print("GMRAE) \tC={cluster:.4f}\tM={mean:.4f}\t=> {diff:.4f}".format(cluster=gmrae_c,mean=gmrae_m,diff=gmrae_diff))
                print("RMSE) \tC={cluster:.4f}\tM={mean:.4f}\t=> {diff:.4f}".format(cluster=mrse_c,mean=mrse_m,diff=mrse_diff))
                if show_plots:
                    plt.show()
                plt.close()
            else:
                print("[ERROR] Unexpected filename format: "+task_file_name)
                print("[ERROR] Cannot process filename properly! Skipping file ...")
    return generated_plots

################################################ MAIN STARTS HERE #######################################################

if __name__ == "__main__":
    if len(sys.argv) == 3:
        workflow_name, bucket_name = sys.argv[1:3]
        run(workflow_name,bucket_name)
        exit(0)
    else:
        print("[*] USAGE: timeseries_clustering.py <workflow-name> <bucket-path>")
        exit(1)