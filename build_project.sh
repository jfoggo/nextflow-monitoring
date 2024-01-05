#!/bin/bash

full_path=$(pwd)
cur_dir=${full_path##*/}

# Check current folder (must be nextflow-monitoring or build folder)
if [ "$cur_dir" != "nextflow-monitoring" ] && [ "$cur_dir" != "build" ] ; then
	echo "[*] ERROR: Please use this script from 'nextflow-monitoring' or 'build' folder only!"
	exit 1
fi


# Create build folder (if not exist)
if [ ! -d ../build ] ; then
  mkdir ../build
fi

# Copy source code into build folder
echo "[1/5] Copy latest source code to 'build' folder ..."
cp -rf ../nextflow-monitoring/* ../build/

# Ensure all required programs are available
echo "[2/5] Ensure all required programs are installed ..."
required_programs=("jq" "curl" "python3" "influx")
missing_prog=0
for prog in ${required_program[@]}
do
  which "$prog" &> /dev/null
  if [ $? -ne 0 ] ; then
    echo "[*] ERROR: Could not find following program: $prog"
    missing_prog=1
  fi
done
if [ $missing_prog -eq 1 ] ; then
  echo "[*] HINT: Please install required programs and restart this script!"
  exit 2
fi

# Install required python modules
pip install -r ../build/nextflow_requirements.txt

echo "[3/5] Ensure valid authentication variables (token + org) ..."
grep "<INFLUXDB-TOKEN>" ../build/modules/nextflow/src/main/groovy/nextflow/trace/InfluxConnector.groovy
rc1=$?
grep "<INFLUXDB-ORG>" ../build/modules/nextflow/src/main/groovy/nextflow/trace/InfluxConnector.groovy
rc2=$?
if [ $rc1 -eq 0 ] || [ $rc2 -eq 0 ] ; then
  echo "[ERROR] Missing authentication variables (token + org) in following files: InfluxConnector.groovy and extract_data_from_db.py"
  echo "[ERROR] Please replace these values with actual credentials!"
  echo "[ERROR] Aborting program!"
  exit 3
fi


# Copy helper scripts to /usr/bin
echo "[4/5] Copy helper-scripts to /usr/bin ..."
helper_scripts=(docker-metrics.sh create_monitoring_report.py timeseries_clustering.py extract_data_from_db.py)
for script in ${helper_scripts[@]}
do
  sudo cp $script /usr/bin/$script
done

# Compile source code
echo "[5/5] Compile source code into executable binary ..."
cd ../build
make compile

# Done
echo "[*] Program done! Please use 'launch.sh'"
echo "[*] HINT: You may have to switch to 'build' dir !!!"
exit 0
