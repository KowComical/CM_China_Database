#!/bin/bash

# Define variables for the command options and directory paths
python_cmd="/home/dxinyu/anaconda3/envs/china_env/bin/python"
script_file="/data3/kow/CM_China_Database/code/global_code/all_process.py"
log_dir="/data3/kow/CM_China_Database/log/all_process"
email_log_dir="/data3/kow/CM_China_Database/log/sending_email"
year_dir="$(date '+%Y')"
month_dir="$(date '+%m')"
log_file="nohup-$(date '+%Y-%m-%d').out"
email_script="/data3/kow/CM_China_Database/code/global_code/sending_email.py"

# Create the year-based and month-based directories if they don't exist
mkdir -p "$log_dir/$year_dir/$month_dir"
mkdir -p "$email_log_dir/$year_dir/$month_dir"

if pgrep -f "$script_file" > /dev/null; then
    echo "The script is already running. Killing all related processes."
    pkill -f "$script_file"
    pkill -f "$email_script"
fi

# Record the start time
start_time=$(date +%s)

# Run your command with nohup and redirect output to the file
nohup "$python_cmd" "$script_file" > "$log_dir/$year_dir/$month_dir/$log_file" 2>&1 &

# Get the process ID of the Python script
python_pid=$!

# Run a background process that periodically checks if the Python script is still running and logs the running time when it finishes
(
  while kill -0 $python_pid 2>/dev/null; do
    sleep 1
  done
  end_time=$(date +%s)
  total_time=$((end_time - start_time))
  total_time_hours=$(echo "scale=2; $total_time / 3600" | bc)
  echo "Total running time: $total_time_hours hours" >> "$log_dir/$year_dir/$month_dir/$log_file"
) &

# Wait for the Python script to finish
wait $python_pid

# Wait for the background process to finish and log the running time
sleep 10

# Define the email log file path
email_log_file="$email_log_dir/$year_dir/$month_dir/sending_email-$(date '+%Y-%m-%d').out"

# Call the sending_email.py script after the log file is created, passing the log file path as an argument
nohup "$python_cmd" "$email_script" "$log_dir/$year_dir/$month_dir/$log_file" "CM_China_database" > "$email_log_file" 2>&1 &

# Get the process ID of the email script
email_pid=$!

# Wait for the email script to finish
wait $email_pid
