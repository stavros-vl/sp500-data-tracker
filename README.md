# sp500-data-tracker
End-to-end ELT project for stock market data


## Running and configuring GCP VM 
This project is run on a [GCP Virtual Machine](https://cloud.google.com/products/compute?hl=en). 

Reasons are:
1. For practice with VMs
2. Due to limitations in hardware, especially with the vanilla Airflow which is heavy for a "normal" home laptop.

To connect from the terminal to the VM, after starting it:

1. Open the .ssh file config
```
$ nano .ssh/config
```

2. Update the External IP parameter with the External IP found in the GCP VM page

3. To open the VM's shell run the following command:

```
ssh <name_of_the_VM_instance>
```

4. (optional). Connect to VM instance through VSCode
Open VS Code and click on the bottom left corner. Then click "Connect to Host..." and select the VM instance name

## VM schedules & automations
### Scheduling
In order to reduce costs, the VM is not always up and running. Instead the approach is to schedule the instance to Start and Stop based on when the Airflow dags schedules. More info here: [Sceduling a VM instance to start and stop](https://cloud.google.com/compute/docs/instances/schedule-instance-start-stop)

### Automations
To run the Airflow dags when the instance starts, we need to first initialize the Docker containers and all the Airflow services. 
*See also the workflow_orchestration/aiflow folder's README for more information about the commands.*
For that reason we have 2 scripts running, one on instance start-up and one on shut-down:

More information about start-up scripts: https://cloud.google.com/compute/docs/instances/startup-scripts/linux

1. Start-up script

```
#!/bin/bash

# Define a log file
LOG_FILE=/var/log/startup-script.log
exec > >(tee -a $LOG_FILE) 2>&1

# Change directory to where your docker-compose.yml is located
cd /home/stavros/sp500-data-tracker/workflow_orchestration/airflow || { echo "Directory not found"; exit 1; }

# Build and start Docker Compose services
/home/stavros/bin/docker-compose build || { echo "Docker Compose build failed"; exit 1; }
/home/stavros/bin/docker-compose up airflow-init || { echo "Airflow init failed"; exit 1; }
/home/stavros/bin/docker-compose up -d || { echo "Docker Compose up failed"; exit 1; }

# Wait for a short time to ensure all services are up
sleep 60

# Verify if Airflow Scheduler is running
if ! /home/stavros/bin/docker-compose ps | grep -q 'airflow-scheduler.*Up'; then
  echo "Airflow Scheduler is not running"
  exit 1
fi

# List DAGs to ensure they are loaded
docker exec $(docker ps --filter name=airflow-scheduler -q) airflow dags list || { echo "Failed to list DAGs"; exit 1; }

# Unpause specific DAGs
docker exec $(docker ps --filter name=airflow-scheduler -q) airflow dags unpause sp500_wiki_data_pipeline || { echo "Failed to unpause sp500_wiki_data_pipeline"; exit 1; }
docker exec $(docker ps --filter name=airflow-scheduler -q) airflow dags unpause sp500_yfinance_data_pipeline || { echo "Failed to unpause sp500_yfinance_data_pipeline"; exit 1; }

# Check Docker containers status
echo "Docker containers status:"
docker ps --format "table {{.ID}}\t{{.Names}}\t{{.Status}}"
```

2. Shut-down script

```
#!/bin/bash
cd /home/stavros/sp500-data-tracker/workflow_orchestration/airflow || { echo "Directory not found"; exit 1; }

docker-compose down
```
