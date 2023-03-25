import requests
import configparser
import base64
import time

# Get the full path to the config.ini file
config = configparser.ConfigParser()
config.read("/Users/gomes/Desktop/Projects/Data Engineer/3-Project/config/config.ini")

# Set Databricks domain, personal access token, and workspace path
databricks_domain = config.get("databricks", "databricks_domain")
personal_access_token = config.get("databricks", "access_token")
workspace_path = "/Users/guigomes0499@gmail.com/dataprocess"

# Set the path to your local notebook file
local_notebook_path = "/Users/gomes/Desktop/Projects/Data Engineer/3-Project/scripts/dataprocess.ipynb"

# Function to make an authenticated request to the Databricks REST API
def databricks_api_request(endpoint, method, data=None):
    url = f"{databricks_domain}/api/2.0{endpoint}"
    headers = {"Authorization": f"Bearer {personal_access_token}"}
    response = requests.request(method, url, headers=headers, json=data)
    return response

# Function to create a new cluster and get its cluster_id
def create_new_cluster():
    response = databricks_api_request(
        "/clusters/create",
        "POST",
        {
            "cluster_name": "WU3Cluster",
            "spark_version": "11.3.x-scala2.12",
            "node_type_id": "Standard_DS3_v2",
            "driver_node_type_id": "Standard_DS3_v2",
            "num_workers": 0,
        },
    )

    if response.status_code == 200:
        return response.json()["cluster_id"]
    else:
        print(f"Error creating cluster: {response.status_code}: {response.text}")
        return None

# Function to get the cluster status
def get_cluster_status(cluster_id):
    response = databricks_api_request(f"/clusters/get", "GET", {"cluster_id": cluster_id})

    if response.status_code == 200:
        return response.json()["state"]
    else:
        print(f"Error getting cluster status: {response.status_code}: {response.text}")
        return None

# Function to wait for the cluster to be ready
def wait_for_cluster_ready(cluster_id, polling_interval=30, max_retries=20):
    retries = 0
    while retries < max_retries:
        status = get_cluster_status(cluster_id)
        if status == "RUNNING":
            print("Cluster is ready.")
            return True
        elif status in {"TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS"}:
            print(f"Cluster is in a non-recoverable state: {status}")
            return False
        else:
            print(f"Cluster status is {status}. Waiting...")
            time.sleep(polling_interval)
            retries += 1

    print("Max retries reached. Cluster is not ready.")
    return False


# Upload the notebook to the Databricks workspace
with open(local_notebook_path, "r") as notebook_file:
    notebook_content = notebook_file.read()

response = databricks_api_request(
    f"/workspace/import",
    "POST",
    {
        "path": workspace_path,
        "format": "JUPYTER",
        "language": "PYTHON",
        "content": base64.b64encode(notebook_content.encode("utf-8")).decode("utf-8"),
        "overwrite": True,
    },
)

if response.status_code == 200:
    print("Notebook uploaded successfully!")
else:
    print(f"Error uploading notebook: {response.status_code}: {response.text}")

# Create a new cluster and get its cluster_id
cluster_id = create_new_cluster()
if cluster_id is not None:
    if wait_for_cluster_ready(cluster_id):
        # Create a job to execute the notebook
        response = databricks_api_request(
            "/jobs/create",
            "POST",
            {
                "name": "Execute Uploaded Notebook",
                "existing_cluster_id": cluster_id,
                "notebook_task": {"notebook_path": workspace_path},
            },
        )

        if response.status_code == 200:
            job_id = response.json()["job_id"]
            print(f"Job created successfully! Job ID: {job_id}")

            # Run the job
            response = databricks_api_request(f"/jobs/run-now", "POST", {"job_id": job_id})

            if response.status_code == 200:
                run_id = response.json()["run_id"]
                print(f"Job started successfully! Run ID: {run_id}")
            else:
                print(f"Error starting job: {response.status_code}: {response.text}")

        else:
            print(f"Error creating job: {response.status_code}: {response.text}")
    else:
        print("Cluster is not ready. Exiting.")
else:
    print("Failed to create a cluster. Exiting.")


