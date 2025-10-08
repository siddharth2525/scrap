from google.cloud.dataflow_v1beta3 import FlexTemplatesServiceClient, LaunchFlexTemplateParameter, LaunchFlexTemplateRequest, JobsV1Beta3Client, GetJobRequest, ListJobsRequest
from dotenv import load_dotenv
import os
from datetime import datetime

timestamp = datetime.now().strftime("%H%M%S-%Y%m%d")

load_dotenv()

def get_dataflow_client():
    return FlexTemplatesServiceClient()

def run_dataflow_job_tool(parameters:dict, template_path:str):
    """Take parameters and template path for dataflow job as an input and run the job and output the job status, job id, job name."""
    try:
        launch_params = LaunchFlexTemplateParameter(job_name = f"adk-job-{timestamp}", container_spec_gcs_path = template_path, parameters = parameters)
        request = LaunchFlexTemplateRequest(project_id = os.getenv("GOOGLE_CLOUD_PROJECT"), location = os.getenv("GOOGLE_CLOUD_LOCATION"), launch_parameter = launch_params)

        try:
            client = get_dataflow_client()
            response = client.launch_flex_template(request)
            job_id = response.job.id
            job_name = response.job.name

            return {"status": "success","job_id":job_id, "job_name":job_name}
        
        except Exception as e:
            return {"status": "error", "error": str(e)}

    except Exception as e:
        return {"status":"error", "error":str(e)}
    
def get_dataflow_job_status_tool(job_id:str):
    """Take dataflow job id as an input and return the job status"""
    try:
        client = JobsV1Beta3Client()
        request = GetJobRequest(
                project_id=os.getenv("GOOGLE_CLOUD_PROJECT"),
                location=os.getenv("GOOGLE_CLOUD_LOCATION"),
                job_id=job_id,
                view=1
        )
        response = client.get_job(request)
        job_state =['JOB_STATE_UNKNOWN', 'JOB_STATE_STOPPED', 'JOB_STATE_RUNNING', 'JOB_STATE_DONE', 
                        'JOB_STATE_FAILED', 'JOB_STATE_CANCELLED', 'JOB_STATE_UPDATED', 'JOB_STATE_DRAINING', 
                        'JOB_STATE_DRAINED', 'JOB_STATE_PENDING', 'JOB_STATE_CANCELLING', 'JOB_STATE_QUEUED', 
                        'JOB_STATE_RESOURCE_CLEANING_UP']
        
        return {"status":"success", "job_state":job_state[response.current_state]}

    except Exception as e:
        return {"status":"error", "error":str(e)}
    
def list_dataflow_recent_jobs(num_jobs:int=10):
    """Lists the {num_jobs} most recent Google Cloud Dataflow jobs for the current project and location, using the Dataflow V1Beta3 API. Jobs are sorted by creation time in descending order and returned as a list of dictionaries containing job ID, name, state."""
    
    try:
        client = JobsV1Beta3Client()
        request = ListJobsRequest(project_id=os.getenv("GOOGLE_CLOUD_PROJECT"), location=os.getenv("GOOGLE_CLOUD_LOCATION"))
        response = client.list_jobs(request)

        sorted_jobs = sorted(response.jobs, key=lambda job: job.create_time, reverse=True)

        recent_jobs = sorted_jobs[:num_jobs]

        job_dicts = []
        for job in recent_jobs:
            job_dicts.append({
                "id": job.id,
                "name": job.name,
                "state": job.current_state.name
            })

        return {"status": "success", "jobs": job_dicts}

    except Exception as e:
        return {"status":"error", "error":str(e)}