from google.cloud import logging_v2
import google.generativeai as genai
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
import re

load_dotenv()

def summarize_dataflow_error_logs_tool(time_window_minutes: int = 60, job_id: str = None):
    """
    Fetches recent Dataflow error logs.
    If no job_id is provided:
        → Lists Dataflow jobs that have errors and waits for user selection.
    If job_id is provided:
        → Summarizes logs for that specific job.
    """

    try:
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        if not project_id:
            return {
                "status": "error",
                "error": "Missing GOOGLE_CLOUD_PROJECT in environment variables."
            }

        genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
        client = logging_v2.Client(project=project_id)

        since_time = (datetime.utcnow() - timedelta(minutes=time_window_minutes)).isoformat("T") + "Z"
        base_filter = f'resource.type="dataflow_step" severity=ERROR timestamp>="{since_time}"'

        entries = list(client.list_entries(filter_=base_filter))
        if not entries:
            return {
                "status": "success",
                "summary": f"No Dataflow errors found in the last {time_window_minutes} minutes."
            }

        # Identify unique job IDs from logs
        job_pattern = re.compile(r'job[-_]id["\']?\s*[:=]\s*["\']?([\w-]+)', re.IGNORECASE)
        jobs_found = {}

        for entry in entries:
            text = str(entry.payload)
            match = job_pattern.search(text)
            if match:
                jid = match.group(1)
            else:
                # fallback from resource labels
                jid = getattr(entry.resource, "labels", {}).get("job_id")

            if jid:
                jobs_found.setdefault(jid, []).append(text)

        # --- CASE 1: No job_id provided → list all jobs ---
        if not job_id:
            if not jobs_found:
                return {
                    "status": "success",
                    "summary": f"No identifiable job IDs found in error logs within {time_window_minutes} minutes."
                }

            job_list = [
                {"job_id": jid, "error_count": len(logs)} for jid, logs in jobs_found.items()
            ]
            job_list_sorted = sorted(job_list, key=lambda x: x["error_count"], reverse=True)

            return {
                "status": "pending_user_input",
                "message": (
                    f"Found {len(job_list_sorted)} Dataflow jobs with errors in the last "
                    f"{time_window_minutes} minutes. Please select one job ID to summarize."
                ),
                "jobs": job_list_sorted,
                "follow_up_instructions": (
                    "Reply with: summarize_dataflow_error_logs_tool(job_id='<job_id_here>')"
                ),
            }

        # --- CASE 2: Specific job_id provided → summarize ---
        if job_id not in jobs_found:
            return {"status": "error", "error": f"No logs found for job ID: {job_id}"}

        combined_logs = "\n\n".join(jobs_found[job_id])[:15000]

        prompt = f"""
        You are a Google Cloud Dataflow expert.
        Analyze the following error logs for Dataflow job ID `{job_id}` and summarize:
        1. The **root cause** of failure.
        2. The **transform or step** where it occurred.
        3. Possible **resolutions or configuration fixes**.
        4. Any **recurring patterns or warnings**.

        Keep your explanation clear, short, and actionable.

        Logs:
        {combined_logs}
        """

        model = genai.GenerativeModel("gemini-2.5-flash")
        response = model.generate_content(prompt)

        return {
            "status": "success",
            "job_id": job_id,
            "summary": response.text.strip() if response and response.text else "No summary generated.",
            "metadata": {
                "project_id": project_id,
                "log_count": len(jobs_found[job_id]),
                "time_window_minutes": time_window_minutes,
            },
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}
