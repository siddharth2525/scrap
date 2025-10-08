from google.cloud import logging_v2
import google.generativeai as genai
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os

load_dotenv()

def summarize_dataflow_error_logs_tool(time_window_minutes: int = 60):
    """
    Fetches and summarizes recent Dataflow job error logs from Cloud Logging using the Gemini-2.5-flash model.
    """

    try:
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        if not project_id:
            return {"status": "error", "error": "Missing GOOGLE_CLOUD_PROJECT in environment variables."}

        genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
        client = logging_v2.Client(project=project_id)

        since_time = (datetime.utcnow() - timedelta(minutes=time_window_minutes)).isoformat("T") + "Z"
        filter_str = f'resource.type="dataflow_step" severity=ERROR timestamp>="{since_time}"'

        entries = list(client.list_entries(filter_=filter_str))
        if not entries:
            return {"status": "success", "summary": f"No Dataflow errors found in the last {time_window_minutes} minutes."}

        logs = [str(entry.payload) for entry in entries]
        combined_logs = "\n\n".join(logs)[:15000]  # safe truncation for model limits

        prompt = f"""
        You are a Google Cloud Dataflow expert.
        Analyze the following error logs and provide a clear, concise summary including:
        1. The **primary cause** of failure.
        2. The **Dataflow transform or step** where it occurred.
        3. Possible **resolution steps** or configuration fixes.
        4. Any **recurring patterns or warnings** worth noting.

        Make the explanation short, actionable, and written in plain English.

        Logs (last {time_window_minutes} minutes):
        {combined_logs}
        """

        model = genai.GenerativeModel("gemini-2.5-flash")
        response = model.generate_content(prompt)

        return {
            "status": "success",
            "summary": response.text.strip() if response and response.text else "No summary generated.",
            "metadata": {
                "project_id": project_id,
                "log_count": len(logs),
                "time_window_minutes": time_window_minutes,
            },
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}
