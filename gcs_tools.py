from google.cloud import storage
from dotenv import load_dotenv
import os

load_dotenv()

def get_gcs_client():
    return storage.Client(project=os.getenv("GOOGLE_CLOUD_PROJECT"))

def read_gcs_file_tool(bucket_name:str, file_path:str):
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        
        if not blob.exists():
            return {"status":"error",
                    "error":f"File {file_path} not found"}
        
        content = blob.download_as_text()
        lines = content.splitlines()

        return {
            "status": "success",
            "bucket_name": bucket_name,
            "file_path": file_path,
            "content": "\n".join(lines),
            "metadata": {
                "size": blob.size,
                "content_type": blob.content_type,
                "created": (
                    blob.time_created.isoformat() if blob.time_created else None
                ),
                "updated": blob.updated.isoformat() if blob.updated else None,
            },
        }
    
    except Exception as e:
        return {"status": "error", "error": str(e)}