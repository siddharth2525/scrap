from sqlalchemy import create_engine, text
from google.cloud import secretmanager_v1
import google_crc32c
import json

def get_mysql_input_query(databse_name:str, table_name:str):
    """Generates a MySQL SELECT query for table '{table_name}' in database '{databse_name}' casting selected columns to CHAR for safe ingestion. Credentials are securely fetched from Secret Manager and used to connect via SQLAlchemy. Returns a query string or error dict."""

    client = secretmanager_v1.SecretManagerServiceClient()
    name = "projects/648723306784/secrets/dataflow_db_credentials/versions/1"
    try:
        response = client.access_secret_version(request={"name": name})
        crc32c = google_crc32c.Checksum()
        crc32c.update(response.payload.data)
        if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
            raise Exception("Data corruption detected.")
    
    except Exception as e:
        return {"status":"error", "error":str(e)}
    
    payload = response.payload.data.decode("UTF-8")
    credentials = json.loads(payload)
    db_creds = credentials['mysql']
    username = db_creds['username']
    password = db_creds['password']
    ip = db_creds['ip']
    port = db_creds['port']

    try:
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{ip}:{port}/{databse_name}")

        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = '{table_name}'"))

    except Exception as e:
        return {"status":"error", "error":str(e)}

    rows = result.mappings().all()
    col_dtype = {}
    for row in rows:
        col_dtype[row["COLUMN_NAME"]] = row["DATA_TYPE"]

    dtypes_not_to_cast = ["char","enum","longtext","mediumtext","set","text","tinytext","varchar","json"]

    query = []
    for col, dtype in col_dtype.items():
        if dtype not in dtypes_not_to_cast:
            query.append(f"CAST({col} AS CHAR) AS {col}")
        else:
            query.append(col)

    query = ", ".join(query)
    query = "SELECT " + query + f" FROM {table_name};"

    return {"status":"success", "query":query}