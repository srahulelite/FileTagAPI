# gcs_test.py
import os, sys
from google.cloud import storage

bucket_name = os.getenv("GCS_BUCKET")
print(bucket_name)
if not bucket_name:
    print("GCS_BUCKET not set")
    sys.exit(1)

client = storage.Client()
bucket = client.bucket(bucket_name)
print("Bucket exists?", bucket.exists())
blob = bucket.blob("filetagapi_debug/health_check.txt")
try:
    blob.upload_from_string("filetagapi debug upload\n")
    print("Upload OK: gs://%s/%s" % (bucket_name, blob.name))
except Exception as e:
    print("Upload failed:", e)
