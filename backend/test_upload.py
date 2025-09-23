# test_upload.py
from google.cloud import storage
from datetime import timedelta

BUCKET = "filetagapi-uploads-filetagapi-prod"
OBJ = "smoke-test/test.txt"

client = storage.Client()
bucket = client.bucket(BUCKET)
blob = bucket.blob(OBJ)
blob.upload_from_string("hello-from-filetagapi\n", content_type="text/plain")
print("Uploaded:", f"gs://{BUCKET}/{OBJ}")

url = blob.generate_signed_url(expiration=timedelta(minutes=15), version="v4")
print("Signed URL (15m):", url)
