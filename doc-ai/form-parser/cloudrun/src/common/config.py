"""
  Extraction Service config file
"""
import os
import google

# Reading environment variables
PROJECT_ID = os.environ.get("PROJECT_ID", "")
if PROJECT_ID == "":
	_, PROJECT_ID = google.auth.default()
if PROJECT_ID != "":
	os.environ["GOOGLE_CLOUD_PROJECT"] = PROJECT_ID

PROCESSORS_CONFIG = os.environ.get("PROCESSORS_CONFIG", "")

LOCATION = os.environ.get("PARSER_LOCATION", "us")  # Format is 'us' or 'eu'
# TODO In future - will be detected based on the form
PROCESSOR_ID = os.environ["PROCESSOR_ID"]
BUCKET_NAME = os.environ.get("BUCKET_NAME", PROJECT_ID)

# BigQuery Streaming
DATASET_NAME = os.environ["DATASET_NAME"]
ENTITIES_TABLE_NAME = os.environ["ENTITIES_TABLE_NAME"]

# INPUT - When Running Job to process bucket, file
GCS_INPUT_BUCKET_NAME = os.environ.get(
	"GCS_INPUT_BUCKET_NAME",
	BUCKET_NAME)  # needs to be existing

# OUTPUT -> Stores extracted JSON file
GCS_OUTPUT_BUCKET_NAME = os.environ.get(
	"GCS_OUTPUT_BUCKET_NAME",
	BUCKET_NAME)  # needs to be existing

GCS_OUTPUT_URI_PREFIX = os.environ.get("GCS_OUTPUT_PREFIX", "output")
GCS_INPUT_URI_PREFIX = os.environ.get("GCS_INPUT_URI_PREFIX", "forms")

DESTINATION_URI = f"gs://{GCS_OUTPUT_BUCKET_NAME}/{GCS_OUTPUT_URI_PREFIX}/"
ACCEPTED_MIME_TYPES = {
	"application/pdf", "image/jpeg", "image/png",
	"image/tiff", "image/gif"}

print(
	f"PROJECT_ID={PROJECT_ID}, "
	f"PROCESSOR_ID={PROCESSOR_ID}, "
	f"BUCKET_NAME={BUCKET_NAME}, "
	f"DATASET_NAME={DATASET_NAME}, "
	f"GCS_OUTPUT_URI_PREFIX={GCS_OUTPUT_URI_PREFIX}, "
	f"PROCESSORS_CONFIG={PROCESSORS_CONFIG}, "
	f"ENTITIES_TABLE_NAME={ENTITIES_TABLE_NAME}")
