"""
    An application to extract information from PDF forms and update a
    database with that information. Intended to run in Cloud Run Jobs.

    Requirements:

    -   Python 3.7 or later
    -   All packages in requirements.txt installed
    -   A bucket with the forms files in the /GCS_INPUT_PREFIX folder
    -   Streams data to BigQuery
    -   The name of the bucket (not the URI) in the environment variable BUCKET
    -   The name of the folder with forms (not the URI) in the environment variable GCS_INPUT_PREFIX
    -   The DocumentAI processor in the environment variable PROCESSOR_ID

    This app can be run directly via "python main.py".
"""

import os

import common1

GCS_INPUT_PREFIX = os.environ["GCS_INPUT_PREFIX"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
gcs_input_bucket = BUCKET_NAME


def process_batch():
  common1.process(gcs_input_bucket, GCS_INPUT_PREFIX, "application/pdf", True)


process_batch()

