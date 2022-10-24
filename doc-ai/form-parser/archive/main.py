import common1


# pylint: disable=unused-argument
def process_form(event, context):
  """
  # Extract Invoice Entities and Save to BQ
  # """

  input_bucket = event.get("bucket")
  input_filename = event.get("name")
  mime_type = event.get("contentType")

  print(f"input_bucket={input_bucket}, input_filename={input_filename}, "
        f"mime_type={mime_type}")
  common1.process(input_bucket, input_filename, mime_type)

  return
