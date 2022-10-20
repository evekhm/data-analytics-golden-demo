from typing import Any
from typing import Dict

from google.cloud import bigquery
import json
import datetime

bq_client = bigquery.Client()


def write_to_bq(
	dataset_name: str,
	table_name: str,
	entities_normalized: Dict[str, Any],
	context_args:  Dict[str, Any]):
	"""
	Write Data to BigQuery
	"""

	print(f"{'*' * 15} Streaming Processed data into the BigQuery [{dataset_name}].[{table_name}] {'*' * 15}")
	dataset_ref = bq_client.dataset(dataset_name)
	table_ref = dataset_ref.table(table_name)

	schema_update_options = [
		bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
		bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
	]
	source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

	job_config = bigquery.LoadJobConfig(
		autodetect=True,
		schema_update_options=schema_update_options,
		source_format=source_format,
	)

	timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
	operation = "CREATE"
	new_form_dict = {"operation": operation,
					 "timestamp": timestamp,
					 "details": json.dumps(entities_normalized, indent = 4) }

	data = {**context_args, **new_form_dict}
	# context_args.update(new_form_dict)
	json_data = json.dumps([data], sort_keys=False)
	# Convert to a JSON Object
	json_object = json.loads(json_data)

	job = bq_client.load_table_from_json(json_object, table_ref,
										 job_config=job_config)
	print(job.result())  # Waits for table load to complete.
