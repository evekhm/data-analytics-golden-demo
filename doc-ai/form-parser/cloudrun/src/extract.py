import re

from transform import batch_transform
from common.utils import extract_form_fields, clean_form_parser_keys, \
	strip_value
from google.cloud import documentai_v1 as documentai
from google.cloud import storage
from google.api_core.client_options import ClientOptions
from google.cloud import logging
from common.processor import Processor
from common.config import PROJECT_ID, DATASET_NAME, ENTITIES_TABLE_NAME, GCS_INPUT_BUCKET_NAME
import classify
from typing import Any, Dict, List

# Instantiates a client
logging_client = logging.Client()
logging_client.setup_logging()
logger = logging_client.logger(__name__)

storage_client = storage.Client()

processor_to_forms = {}
PROCESSORS = Processor.load_processors()


# Extracts Entities from Forms and saves as JSON
# For each Form identifies matching Processor and mapping
# The output JSON file contains info about the Processor used
def batch_extract_docs(
	bucket_name: str,
	prefix_uri: str
):
	def process_forms(
		processor: Processor,
		documents_list: [],
		timeout: int = 400
	):
		"""
		This is form parser extraction main function. It will send
		request to parser and retrieve response and call
			default and derived entities functions

		Parameters
			----------
			:param processor: Processor for DocAI parsing. Has info like parser id, name, location, and etc
			:param documents_list: list of Document gcs path in form of dictionary
			:param timeout: Max time given for extraction entities using async form parser API

		Returns: Form parser response - list of dicts having entity, value,
			confidence and manual_extraction information.
			-------
		"""

		print(processor)
		print(
			f"Using [{processor.name}] processor with  documents list: {documents_list}")

		gcs_documents = documentai.GcsDocuments(documents=documents_list)
		input_config = documentai.BatchDocumentsInputConfig(
			gcs_documents=gcs_documents)

		destination_uri = processor.destination_uri
		output_config = documentai.DocumentOutputConfig(
			gcs_output_config={"gcs_uri": destination_uri})

		# Instantiates a client
		client_options = ClientOptions(
			api_endpoint=f"{processor.location}-documentai.googleapis.com")
		docai_client = documentai.DocumentProcessorServiceClient(
			client_options=client_options)

		# The full resource name of the processor, e.g.:
		# projects/project-id/locations/location/processor/processor-id
		resource_name = docai_client.processor_path(PROJECT_ID,
													processor.location,
													processor.id)

		logger.log_text(
			f"processor_id = {processor.id}, input_config = {input_config}, output_config = {output_config}")

		# Configure Process Request
		request = documentai.BatchProcessRequest(
			name=resource_name,
			input_documents=input_config,
			document_output_config=output_config,
		)

		# async def my_callback(future):
		# 	result = await future.result()
		# 	print("Done!")

		operation = docai_client.batch_process_documents(request)
		# operation.add_done_callback(my_callback)

		print(f"Waiting for operation {operation.operation.name} to complete...")

		# async def my_callback(future):
		# 	result = await future.result()
		# 	# Once the operation is complete,
		# 	# get output document information from operation metadata
		# 	metadata = documentai.BatchProcessMetadata(operation.metadata)
		#
		# 	if metadata.state != documentai.BatchProcessMetadata.State.SUCCEEDED:
		# 		raise ValueError(f"Batch Process Failed: {metadata.state_message}")
		#
		# 	for process in metadata.individual_process_statuses:
		# 		# output_gcs_destination format: gs://BUCKET/PREFIX/OPERATION_NUMBER/0
		# 		# The GCS API requires the bucket name and URI prefix separately
		# 		output_bucket, output_prefix = re.match(
		# 			r"gs://(.*?)/(.*)", process.output_gcs_destination
		# 		).groups()
		#
		# 		extract_from_json(output_bucket, output_prefix, process.input_gcs_source)

		# operation.add_done_callback(my_callback)
		# # Wait for the operation to finish
		result = operation.result(timeout=timeout)

		# Once the operation is complete,
		# get output document information from operation metadata
		metadata = documentai.BatchProcessMetadata(operation.metadata)

		if metadata.state != documentai.BatchProcessMetadata.State.SUCCEEDED:
			raise ValueError(f"Batch Process Failed: {metadata.state_message}")

		for process in metadata.individual_process_statuses:
			# output_gcs_destination format: gs://BUCKET/PREFIX/OPERATION_NUMBER/0
			# The GCS API requires the bucket name and URI prefix separately
			output_bucket, output_prefix = re.match(
				r"gs://(.*?)/(.*)", process.output_gcs_destination
			).groups()

			extract_from_json(output_bucket, output_prefix, process.input_gcs_source)

	print(f"{'*' * 15} Running Document Extraction on bucket_name={bucket_name} and uri={prefix_uri} {'*' * 15}")

	# Get List of Document Objects from the Output Bucket
	bucket = storage_client.get_bucket(bucket_name)
	blob_list = list(bucket.list_blobs(prefix=prefix_uri))

	# Browse through output Forms and identify matching Processor for each Form
	for i, blob in enumerate(blob_list):
		if blob.name and not blob.name.endswith('/'):
			processor = classify.select_processor(bucket_name, blob.name)
			if processor not in processor_to_forms.keys():
				processor_to_forms[processor] = []

			gcs_doc_path = f"gs://{bucket_name}/{blob.name}"
			gcs_document = {
				"gcs_uri": gcs_doc_path,
				"mime_type": blob.content_type
			}
			processor_to_forms[processor].append(gcs_document)

	# TODO Can be run in parallel tasks no sequential
	tasks = []

	for _processor_ in processor_to_forms:
		process_forms(_processor_, processor_to_forms[_processor_])



def get_document_protos_from_gcs(
	output_bucket: str, output_directory: str
) -> List[documentai.Document]:
	"""
	Download document proto output from GCS. (Directory)
	"""
	# List of all of the files in the directory
	# `gs://gcs_output_uri/operation_id`
	blob_list = list(
		storage_client.list_blobs(output_bucket, prefix=output_directory))
	document_protos = []

	for blob in blob_list:
		# Document AI should only output JSON files to GCS
		if ".json" in blob.name:
			print("Fetching from " + blob.name)
			document = documentai.types.Document.from_json(
				blob.download_as_bytes())
			document_protos.append(document)

	return document_protos


#  For Specialized Processor
def extract_document_entities(document: documentai.Document) -> dict:
	"""
	Get all entities from a document and output as a dictionary
	Flattens nested entities/properties
	Format: entity.type_: entity.mention_text OR entity.normalized_value.text
	"""
	document_entities: Dict[str, Any] = {}

	def extract_document_entity(entity: documentai.Document.Entity):
		"""
		Extract Single Entity and Add to Entity Dictionary
		"""
		entity_key = entity.type_.replace("/", "_")
		normalized_value = getattr(entity, "normalized_value", None)

		new_entity_value = (
			normalized_value.text if normalized_value else entity.mention_text
		)

		existing_entity = document_entities.get(entity_key)

		# For entities that can have multiple (e.g. line_item)
		if existing_entity:
			# Change Entity Type to a List
			if not isinstance(existing_entity, list):
				existing_entity = list([existing_entity])

			existing_entity.append(new_entity_value)
			document_entities[entity_key] = existing_entity
		else:
			document_entities.update({entity_key: new_entity_value})

	for entity in document.entities:
		# Fields detected. For a full list of fields for each processor see
		# the processor documentation:
		# https://cloud.google.com/document-ai/docs/processors-list
		extract_document_entity(entity)

		# Properties are Sub-Entities
		for prop in entity.properties:
			extract_document_entity(prop)

	return document_entities


# For Form Processor
#  For Specialized Processor
def extract_document_entities_form(document: documentai.Document,
	document_entities: Dict[str, Any] ):
	"""
	Get all entities from a document and output as a dictionary
	Flattens nested entities/properties
	Format: entity.type_: entity.mention_text OR entity.normalized_value.text
	"""

	for page in document.pages:
		for form_field in page.form_fields:
			field_name, field_name_confidence, field_coordinates = \
				extract_form_fields(form_field.field_name, document)
			field_value, field_value_confidence, value_coordinates = \
				extract_form_fields(form_field.field_value, document)
			# noise removal from keys and values
			field_name = clean_form_parser_keys(field_name)
			field_value = strip_value(field_value)

			existing_entity = document_entities.get(field_name)
			# For entities that can have multiple (e.g. line_item)
			if existing_entity:
				# Change Entity Type to a List
				if not isinstance(existing_entity, list):
					existing_entity = list([existing_entity])
				existing_entity.append(field_value)
				document_entities[field_name] = existing_entity
			else:
				document_entities.update({field_name: field_value})

			# temp_dict = {
			# 	"key": field_name,
			# 	"key_coordinates": field_coordinates,
			# 	"value": field_value,
			# 	"value_coordinates": value_coordinates,
			# 	"key_confidence": round(field_name_confidence, 2),
			# 	"value_confidence": round(field_value_confidence, 2),
			# 	"page_no": int(page.page_number),
			# 	"page_width": int(page.dimension.width),
			# 	"page_height": int(page.dimension.height)
			# }
			#
			# extracted_entity_list.append(temp_dict)

	return document_entities


def find_processor(name: str, processors: List[Processor]):
	result = [p for p in processors if p.name == name]
	if len(result) > 0:
		return result[0]
	return None


# Uses json output from DocAI operations
def extract_from_json(
	output_bucket: str,  # Format: gs://{bucket_name}/full/path/to/operationid
	prefix: str,
	input_filename=None
):
	print(f"{'*' * 15} Reading JSON data extracted by DocumentAI and saved into gs://{output_bucket}/{prefix} {'*' * 15}")
	parser_details = ""

	match = re.match(r"([^/]+)/(.+)/(.+)/(.+)", prefix)
	if len(match.groups()) >= 3:
		parser_name = match.group(2)
		parser_details += parser_name
		processor = find_processor(parser_name, PROCESSORS)
		if processor:
			print(
				f"Starting normalization using {parser_name} Processor with {processor.mapping_file} mapping config for mappings")
			parser_details += " (" + processor.description + ")"

	bucket = storage_client.get_bucket(output_bucket)
	blob_list = list(bucket.list_blobs(prefix=prefix))
	document_entities: Dict[str, Any] = {}

	for i, blob in enumerate(blob_list):
		if ".json" in blob.name:
			print(f"Fetching JSON from gs://{output_bucket}/{blob.name}")
			document = documentai.Document.from_json(
				blob.download_as_bytes(), ignore_unknown_fields=True
			)
			# extracted_entity_list.append()
			extract_document_entities_form(document, document_entities)
			for k in document_entities.keys():
				print(f"{k}: {document_entities[k]}")

	# print(json.dumps(entity, indent=4, sort_keys=True))
	context_args = {
		"input_file": input_filename,
		"parser": parser_details
	}
	batch_transform(document_entities, context_args)


# def task_cb(context):
# 	print("Task completion received...")
# 	print("Name of the task:%s"%context.get_name())
# 	print("Wrapped coroutine object:%s"%context.get_coro())
# 	print("Task is done:%s"%context.done())
# 	print("Task has been cancelled:%s"%context.cancelled())
# 	print("Task result:%s"%context.result())
# 	print(type(context))
# 	print(context)
#
# # A simple Python coroutine
# async def simple_coroutine():
# 	await asyncio.sleep(1)
# 	return 1
#
# async def main():
# 	t1 = asyncio.create_task(simple_coroutine())
# 	t1.add_done_callback(task_cb)
# 	await t1
# 	print("Coroutine main() exiting")

# el = asyncio.new_event_loop()
# asyncio.set_event_loop(el)
# asyncio.run(batch_extract_docs(GCS_INPUT_BUCKET_NAME, "test"))

batch_extract_docs(GCS_INPUT_BUCKET_NAME, "test")
# asyncio.run(batch_extract_docs(GCS_INPUT_BUCKET_NAME, "pa-forms"))
#extract_from_json(GCS_INPUT_BUCKET_NAME, "output/default/14391054591511450495/0", "default-form")


