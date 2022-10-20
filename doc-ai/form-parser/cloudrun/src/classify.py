from google.cloud import storage
from google.cloud import logging
from common.processor import Processor

# Instantiates a client
logging_client = logging.Client()
logging_client.setup_logging()
logger = logging_client.logger(__name__)

storage_client = storage.Client()

processor_to_forms = {}
PROCESSORS = Processor.load_processors()


def select_processor(
	bucket_name: str,
	prefix_uri: str
):
	# #TODO: load the Form and Using Classifier select Processor and Configuration
	if "CVS-Global" in prefix_uri:
		selected_processor = next(proc for proc in PROCESSORS if proc.name == "CVS_FORM_GENERIC")
	elif "CVS" in prefix_uri:
		selected_processor = next(proc for proc in PROCESSORS if proc.name == "CVS_FORM_CA")
	elif "BSC" in prefix_uri:
		selected_processor = next(proc for proc in PROCESSORS if proc.name == "BSC")
	else:
		selected_processor = next(proc for proc in PROCESSORS if proc.name == "default")

	logger.log_text(
		f"Form: gs://{bucket_name}/{prefix_uri} -> "
		f"Processor: {selected_processor.name} ({selected_processor.type})")
	print(
		f"Form: gs://{bucket_name}/{prefix_uri} -> Processor: {selected_processor.name}")
	return selected_processor
