from .utils import split_uri
import json
from .config import GCS_OUTPUT_BUCKET_NAME, GCS_OUTPUT_URI_PREFIX, PROCESSORS_CONFIG
from google.cloud import storage

client = storage.Client()


class Processor:

	def __init__(
			self, processor_id, location, processor_type,
			processor_name, mapping_file, golden_forms=None, description=None):

		self.id = processor_id
		self.type = processor_type  # processor type, like forms, ocr, specialized
		self.name = processor_name  # must be unique
		self.location = location

		if description is None:
			description = f"{processor_name}"
		self.description = description
		self.golden_forms = golden_forms

		self.mapping_file = mapping_file
		self.destination_uri = f"gs://{GCS_OUTPUT_BUCKET_NAME}/" \
							   f"{GCS_OUTPUT_URI_PREFIX}/" \
							   f"{self.name}"

	def __repr__(self):
		str_txt = "Processor: "
		for k in self.__dict__.keys():
			str_txt += f"{k}={self.__dict__[k]} "
		return str_txt



	@staticmethod
	def load_processors():
		bucket_name, prefix = split_uri(PROCESSORS_CONFIG)
		bucket = client.get_bucket(bucket_name)
		blob = bucket.get_blob(prefix)

		data = json.loads(blob.download_as_text(encoding="utf-8"))
		processors = []

		#Todo use object_hook and encode/decode in Python
		for processor_str in data['processors']:
			processor = Processor(
				processor_id=processor_str["id"],
				location=processor_str["location"],
				processor_type=processor_str["type"],
				processor_name=processor_str["name"],
				description=processor_str["description"],
				mapping_file=processor_str["mapping_file"],
				golden_forms=processor_str["golden_forms"]
			)
			processors.append(processor)


		print(f"Loaded active processors from  {PROCESSORS_CONFIG} file:")
		for p in processors:
			print(str(p))
		# print(' '.join(str(processors)))
		return processors
