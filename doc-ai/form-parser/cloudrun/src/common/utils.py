import re
import logging


def split_uri(uri: str):
	match = re.match(r"gs://([^/]+)/(.+)", uri)
	if not match:
		return "", ""
	bucket = match.group(1)
	prefix = match.group(2)
	return bucket, prefix


def extract_form_fields(doc_element: dict, document: dict):
	"""
	 # Extract form fields from form parser raw json
	  Parameters
	  ----------
	  doc_element: Entitiy
	  document: Extracted OCR Text

	  Returns: Entity name and Confidence
	  -------
	"""

	response = ""
	list_of_coordidnates = []
	# If a text segment spans several lines, it will
	# be stored in different text segments.
	for segment in doc_element.text_anchor.text_segments:
		start_index = (
			int(segment.start_index)
			if segment in doc_element.text_anchor.text_segments
			else 0
		)
		end_index = int(segment.end_index)
		response += document.text[start_index:end_index]
	confidence = doc_element.confidence
	coordinate = list([doc_element.bounding_poly.normalized_vertices])
	# print("coordinate", coordinate)
	# print("type", type(coordinate))

	for item in coordinate:
		# print("item", item)
		# print("type", type(item))
		for xy_coordinate in item:
			# print("xy_coordinate", xy_coordinate)
			# print("x", xy_coordinate.x)
			list_of_coordidnates.append(float(round(xy_coordinate.x, 4)))
			list_of_coordidnates.append(float(round(xy_coordinate.y, 4)))
	return response, confidence, list_of_coordidnates


def clean_form_parser_keys(text):
	"""
	  Cleaning form parser keys
	  Parameters
	  ----------
	  text: original text before noise removal - removed spaces, newlines
	  Returns: text after noise removal
	  -------
	"""
	# removing special characters from beginning and end of a string
	try:
		if len(text):
			text = text.strip()
			text = text.replace("\n", " ")
			text = re.sub(r"^\W+", "", text)
			last_word = text[-1]
			text = re.sub(r"\W+$", "", text)
		if last_word in [")", "]"]:
			text += last_word

	except: # pylint: disable=bare-except
		logging.warning("Exception occurred while cleaning keys")

	return text


def strip_value(value):
	'''Function for default cleaning of values to remove space at end and begining
	and '\n' at end
	Input:
		 value: Input string
	Output:
		 corrected_value: corrected string without noise'''
	if value is None:
		corrected_value = value
	else:
		corrected_value = value.strip()
		corrected_value = corrected_value.replace("\n", " ")
	return corrected_value