# type: ignore[1]
"""
Uses the Document AI online processing method to call a form parser processor
Extracts the key value pairs found in the document.
"""
import datetime
import os
import re
import json
import copy
from os import path

from . import config
import pandas as pd
from google.api_core.client_options import ClientOptions
from google.api_core.operation import Operation
from google.cloud import bigquery
from google.cloud import documentai_v1 as documentai
from typing import Any, Dict, List
from google.cloud import storage

ENTITY_KEYS = [
    "first_name",
    "last_name",
    "dob",
    "residential_address",
    "email",
    "phone_no",
]




if path.exists("/Users/evekhm/gitLab/claims-accelerator-demo/doc-ai/key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/evekhm/gitLab/claims-accelerator-demo/doc-ai/key.json"
# gcs_output_uri_prefix = os.environ.get("GCS_OUTPUT_URI_PREFIX") #TODO

# Attributes not required from specialized parser raw json
DOCAI_ATTRIBUTES_TO_IGNORE = [
    "textStyles", "textChanges", "revisions", "pages.image"
]

# GCS Variables

gcs_output_bucket = config.BUCKET_NAME
destination_uri = f"gs://{gcs_output_bucket}/{config.GCS_OUTPUT_URI_PREFIX}/"

# Document AI
client_options = ClientOptions(api_endpoint=f"{config.LOCATION}-documentai.googleapis.com")

docai_client = documentai.DocumentProcessorServiceClient(client_options=client_options)

storage_client = storage.Client()

extracted_entity_list = []




def _batch_process_documents(
    project_id: str,
    location: str,
    processor_id: str,
    gcs_input_uri: str,
    gcs_output_uri: str,
) -> Operation:
    """
    Constructs a request to process a document using the Document AI
    Batch Method.
    """

    print(f"Processing DOC AI request: project_id={project_id}, "
          f"location={location}, processor_id={processor_id}, "
          f"gcs_input_uri={gcs_input_uri}, gcs_output_uri={gcs_output_uri}")

    # The full resource name of the processor, e.g.:
    # projects/project-id/locations/location/processor/processor-id
    resource_name = docai_client.processor_path(project_id, location, processor_id)

    # Load GCS Input URI Prefix into Input Config Object
    input_config = documentai.BatchDocumentsInputConfig(
        gcs_prefix=documentai.GcsPrefix(gcs_uri_prefix=gcs_input_uri)
    )

    # Cloud Storage URI for Output directory
    gcs_output_config = documentai.DocumentOutputConfig.GcsOutputConfig(
        gcs_uri=gcs_output_uri
    )

    # Load GCS Output URI into Output Config Object
    output_config = documentai.DocumentOutputConfig(gcs_output_config=gcs_output_config)

    # Configure Process Request
    request = documentai.BatchProcessRequest(
        name=resource_name,
        input_documents=input_config,
        document_output_config=output_config,
    )

    # Future for long-running operations returned from Google Cloud APIs.
    operation = docai_client.batch_process_documents(request)

    return operation


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
        print("Exception occurred while cleaning keys")

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


def get_document_protos_from_gcs(
    output_bucket: str, output_directory: str
) -> List[documentai.Document]:
    """
    Download document proto output from GCS. (Directory)
    """
    # List of all of the files in the directory
    # `gs://gcs_output_uri/operation_id`
    blob_list = list(storage_client.list_blobs(output_bucket, prefix=output_directory))
    document_protos = []

    for blob in blob_list:
        # Document AI should only output JSON files to GCS
        if ".json" in blob.name:
            print("Fetching from " + blob.name)
            document = documentai.types.Document.from_json(blob.download_as_bytes())
            document_protos.append(document)

    return document_protos


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
        print(entity)
        # Fields detected. For a full list of fields for each processor see
        # the processor documentation:
        # https://cloud.google.com/document-ai/docs/processors-list
        extract_document_entity(entity)

        # Properties are Sub-Entities
        for prop in entity.properties:
            extract_document_entity(prop)

    return document_entities

def trim_text(text: str):
    """
    Remove extra space characters from text (blank, newline, tab, etc.)
    """
    return text.strip().replace("\n", " ")



def stream_form_to_bigquery(bq_client, form_dict, operation, timestamp, input_filename):
    table_id = f"{PROJECT_ID}.prior-auth.forms"
    #prior-auth-demo-2q1555924p:form_parser_results .doc_ai_extracted_entities
    table_id = f"{PROJECT_ID}.form_parser_results.doc_ai_extracted_entities"

    dataset_ref = bq_client.dataset(DATASET_NAME)
    table_ref = dataset_ref.table(ENTITIES_TABLE_NAME)

    job_config = bigquery.LoadJobConfig(
        autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    new_form_dict = {"operation": operation,
                     "timestamp": timestamp,
                     "form_name": input_filename,
                     "details": json.dumps(form_dict, indent = 4) }
    rows_to_insert = [new_form_dict]

        # rows_to_insert = [{u"COL1": 100.453, u"COL2": 108.75, u"COL3": 50.7773},
        #                   {u"COL1": 200.348, u"COL2": 208.29, u"COL3": 60.7773}]

    # print(rows_to_insert)
    job = bq_client.load_table_from_json(rows_to_insert, table_ref, job_config=job_config)
    print(job.result())  # Waits for table load to complete.


    # new_claim_dict = copy.deepcopy(claim_dict)
    # del new_claim_dict["document_details"]
    # new_claim_dict["operation"] = operation
    # new_claim_dict["timestamp"] = timestamp
    # new_claim_dict["created_timestamp"] = timestamp
    # new_claim_dict["last_updated_timestamp"] = timestamp
    # new_claim_dict["all_document_details"] = json.dumps(
    #     claim_dict.get("document_details"))
    # rows_to_insert = [new_claim_dict]
    # # Make an API request
    # errors = client.insert_rows_json(table_id, rows_to_insert)
    # if errors == []:
    #     Logger.info("New rows have been added.")
    # elif isinstance(errors, list):
    #     error = errors[0].get("errors")
    #     Logger.error(f"Encountered errors while inserting rows: {error}")

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


def extract_json(operation_id, input_filename):
    output_directory = f"{GCS_OUTPUT_URI_PREFIX}/{operation_id}"
    #output_directory="output/10086123606244357736/"
    print(f"Output Path: gs://{gcs_output_bucket}/{output_directory}")

    print("Output files:")

    output_document_protos = get_document_protos_from_gcs(
        gcs_output_bucket, output_directory
    )

    # Reading all entities into a dictionary to write into a BQ table

    for document_proto in output_document_protos:
        names = []
        name_confidence = []
        values = []
        value_confidence = []

        form_dict = {}
        for page in document_proto.pages:
            for form_field in page.form_fields:
                field_name, field_name_confidence, field_coordinates = \
                    extract_form_fields(form_field.field_name, document_proto)
                field_value, field_value_confidence, value_coordinates = \
                    extract_form_fields(form_field.field_value, document_proto)
                # noise removal from keys and values
                field_name = clean_form_parser_keys(field_name)
                field_value = strip_value(field_value)

                # names.append(field_name)
                # # Confidence - How "sure" the Model is that the text is correct
                # name_confidence.append(form_field.field_name.confidence)
                #
                # values.append(field_value)
                # value_confidence.append(form_field.field_value.confidence)

                # { Phone: [ value: "Esenina", confidence: 1.0 ], [ value: "Another Value", ] }
                if field_name not in form_dict.keys():
                    form_dict[field_name] = []

                field_dic_tmp = {"value": field_value,
                                 "field_value_confidence": round(
                                     field_value_confidence, 2),
                                 "page_no": int(page.page_number),
                                 }

                form_dict[field_name].append(field_dic_tmp)

                # temp_dict = {
                #     "key": field_name,
                #     "key_coordinates": field_coordinates,
                #     "value": field_value,
                #     "value_coordinates": value_coordinates,
                #     "key_confidence": round(field_name_confidence, 2),
                #     "value_confidence": round(field_value_confidence, 2),
                #     "page_no": int(page.page_number),
                #     "page_width": int(page.dimension.width),
                #     "page_height": int(page.dimension.height)
                # }
                # Create a Pandas Dataframe to print the values in tabular format.
                df = pd.DataFrame(
                    {
                        "Field Name": names,
                        "Field Name Confidence": name_confidence,
                        "Field Value": values,
                        "Field Value Confidence": value_confidence,
                    }
                )
                # print(df)
                # extracted_entity_list.append(temp_dict)

        entities = extract_document_entities(document_proto)
        entities["input_file_name"] = input_filename

        print("Entities:", entities)
        print("Writing DocAI Entities to BQ")

        timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

        client = bigquery.Client()

        # print(form_dict)
        stream_form_to_bigquery(client, form_dict, "CREATE", timestamp, input_filename)


        # entities = extract_document_entities(document_proto)
        # entities["input_file_name"] = input_filename
        #
        # print("Entities:", entities)
        # print("Writing DocAI Entities to BQ")
        #
        # # Add Entities to DocAI Extracted Entities Table
        # write_to_bq("DATSET_NAME", "ENTITIES_TABLE_NAME", entities)
    #
    #     # Send Address Data to PubSub
    #     for address_field in address_fields:
    #         if address_field in entities:
    #             process_address(address_field, entities[address_field], input_filename)
    #
    # cleanup_gcs(
    #     input_bucket,
    #     input_filename,
    #     gcs_output_bucket,
    #     output_directory,
    #     gcs_archive_bucket_name,
    # )


def write_to_bq(dataset_name, table_name, entities_extracted_dict):
    """
    Write Data to BigQuery
    """
    # dataset_ref = bq_client.dataset(dataset_name)
    # table_ref = dataset_ref.table(table_name)
    row_to_insert = []

    row_to_insert.append(entities_extracted_dict)

    json_data = json.dumps(row_to_insert, sort_keys=False)
    # Convert to a JSON Object
    json_object = json.loads(json_data)

    schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
    ]
    source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    job_config = bigquery.LoadJobConfig(
        schema_update_options=schema_update_options,
        source_format=source_format,
    )

    job = bq_client.load_table_from_json(json_object, table_ref, job_config=job_config)
    print(job.result())  # Waits for table load to complete.


def process(input_bucket, input_filename, mime_type, extract=False):
    """
    # Extract Form Entities and save as JSON
    # """

    # # input_bucket = event.get("bucket")
    # # input_filename = event.get("name")
    # # mime_type = event.get("contentType")
    #
    if not input_bucket or not input_filename:
        print("No bucket or filename provided")
        return

    if mime_type not in ACCEPTED_MIME_TYPES:
        print("Cannot parse the file type: " + mime_type)
        return

    print("Mime Type: " + mime_type)

    gcs_input_uri = f"gs://{input_bucket}/{input_filename}"

    print("Input File: " + gcs_input_uri)

    operation = _batch_process_documents(
        PROJECT_ID, LOCATION, PROCESSOR_ID, gcs_input_uri, destination_uri
    )

    print("Document Processing Operation: " + operation.operation.name)

    # Wait for the operation to finish
    operation.result(operation.result(timeout=400))

    # Output files will be in a new subdirectory with Operation ID as the name
    operation_id = re.search(
        r"operations\/(\d+)", operation.operation.name, re.IGNORECASE
    ).group(1)

    if extract:
        extract_json(operation_id, input_filename)

    return




