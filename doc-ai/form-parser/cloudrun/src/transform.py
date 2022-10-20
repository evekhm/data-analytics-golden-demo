import copy
from typing import Any
from typing import Dict

import insert
from common.config import DATASET_NAME
from common.config import ENTITIES_TABLE_NAME


# Normalizes Data Using Mappings
def batch_transform(
	entities_extracted_dict: Dict[str, Any],
	context_args:  Dict[str, Any],
	normilize_args: Dict[str, Any]  # such as mapping config
):

	print(f"{'*' * 15} Reading Transformation step on the retrieved entities {'*' * 15}")
	#####
	# TODO Normalization
	#####
	entities_normalized_dict = copy.deepcopy(entities_extracted_dict)
	insert.write_to_bq(DATASET_NAME, ENTITIES_TABLE_NAME, entities_normalized_dict, context_args)

