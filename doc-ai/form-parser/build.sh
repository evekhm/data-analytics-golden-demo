DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${DIR}"/SET

PROJECT_ID=$(gcloud config get-value core/project)
PROJECT_NUM=$(gcloud projects describe "$PROJECT_ID" --format='get(projectNumber)')
gcloud builds submit --tag=gcr.io/"${PROJECT_ID}"/"${IMAGE}"

cd "$DIR/cloudrun"
gcloud builds submit --region=$REGION  --substitutions=_IMAGE='pa-parser'

if gcloud beta run jobs describe  ${JOB}  2>/dev/null; then
  gcloud beta run jobs delete ${JOB} --quiet
fi
gcloud beta run jobs create ${JOB} \
--image=gcr.io/"${PROJECT_ID}"/"${IMAGE}" \
--set-env-vars=PROCESSOR_ID="$PROCESSOR_ID" \
--set-env-vars=DATASET_NAME="$DATASET_NAME" \
--set-env-vars=ENTITIES_TABLE_NAME="$ENTITIES_TABLE_NAME" \
--set-env-vars=BUCKET_NAME="$BUCKET_NAME" \
--set-env-vars=PROCESSORS_CONFIG="$PROCESSORS_CONFIG" \
--set-env-vars=GCS_INPUT_PREFIX="${GCS_INPUT_PREFIX}" \
--service-account="${SERVICE_ACCOUNT}"

cd ..
gcloud beta run jobs describe  ${JOB}

gcloud beta run jobs execute "$JOB"


 gcloud builds submit --region=$REGION  --substitutions=_IMAGE='pa-parser'