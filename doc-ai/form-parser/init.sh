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

exit
JSON_BUCKET="output-json"

gcloud beta functions deploy form_parser --gen2 \
  --runtime python37 \
  --trigger-bucket="${BUCKET_NAME}-test" \
  --source="${DIR}" --entry-point="process_form" \
  --ingress-settings=all \
  --project="$PROJECT_ID" \
  --timeout=540 \
  --set-env-vars=ENTITIES_TABLE_NAME="$ENTITIES_TABLE_NAME",DATASET_NAME="$DATASET_NAME",PROCESSOR_ID="$PROCESSOR_ID",BUCKET_NAME="$BUCKET_NAME", GCS_OUTPUT_PREFIX=$JSON_BUCKET \
  --service-account="${SERVICE_ACCOUNT}"

gcloud beta functions deploy json_extract --gen2 \
  --runtime python37 \
  --trigger-bucket="${JSON_BUCKET}" \
  --source="${DIR}" --entry-point="process_form" \
  --ingress-settings=all \
  --project="$PROJECT_ID" \
  --timeout=540 \
  --set-env-vars=ENTITIES_TABLE_NAME="$ENTITIES_TABLE_NAME",DATASET_NAME="$DATASET_NAME",PROCESSOR_ID="$PROCESSOR_ID",BUCKET_NAME="$BUCKET_NAME", GCS_OUTPUT_PREFIX="output-test" \
  --service-account="${SERVICE_ACCOUNT}"

gcloud beta functions deploy test  \
  --runtime python37 \
  --trigger-bucket="${PROJECT_ID}" \
  --source="${DIR}/test" --entry-point="test" \
  --ingress-settings=all \
  --project="$PROJECT_ID" \
  --timeout=540 \
  --service-account="${SERVICE_ACCOUNT}"


gsutil cp processors.json gs://$BUCKET_NAME/demo/

#Cloud Pub/Sub needs the role roles/iam.serviceAccountTokenCreator granted to service account service-793378548385@gcp-sa-pubsub.iam.gserviceaccount.com on this project to create identity tokens. You can change this later.
#This trigger needs the role roles/pubsub.publisher granted to service account service-793378548385@gs-project-accounts.iam.gserviceaccount.com to receive events via Cloud Storage.

#To grant limited access to Cloud Build to deploy to a Cloud Run service:
gcloud iam service-accounts add-iam-policy-binding \
  "$PROJECT_NUM"-compute@developer.gserviceaccount.com \
  --member="serviceAccount:$PROJECT_ID@cloudbuild.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"

##################
# Cloud Build
# TODO: https://cloud.google.com/build/docs/deploying-builds/deploy-cloud-run#gcloud
# set the status of the Cloud Run Admin role to ENABLED:
#ServiceAccount user ENABLED

gcloud iam service-accounts add-iam-policy-binding \
  "${SERVICE_ACCOUNT}" \
  --member="serviceAccount:${PROJECT_NUM}@cloudbuild.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"


############
gcloud services enable \
    cloudresourcemanager.googleapis.com \
    container.googleapis.com \
    sourcerepo.googleapis.com \
    cloudbuild.googleapis.com \
    containerregistry.googleapis.com \
    --async

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member=serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com \
    --role=roles/container.developer


#Permission "eventarc.events.receiveEvent" denied on "793378548385-compute@developer.gserviceaccount.com"
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member=serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com \
    --role=roles/eventarc.admin



 gcloud builds submit --region=$REGION  --substitutions=_IMAGE='pa-parser'