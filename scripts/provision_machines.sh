#! /usr/bin/env bash

# Usage: provision_machines.sh <num-instances>

ZONE="us-east1-b"

COMPUTE_NAME_PATTERN="vm##"
COMPUTE_COUNT=$1
COMPUTE_MACHINE_TYPE="e2-standard-2"
COMPUTE_IMAGE_PROJECT="ubuntu-os-cloud"
COMPUTE_IMAGE_FAMILY="ubuntu-2204-lts"
COMPUTE_STARTUP_SCRIPT="scripts/startup.sh"

# maybe teardown previous filestore instance?
NUM_STEPS=4

# create the machines
echo "Step 1/${NUM_STEPS}: Creating ${COMPUTE_COUNT} virtual machines..."

# create the client node
gcloud compute instances create vm00 \
    --zone=${ZONE} \
    --machine-type=${COMPUTE_MACHINE_TYPE} \
    --image-project=${COMPUTE_IMAGE_PROJECT} \
    --image-family=${COMPUTE_IMAGE_FAMILY} \

# create the participating nodes
gcloud compute instances bulk create \
    --name-pattern=${COMPUTE_NAME_PATTERN} \
    --count=${COMPUTE_COUNT} \
    --zone=${ZONE} \
    --machine-type=${COMPUTE_MACHINE_TYPE} \
    --image-project=${COMPUTE_IMAGE_PROJECT} \
    --image-family=${COMPUTE_IMAGE_FAMILY} \

echo "Step 2/${NUM_STEPS}: Sending configuration to client..."
sleep 30
gcloud compute scp scripts/config.json vm00:

echo "Step 3/${NUM_STEPS}: Starting binaries across machines..."
for num in $( seq 1 $COMPUTE_COUNT)
do
    SERVER_ID="vm$(printf %02d $num)"
    gcloud compute scp scripts/startup.sh scripts/config.json ${SERVER_ID}:
    gcloud compute ssh ${SERVER_ID} --command="chmod +x startup.sh \
        && ./startup.sh \
        && source ~/.bash_profile \
        && git clone https://github.com/zdmwi/rt3multipaxos.git \
        && cd rt3multipaxos/scripts \
        && ./build.sh \
        && cd .. && bin/server -id ${SERVER_ID} -config ~/config.json -algorithm=paxos"

done

echo "Step 4/${NUM_STEPS}: Starting client binary..."
gcloud compute scp scripts/startup.sh vm00:
gcloud compute ssh vm00 --command="chmod +x startup.sh \
    && ./startup.sh \
    && source ~/.bash_profile \
    && git clone https://github.com/zdmwi/rt3multipaxos.git \
    && cd rt3multipaxos/scripts \
    && ./build.sh \
    && cd .. \
    && bin/client -id vm00 -config ~/config.json"

echo "FINISHED!"