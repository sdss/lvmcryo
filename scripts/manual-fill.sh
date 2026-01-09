#!/usr/bin/env bash

# This script is used to manually fill the LVM cryostats. It is mostly intended
# for emergency purposes so that LCO staff can run the fill procedure with
# minimal training.

# Colour for output
LIGHT_GRAY='\033[0;37m'
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

declare LVMCRYO_ENV
if [[ -n "${LVMCRYO_VERSION:-}" ]]; then
    LVMCRYO_ENV="lvmcryo-${LVMCRYO_VERSION}"
else
    LVMCRYO_ENV="lvmcryo"
fi

echo -e "${YELLOW}Starting manual fill procedure ... ${NC}"

# Load the environment
echo -en "${LIGHT_GRAY}Loading lvmcryo environment (${LVMCRYO_ENV}) ... ${NC}"
pyenv shell "${LVMCRYO_ENV}"

# Disable auto fills
echo -en "${LIGHT_GRAY}Disabling automatic fills ... ${NC}"
if ! kubectl delete -f /home/sdss5/config/kube/cronjobs/ln2fill_2_fills.yml > /dev/null 2>&1; then
    echo -e "${RED}FAILED${NC}"
    exit 1
fi
echo -e "${GREEN}OK${NC}"

# Clear locks
echo -en "${LIGHT_GRAY}Clearing any existing locks and cancelling other fills ... ${NC}"
if ! lvmcryo clear-lock > /dev/null 2>&1; then
    echo -e "${RED}FAILED${NC}"
    pyenv shell --unset
    exit 1
fi
echo -e "${GREEN}OK${NC}"

# Turn off ion pumps
echo -en "${LIGHT_GRAY}Turning off ion pumps ... ${NC}"
if ! lvmcryo ion --off > /dev/null 2>&1; then
    echo -e "${RED}FAILED${NC}"
    pyenv shell --unset
    exit 1
fi
echo -e "${GREEN}OK${NC}"

# Start purge and fill
echo -e "${YELLOW}Starting purge and fill ... ${NC}\n"
if ! lvmcryo ln2 --profile manual-fill; then
    echo -e "${RED}FILL FAILED - Please report this error.${NC}"
    pyenv shell --unset
    exit 1
fi
echo -e "\n${GREEN}FILL COMPLETED SUCCESSFULLY${NC}"
pyenv shell --unset

# Exit successfully
exit 0
