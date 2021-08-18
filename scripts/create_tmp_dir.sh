#!/bin/bash

# creates current date variable for Eastern Daylight Time
date_EDT="$(TZ="EST5EDT" date +'%Y-%m-%d')"
mkdir -p /tmp/data/${date_EDT}


