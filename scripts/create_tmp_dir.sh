#!/bin/bash

# creates current date variable based on Eastern Daylight Time
date_EDT="$(TZ="EST5EDT" date +'%Y-%m-%d')"
mkdir -p /tmp/data/${date_EDT}