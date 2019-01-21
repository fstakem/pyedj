# Build
#
# The primary targets in this file are:
#
# build         Build the docker container
# clean         Clean the build
# run           Run the docker container
# run-mqtt		Run mqtt broker
# run-pub		Run mqtt publisher
# run-sub		Run mqtt subscriber
#

# Directories
mkfile_path 	:= $(abspath $(lastword $(MAKEFILE_LIST)))
current_dir   	:= $(notdir $(patsubst %/,%,$(dir $(mkfile_path))))
build_dir     	:= ./build
script_dir    	:= ./scripts/build
test_dir	  	:= ./test
int_dir	  		:= ($test_dir)/integration

# Variables
server = 'localhost'
port = 1883
topic = 'ingestion/test/'


build:
	$(script_dir)/build.sh

clean:
	$(script_dir)/clean.sh

run:
	$(script_dir)/run.sh

run-mqtt:
	$(script_dir)/run_mosquitto.sh

run-pub:
	$(int_dir)/run_pub.py -s $(server) -p $(port) -t $(topic)

run-sub:
	$(int_dir)/run_sub.py -s $(server) -p $(port) -t $(topic)

.PHONY: build clean run run-mqtt run-pub run-sub
