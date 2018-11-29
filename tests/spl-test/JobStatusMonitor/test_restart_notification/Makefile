# Copyright (C) 2017, International Business Machines Corporation. 
# All Rights Reserved.

.PHONY: build all clean distributed

SPLC_FLAGS = -a
OUTPUT_DIR = output
ifeq ($(STREAMS_INSTALL),)
$(error error: environment variable STREAMS_INSTALL has to be set)
endif
STREAMSX_MONITORING_TOOLKIT ?=../../../../com.ibm.streamsx.monitoring
SPLC = $(STREAMS_INSTALL)/bin/sc
SPL_PATH = $(STREAMSX_MONITORING_TOOLKIT)
SPLC_FLAGS += -t $(SPL_PATH)

SPL_CMD_ARGS ?= 
SPL_MAIN_COMPOSITE = Monitor
SPL_MAIN_COMPOSITE1 = SampleJob

build: distributed 

all: clean build

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)/monitor
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE1) $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)/sample

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf $(OUTPUT_DIR)

configure:
	-streamtool rmappconfig --noprompt com.ibm.streamsx.monitoring.jobs.JobStatusMonitor.ApplicationConfiguration
	streamtool mkappconfig --property user=$(STREAMS_USERNAME) --property password=$(STREAMS_PASSWORD) com.ibm.streamsx.monitoring.jobs.JobStatusMonitor.ApplicationConfiguration

start-sample:
	streamtool submitjob output/sample/SampleJob.sab --jobname restartNotificationJobStatus

stop-sample:
	streamtool canceljob --jobnames restartNotificationJobStatus --force
