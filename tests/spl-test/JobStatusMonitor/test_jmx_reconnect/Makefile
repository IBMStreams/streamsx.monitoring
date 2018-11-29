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
SPL_DATA_DIR = ./data

SPL_CMD_ARGS ?= 
SPL_MAIN_COMPOSITE = Monitor
SPL_MAIN_COMPOSITE1 = SampleJob

build: distributed 

all: clean build

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) --data-directory $(SPL_DATA_DIR) --output-directory=$(OUTPUT_DIR)/monitor
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE1) $(SPL_CMD_ARGS) --data-directory $(SPL_DATA_DIR) --output-directory=$(OUTPUT_DIR)/sample

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf $(OUTPUT_DIR)

configure:
	-streamtool rmappconfig --noprompt com.ibm.streamsx.monitoring.jobs.JobStatusMonitor.ApplicationConfiguration
	streamtool mkappconfig --property domainId=$(TEST_DOMAIN) --property user=$(STREAMS_USERNAME) --property password=$(STREAMS_PASSWORD) com.ibm.streamsx.monitoring.jobs.JobStatusMonitor.ApplicationConfiguration

start-test-domain:
	streamtool startdomain -d $(TEST_DOMAIN)
	streamtool startinst -d $(TEST_DOMAIN) -i $(TEST_INSTANCE)

start-sample:
	streamtool submitjob output/sample/SampleJob.sab --jobname sampleJmxReconnectJobStatus -i $(TEST_INSTANCE) -d $(TEST_DOMAIN)

start-monitor:
	streamtool submitjob output/monitor/Monitor.sab --jobname monitorJmxReconnectJobStatus -P domainId=$(TEST_DOMAIN) -P user=$(STREAMS_USERNAME) -P password=$(STREAMS_PASSWORD) -P connectionURL=`streamtool getjmxconnect -d $(TEST_DOMAIN)`

stop-test-domain:
	streamtool stopinst -d $(TEST_DOMAIN) -i $(TEST_INSTANCE)
	streamtool stopdomain -d $(TEST_DOMAIN)

stop-sample:
	streamtool canceljob --jobnames sampleJmxReconnectJobStatus --force -i $(TEST_INSTANCE) -d $(TEST_DOMAIN)

stop-monitor:
	streamtool canceljob --jobnames monitorJmxReconnectJobStatus --collectlogs

