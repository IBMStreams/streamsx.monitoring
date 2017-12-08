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
STREAMSX_JSON_TOOLKIT ?=$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.json
STREAMSX_TOPOLOGY_TOOLKIT ?=$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.topology
SPL_PATH = $(STREAMSX_MONITORING_TOOLKIT):$(STREAMSX_JSON_TOOLKIT):$(STREAMSX_TOPOLOGY_TOOLKIT)

SPLC_FLAGS += -t $(SPL_PATH)
SPL_DATA_DIR = ./data

SPL_CMD_ARGS ?= 
SPL_MAIN_COMPOSITE = Main

MICROSERVICES_DIR =../../../../microservices

build: distributed 

all: clean build
	cd $(MICROSERVICES_DIR); make -f Makefile all

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) --data-directory $(SPL_DATA_DIR) --output-directory=$(OUTPUT_DIR)

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf $(OUTPUT_DIR)

configure:
	-streamtool rmappconfig --noprompt com.ibm.streamsx.monitoring.microservices.ApplicationConfiguration
	streamtool mkappconfig --property user=$(JMX_USER) --property password=$(JMX_PASSWORD) --property thresholdDocument=`cat etc/microservices.json | perl -e 'my @content = <STDIN>; my $$str = join("", @content); $$str =~ s/\s//g; print $$str;'` com.ibm.streamsx.monitoring.microservices.ApplicationConfiguration

configure-none:
	-streamtool rmappconfig --noprompt com.ibm.streamsx.monitoring.microservices.ApplicationConfiguration

start-monitor:
	streamtool submitjob $(MICROSERVICES_DIR)/MetricsIngestService/output/com.ibm.streamsx.monitoring.service.MetricsIngestService.sab --jobname microserviceMetrics -P applicationConfigurationName=com.ibm.streamsx.monitoring.microservices.ApplicationConfiguration
	streamtool submitjob $(MICROSERVICES_DIR)/MetricsMonitorService/output/com.ibm.streamsx.monitoring.service.MetricsMonitorService.sab --jobname microserviceMonitor -P applicationConfigurationName=com.ibm.streamsx.monitoring.microservices.ApplicationConfiguration
	streamtool submitjob output/Main.sab --jobname testMicroservices

stop-monitor:
	streamtool canceljob --jobnames microserviceMetrics --collectlogs
	streamtool canceljob --jobnames microserviceMonitor --collectlogs
	streamtool canceljob --jobnames testMicroservices --collectlogs

