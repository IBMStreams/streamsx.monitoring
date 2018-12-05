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

build: distributed 

all: clean build
	$(SPLC) $(SPLC_FLAGS) -M com.ibm.streamsx.monitoring.metrics.services::MetricsIngestService $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)/MetricsIngestService
	$(SPLC) $(SPLC_FLAGS) -M com.ibm.streamsx.monitoring.metrics.services::MetricsMonitorService $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)/MetricsMonitorService

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) --data-directory $(SPL_DATA_DIR) --output-directory=$(OUTPUT_DIR)

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf $(OUTPUT_DIR)

configure:
	-streamtool rmappconfig --noprompt thresholds
	streamtool mkappconfig --property thresholdDocument=`cat etc/microservices.json | perl -e 'my @content = <STDIN>; my $$str = join("", @content); $$str =~ s/\s//g; print $$str;'` thresholds
	-streamtool rmappconfig --noprompt monitoring
	streamtool mkappconfig --property user=$(STREAMS_USERNAME) --property password=$(STREAMS_PASSWORD) monitoring

configure-none:
	-streamtool rmappconfig --noprompt monitoring
	-streamtool rmappconfig --noprompt thresholds

start-monitor:
	streamtool submitjob output/MetricsIngestService/com.ibm.streamsx.monitoring.metrics.services.MetricsIngestService.sab --jobname microserviceMetrics
	streamtool submitjob output/MetricsMonitorService/com.ibm.streamsx.monitoring.metrics.services.MetricsMonitorService.sab --jobname microserviceMonitor
	streamtool submitjob output/Main.sab --jobname testMicroservices

stop-monitor:
	streamtool canceljob --jobnames microserviceMetrics --collectlogs
	streamtool canceljob --jobnames microserviceMonitor --collectlogs
	streamtool canceljob --jobnames testMicroservices --collectlogs

