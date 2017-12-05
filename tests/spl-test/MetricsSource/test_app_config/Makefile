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
SPL_MAIN_COMPOSITE = Main
START_MON_ARGS ?= 

build: distributed 

all: clean build

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) --data-directory $(SPL_DATA_DIR) --output-directory=$(OUTPUT_DIR)

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf $(OUTPUT_DIR)

configure:
	-streamtool rmappconfig --noprompt com.ibm.streamsx.monitoring.MetricsSource.ApplicationConfiguration
	streamtool mkappconfig --property user=$(JMX_USER) --property password=$(JMX_PASSWORD) com.ibm.streamsx.monitoring.MetricsSource.ApplicationConfiguration

configure-json:
	-streamtool rmappconfig --noprompt com.ibm.streamsx.monitoring.MetricsSource.ApplicationConfiguration
	streamtool mkappconfig --property user=$(JMX_USER) --property password=$(JMX_PASSWORD) --property filterDocument=`cat etc/MetricsSource_MonitorOperatorMetrics.json | perl -e 'my @content = <STDIN>; my $$str = join("", @content); $$str =~ s/\s//g; print $$str;'` com.ibm.streamsx.monitoring.MetricsSource.ApplicationConfiguration

start-monitor:
	streamtool submitjob output/Main.sab --jobname metricsAppConfig $(START_MON_ARGS)

stop-monitor:
	streamtool canceljob --jobnames metricsAppConfig --collectlogs

