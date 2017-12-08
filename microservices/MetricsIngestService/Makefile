# Copyright (C) 2017, International Business Machines Corporation. 
# All Rights Reserved.

.PHONY: build all clean distributed

SPLC_FLAGS = -a
OUTPUT_DIR = output
TO_BE_DELETED = 
ifeq ($(STREAMS_STUDIO_BUILDING), 1)
  # Building from Streams Studio, use env vars set by studio
  SPLC = $(STREAMS_STUDIO_SC_PATH)
  SPL_PATH = $(STREAMS_STUDIO_SPL_PATH)
  OUTPUT_DIR = $(STREAMS_STUDIO_OUTPUT_DIRECTORY)
else
  # Building outside of Streams Studio...
  ifeq ($(STREAMS_INSTALL),)
    $(error error: environment variable STREAMS_INSTALL has to be set)
  endif
  # Set this variable if streamsx.monitoring sample or toolkit location changed in your environment.
  STREAMSX_MONITORING_TOOLKIT ?=../../com.ibm.streamsx.monitoring
  SPLC = $(STREAMS_INSTALL)/bin/sc
  STREAMSX_JSON_TOOLKIT ?=$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.json
  STREAMSX_TOPOLOGY_TOOLKIT ?=$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.topology
  SPL_PATH = $(STREAMSX_MONITORING_TOOLKIT):$(STREAMSX_JSON_TOOLKIT):$(STREAMSX_TOPOLOGY_TOOLKIT)
  # delete directories and files generated and required by StreamsStudio
  TO_BE_DELETED += bin impl .toolkitList .classpath
endif
TO_BE_DELETED += $(OUTPUT_DIR)
SPLC_FLAGS += -t $(SPL_PATH) --output-directory=$(OUTPUT_DIR)

SPL_CMD_ARGS ?= 
SPL_MAIN_COMPOSITE = com.ibm.streamsx.monitoring.service::MetricsIngestService


build: distributed 

all: clean build

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)

clean:
	$(SPLC) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf $(TO_BE_DELETED)

configure:
	-streamtool rmappconfig --noprompt com.ibm.streamsx.monitoring.metrics.MetricsSource.ApplicationConfiguration
	streamtool mkappconfig --property connectionURL=`streamtool getjmxconnect` --property user=$(JMX_USER) --property password=$(JMX_PASSWORD) com.ibm.streamsx.monitoring.metrics.MetricsSource.ApplicationConfiguration

configure-json:
	-streamtool rmappconfig --noprompt com.ibm.streamsx.monitoring.metrics.MetricsSource.ApplicationConfiguration
	streamtool mkappconfig --property user=$(JMX_USER) --property password=$(JMX_PASSWORD) --property filterDocument=`cat etc/MetricsSource_MonitorOperatorMetrics.json | perl -e 'my @content = <STDIN>; my $$str = join("", @content); $$str =~ s/\s//g; print $$str;'` com.ibm.streamsx.monitoring.metrics.MetricsSource.ApplicationConfiguration

configure-none:
	-streamtool rmappconfig --noprompt com.ibm.streamsx.monitoring.metrics.MetricsSource.ApplicationConfiguration

release: all
	-mkdir ../../tmp
	cp $(OUTPUT_DIR)/*.sab ../../tmp

