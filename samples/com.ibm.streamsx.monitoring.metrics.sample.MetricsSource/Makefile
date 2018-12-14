# Copyright (C) 2016, International Business Machines Corporation. 
# All Rights Reserved.

.PHONY: build all clean distributed

SPLC_FLAGS = -a
OUTPUT_DIR = output
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
  SPL_PATH = $(STREAMSX_MONITORING_TOOLKIT)
endif

SPLC_FLAGS += -t $(SPL_PATH) --output-directory=$(OUTPUT_DIR)

SPL_CMD_ARGS ?= 
SPL_MAIN_COMPOSITE = com.ibm.streamsx.monitoring.metrics.sample.MetricsSource::Monitor
SPL_MAIN_COMPOSITE1 = com.ibm.streamsx.monitoring.metrics.sample.MetricsSource::SampleJob


build: distributed 

all: clean build

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)/monitor
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE1) $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)/sample

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf $(OUTPUT_DIR)

configure:
	-streamtool rmappconfig --noprompt monitoring
	streamtool mkappconfig --property connectionURL=`streamtool getjmxconnect` --property user=$(STREAMS_USER) --property password=$(STREAMS_PASSWORD) monitoring

configure-json:
	-streamtool rmappconfig --noprompt monitoring
	streamtool mkappconfig --property user=$(STREAMS_USER) --property password=$(STREAMS_PASSWORD) --property filterDocument=`cat etc/MetricsSource_MonitorOperatorMetrics.json | perl -e 'my @content = <STDIN>; my $$str = join("", @content); $$str =~ s/\s//g; print $$str;'` monitoring

configure-none:
	-streamtool rmappconfig --noprompt monitoring

configure-incomplete:
	-streamtool rmappconfig --noprompt monitoring
	streamtool mkappconfig --property user=$(STREAMS_USER) monitoring

configure-memory:
	streamtool chappconfig --property user=$(STREAMS_USER) --property password=$(STREAMS_PASSWORD) --property filterDocument=`cat etc/MetricsSource_MonitorMemoryMetrics.json | perl -e 'my @content = <STDIN>; my $$str = join("", @content); $$str =~ s/\s//g; print $$str;'` monitoring

configure-original:
	streamtool chappconfig --property user=$(STREAMS_USER) --property password=$(STREAMS_PASSWORD) --property filterDocument=`cat etc/MetricsSource_MonitorOperatorMetrics.json | perl -e 'my @content = <STDIN>; my $$str = join("", @content); $$str =~ s/\s//g; print $$str;'` monitoring
