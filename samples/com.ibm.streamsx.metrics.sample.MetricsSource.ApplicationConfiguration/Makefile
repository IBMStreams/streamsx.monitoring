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
  # Set this variable if streamsx.metrics sample or toolkit location changed in your environment.
  STREAMSX_METRICS_TOOLKIT ?=../../com.ibm.streamsx.metrics
  SPLC = $(STREAMS_INSTALL)/bin/sc
  SPL_PATH = $(STREAMSX_METRICS_TOOLKIT)
endif

SPLC_FLAGS += -t $(SPL_PATH) --output-directory=$(OUTPUT_DIR)

SPL_CMD_ARGS ?= 
SPL_MAIN_COMPOSITE = com.ibm.streamsx.metrics.sample.MetricsSource.ApplicationConfiguration::Main

build: distributed 

all: clean build

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf $(OUTPUT_DIR)

configure:
	# see https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.admin.doc/doc/creating-secure-app-configs.html
	-streamtool rmappconfig --noprompt com.ibm.streamsx.metrics.sample.MetricsSource.ApplicationConfiguration
	streamtool mkappconfig --property connectionURL=`streamtool getjmxconnect` --property user=$(USER) com.ibm.streamsx.metrics.sample.MetricsSource.ApplicationConfiguration

configure-json:
	# see https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.admin.doc/doc/creating-secure-app-configs.html
	-streamtool rmappconfig --noprompt com.ibm.streamsx.metrics.sample.MetricsSource.ApplicationConfiguration
	streamtool mkappconfig --property connectionURL=`streamtool getjmxconnect` --property user=$(USER) --property filterDocument=`cat etc/MetricsSource_MonitorOperatorMetrics.json | perl -e 'my @content = <STDIN>; my $$str = join("", @content); $$str =~ s/\s//g; print $$str;'` com.ibm.streamsx.metrics.sample.MetricsSource.ApplicationConfiguration

configure-none:
	# see https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.admin.doc/doc/creating-secure-app-configs.html
	-streamtool rmappconfig --noprompt com.ibm.streamsx.metrics.sample.MetricsSource.ApplicationConfiguration

configure-incomplete:
	# see https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.admin.doc/doc/creating-secure-app-configs.html
	-streamtool rmappconfig --noprompt com.ibm.streamsx.metrics.sample.MetricsSource.ApplicationConfiguration
	streamtool mkappconfig --property user=$(USER) com.ibm.streamsx.metrics.sample.MetricsSource.ApplicationConfiguration

configure-memory:
	streamtool chappconfig --property connectionURL=`streamtool getjmxconnect` --property user=$(USER) --property filterDocument=`cat etc/MetricsSource_MonitorMemoryMetrics.json | perl -e 'my @content = <STDIN>; my $$str = join("", @content); $$str =~ s/\s//g; print $$str;'` com.ibm.streamsx.metrics.sample.MetricsSource.ApplicationConfiguration

configure-original:
	streamtool chappconfig --property connectionURL=`streamtool getjmxconnect` --property user=$(USER) --property filterDocument=`cat etc/MetricsSource_MonitorOperatorMetrics.json | perl -e 'my @content = <STDIN>; my $$str = join("", @content); $$str =~ s/\s//g; print $$str;'` com.ibm.streamsx.metrics.sample.MetricsSource.ApplicationConfiguration
