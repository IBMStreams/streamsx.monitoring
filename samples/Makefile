# Licensed Materials - Property of IBM
# Monitoring Toolkit for IBM Streams
# (c) Copyright IBM Corp. 2019
# All rights reserved.                               

DIRS := $(shell for x in *; do if [ -d $$x ]; then echo -n "$$x "; fi; done)
$(info Samples to build: $(DIRS))
$(info )

.PHONY: all clean 

all:
	for x in $(DIRS); do echo -e "\n######## make sample $$x ########"; cd $$x; make all; cd -; done

clean: 
	for x in $(DIRS); do cd $$x; make clean; cd -; done

