# Copyright (C) 2017, International Business Machines Corporation. 
# All Rights Reserved.                             
.PHONY: all clean

DIRS := $(shell for x in *; do if [ -d $$x ]; then echo -n "$$x "; fi; done)

all: 
	for dir in $(shell ls -d -1 */); do\
		make -C $$dir || exit 1; \
	done

clean: clean-doc
	for dir in $(shell ls -d -1 */); do\
		make -C $$dir clean || exit 1; \
	done

make-doc:
	for x in $(DIRS); do echo -e "\n######## make-doc sample $$x ########"; cd $$x; spl-make-doc -i . --include-all; cd -; done

clean-doc:
	for x in $(DIRS); do cd $$x; rm -rf doc; cd -; done


