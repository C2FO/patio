PREFIX ?= /usr/local
PWD = `pwd`
JSCOV = support/jscoverage/node-jscoverage
JS_FILES = $(shell find ./lib | grep index.js && find lib | awk '!/index.js/ && /.js/' )
BENCHMARKS = `find benchmark -name *.benchmark.js `
DOC_COMMAND=java -jar ./support/jsdoc/jsrun.jar ./support/jsdoc/app/run.js -t=./support/jsdoc/templates/CoolTemplate -d=./docs/api  -D="github:pollenware/patio"

test:
	export NODE_PATH=$NODE_PATH:lib && node test/runner.js

test-coverage:
	export NODE_PATH=$NODE_PATH:lib-cov && node test/runner.js true $(SHOW_SOURCE)

docs: docclean
	$(DOC_COMMAND) $(JS_FILES)

docclean :
	rm -rf docs/api

benchmarks:
	for file in $(BENCHMARKS) ; do \
		node $$file ; \
	done

install: install-jscov

install-jscov: $(JSCOV)
	install $(JSCOV) $(PREFIX)/bin

$(JSCOV):
	cd support/jscoverage && ./configure && make && mv jscoverage node-jscoverage

uninstall:
	rm -f $(PREFIX)/bin/node-jscoverage

.PHONY: install install-jscov test docs docclean uninstall



