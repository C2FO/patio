PREFIX ?= /usr/local
JSCOV = support/jscoverage/node-jscoverage
JS_FILES = $(shell find ./lib | grep index.js && find lib | awk '!/index.js/ && /.js/' )
BENCHMARKS = `find benchmark -name *.benchmark.js `
DOC_COMMAND=java -jar ./support/jsdoc/jsrun.jar ./support/jsdoc/app/run.js -t=./support/jsdoc/templates/jsdoc -d=./docs

test:
	node test/runner.js

test-coverage:
	node test/runner.js coverage
	rm -rf lib-cov

docs: docclean
	$(DOC_COMMAND) $(JS_FILES)

docclean :
	rm -rf docs

install: install-jscov

install-jscov: $(JSCOV)
	install $(JSCOV) $(PREFIX)/bin

$(JSCOV):
	cd support/jscoverage && ./configure && make && mv jscoverage node-jscoverage

uninstall:
	rm -f $(PREFIX)/bin/node-jscoverage

.PHONY: install install-jscov test docs docclean uninstall



