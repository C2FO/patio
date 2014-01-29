PREFIX ?= /usr/local
PWD = `pwd`
JSCOV = support/jscoverage/node-jscoverage
BENCHMARKS = `find benchmark -name *benchmark.js `
DOC_COMMAND=coddoc -f multi-html -d ./lib --dir ./docs

test:
	export NODE_PATH=lib:$(NODE_PATH) && export NODE_ENV=test \
	&& export PATIO_DB=mysql && ./node_modules/it/bin/it -r dotmatrix \
	&& export PATIO_DB=pg && ./node_modules/it/bin/it -r dotmatrix

test-pg:
	export NODE_PATH=lib:$(NODE_PATH) && export NODE_ENV=test \
	&& export PATIO_DB=pg && ./node_modules/it/bin/it -r dotmatrix

test-travis:
	export NODE_PATH=lib:$(NODE_PATH) && export NODE_ENV=test \
	&& export PATIO_DB=mysql && ./node_modules/it/bin/it -r tap \
	&& export PATIO_DB=pg && ./node_modules/it/bin/it -r tap
test-coverage:
	rm -rf ./lib-cov && node-jscoverage ./lib ./lib-cov && export NODE_PATH=lib-cov:$(NODE_PATH) && export NODE_ENV=test-coverage \
	&& export PATIO_DB=mysql && ./node_modules/it/bin/it -r dotmatrix --cov-html ./docs-md/coverage.html \
    && export PATIO_DB=pg && ./node_modules/it/bin/it -r dotmatrix --cov-html ./docs-md/coverage.html

docs: docclean
	$(DOC_COMMAND)

docclean :
	rm -rf docs/*

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



