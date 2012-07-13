PREFIX ?= /usr/local
PWD = `pwd`
JSCOV = support/jscoverage/node-jscoverage
BENCHMARKS = `find benchmark -name *benchmark.js `
DOC_COMMAND=coddoc -f multi-html -d /Users/doug/git/patio/lib --dir /Users/doug/git/patio/docs

test:
	export NODE_PATH=$NODE_PATH:lib && ./node_modules/it/bin/it

test-coverage:
	rm -rf ./lib-cov && node-jscoverage ./lib ./lib-cov && export NODE_PATH=$NODE_PATH:lib-cov && ./node_modules/it/bin/it

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



