PROJECT = dumpsterl
PROJECT_DESCRIPTION = Dumpsterl
PROJECT_VERSION = 0.5.0

# Whitespace to be used when creating files from templates.
SP = 4

DEPS =

PLT_APPS = erts kernel stdlib compiler crypto mnesia wx
DIALYZER_OPTS = -Wunmatched_returns

# We want warnings to be warnings, not errors.
ERLC_OPTS := $(filter-out -Werror,$(ERLC_OPTS))

include erlang.mk

CONFIG_HRL = include/config.hrl
$(PROJECT).d:: $(CONFIG_HRL)

include/config.hrl:: configure.sh
	$(gen_verbose) ./configure.sh $@

clean::
	rm -f $(CONFIG_HRL)

distclean::
	make -C doc/guide clean

# Remove synthetically generated test data
testclean:
	rm -rf test/data

docs:: asciidoc

asciidoc:
	make -C doc/guide
