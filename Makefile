PROJECT = dumpsterl
PROJECT_DESCRIPTION = Dumpsterl
PROJECT_VERSION = 0.5.0

# Whitespace to be used when creating files from templates.
SP = 4

DEPS = asciideck

PLT_APPS = erts kernel stdlib compiler crypto mnesia wx
DIALYZER_OPTS = -Wunmatched_returns

include erlang.mk

CONFIG_HRL = include/config.hrl
$(PROJECT).d:: $(CONFIG_HRL)

include/config.hrl:: configure.sh
	$(gen_verbose) ./configure.sh $@

clean::
	rm -f $(CONFIG_HRL)

# We want warnings to be warnings, not errors.
ERLC_OPTS := $(filter-out -Werror,$(ERLC_OPTS))
