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

distclean:: docs-clean

# Remove synthetically generated test data
testclean:
	rm -rf test/data

docs:: guide doc/shell_ref.txt README.html

guide:
	make -C doc/guide

doc/shell_ref.txt:
	echo "Dumpsterl interactive probe shell;" > $@
	echo "start it via typing \`ds:shell().' into the Erlang shell." >> $@
	echo >> $@
	erl -pa ebin -noshell -eval 'ds_shell:help("all", state), init:stop().' >> $@

README.html: README.adoc
	asciidoctor $^

docs-clean:
	rm -f README.html
	rm -f doc/shell_ref.txt
	make -C doc/guide clean
