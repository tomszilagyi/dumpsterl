PROJECT = dumpsterl
PROJECT_DESCRIPTION = Dumpsterl
PROJECT_VERSION = 0.1.0

# Whitespace to be used when creating files from templates.
SP = 4

include erlang.mk

# We want warnings to be warnings, not errors.
ERLC_OPTS := $(filter-out -Werror,$(ERLC_OPTS))
