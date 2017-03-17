%% Defines for portability (pre- and post-R18)

-ifndef(_RANDOM_HRL_).
-define(_RANDOM_HRL_, true).

-include("config.hrl").

-ifdef(CONFIG_RAND_MODULE).
-define(RNDMOD, rand).
-define(RNDINIT, ok).
-else.
-define(RNDMOD, random).
-define(RNDINIT, random:seed(erlang:now())).
-endif.

-endif.
