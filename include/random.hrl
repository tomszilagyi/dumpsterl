%% Defines for portability (pre- and post-R18)

-ifndef(_RANDOM_HRL_).
-define(_RANDOM_HRL_, true).

-include("config.hrl").

-ifdef(CONFIG_RAND_MODULE).
-define(RNDMOD, rand).
-define(RNDINIT, ok).
-define(RNDINIT_DET, rand:seed(exs1024, {123,456,789})).
-else.
-define(RNDMOD, random).
-define(RNDINIT, random:seed(erlang:now())).
-define(RNDINIT_DET, random:seed({123,456,789})).
-endif.

-endif.
