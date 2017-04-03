%% Defines for debugging

-ifndef(_DEBUG_HRL_).
-define(_DEBUG_HRL_, true).

-ifdef(DEBUG).
-define(debug(F,A), io:format(user, "[~s~4B] " F "~n", [?MODULE,?LINE]++A)).
-else.
-define(debug(F,A), true).
-endif.

-endif.
