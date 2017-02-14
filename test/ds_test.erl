-module(ds_test).

-include_lib("eunit/include/eunit.hrl").

new_test() ->
    ?assertMatch({'T', {_Stats, _Ext}, []}, ds:new()).
