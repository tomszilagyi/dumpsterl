-module(ds_pvattrs).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ new/1
        , add/2
        , join/2

        , get_count/1
        , get_timespan/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(pvattrs,    % Per-Value Attributes
        { count     % integer
        , timespan  % {MinTK, MaxTK} where TK is {Ts, Key}
        }).

%% TODO maybe require a normalized form of Attrs here
%% (pre-process them in ds_drv) so we can do simple pattern matching
%% instead of proplists:get_value/2 each time we want ts and key
new(#pvattrs{} = PVS) -> PVS;
new(Attrs) ->
    Ts = proplists:get_value(ts, Attrs),
    Key = proplists:get_value(key, Attrs),
    #pvattrs{count = 1, timespan = new_timespan({Ts, Key})}.

add(#pvattrs{count=Count0, timespan=TSp0},
    #pvattrs{count=Count1, timespan=TSp1}) ->
    #pvattrs{count = Count0+Count1, timespan = join_timespan(TSp0, TSp1)};
add(Attrs, #pvattrs{count=Count, timespan=TSp}) ->
    Ts = proplists:get_value(ts, Attrs),
    Key = proplists:get_value(key, Attrs),
    #pvattrs{count = Count+1, timespan = add_timespan({Ts, Key}, TSp)}.

%% Since we can discriminate Attrs from #pvattrs{}, we use add/2 for joins too.
%% This is useful in ds_sampler:join/2. Provide join/2 here as a simple alias.
join(PVS0, PVS1) -> add(PVS0, PVS1).


get_count(#pvattrs{count = Count}) -> Count.

get_timespan(#pvattrs{timespan = TSp}) -> TSp.


%% Timespan: a 2-tuple {MinTK, MaxTK} of 2-tuples TK={Ts, Key}
new_timespan(TK) -> {TK, TK}.

add_timespan(TK, {MinTK, MaxTK}) -> {min(TK, MinTK), max(TK, MaxTK)}.

join_timespan({TK0, TK1}, {TK2, TK3}) -> {min(TK0, TK2), max(TK1, TK3)}.


%% Tests
-ifdef(TEST).

timespan_test() ->
    TS0 = new_timespan({1, a}),
    ?assertEqual({{1,a}, {1,a}}, TS0),

    TS1 = add_timespan({1,a}, TS0),
    ?assertEqual({{1,a}, {1,a}}, TS1),

    TS2 = join_timespan(new_timespan({3,c}), TS1),
    ?assertEqual({{1,a}, {3,c}}, TS2).

pvattrs_test() ->
    PV0 = new([{ts, 1}, {key, a}]),
    ?assertEqual(#pvattrs{count=1, timespan={{1,a}, {1,a}}}, PV0),

    PV1 = add([{ts, 1}, {key, a}], PV0),
    ?assertEqual(#pvattrs{count=2, timespan={{1,a}, {1,a}}}, PV1),

    PV2 = add([{ts, 2}, {key, b}], PV1),
    ?assertEqual(#pvattrs{count=3, timespan={{1,a}, {2,b}}}, PV2),

    PV3 = new([{ts, 0}, {key, x}]),
    PV4 = new([{ts, 0}, {key, w}]),
    PV5 = join(PV3, PV4),
    ?assertEqual(#pvattrs{count=2, timespan={{0,w}, {0,x}}}, PV5),

    PV6 = join(PV2, PV5),
    ?assertEqual(#pvattrs{count=5, timespan={{0,w}, {2,b}}}, PV6).

-endif.
