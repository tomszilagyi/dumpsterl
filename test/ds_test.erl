%% -*- coding: utf-8 -*-
-module(ds_test).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-include_lib("eunit/include/eunit.hrl").

-include("config.hrl").

new_test() ->
    ?assertMatch({term, {_Stats, _Ext}, []}, ds_spec:new()).

-ifdef(CONFIG_MAPS).

maps_test() ->
    L = [ #{k3 => ca, k2 => ba, k1 => aa}
        , #{k4 => db, k2 => bb}
        , #{k1 => ac, k3 => cc}
        , #{k2 => be, k4 => de}
        , #{k1 => ad, k4 => dd, k2 => bd}
        ],

    Spec = lists:foldl(fun ds_spec:add/2, ds_spec:new(), [{V, []} || V <- L]),

    {term, _, [{map, {Stats, [k1,k2,k3,k4]}, [Ch1, Ch2, Ch3, Ch4]}]} = Spec,
    ?assertEqual(5, ds_stats:get_count(Stats)),

    {term, {_, []}, [{atom, {StatsK1, _}, []}]} = Ch1,
    ?assertEqual(3, ds_stats:get_count(StatsK1)),
    ?assertEqual([aa,ad,ac], [V || {V,_PV} <- ds_stats:get_samples(StatsK1)]),

    {term, {_, []}, [{atom, {StatsK2, _}, []}]} = Ch2,
    ?assertEqual(4, ds_stats:get_count(StatsK2)),
    ?assertEqual([ba,bd,be,bb], [V || {V,_PV} <- ds_stats:get_samples(StatsK2)]),

    {term, {_, []}, [{atom, {StatsK3, _}, []}]} = Ch3,
    ?assertEqual(2, ds_stats:get_count(StatsK3)),
    ?assertEqual([ca,cc], [V || {V,_PV} <- ds_stats:get_samples(StatsK3)]),

    {term, {_, []}, [{atom, {StatsK4, _}, []}]} = Ch4,
    ?assertEqual(3, ds_stats:get_count(StatsK4)),
    ?assertEqual([de,dd,db], [V || {V,_PV} <- ds_stats:get_samples(StatsK4)]).

-endif.
