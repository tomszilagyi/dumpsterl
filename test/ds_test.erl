%% -*- coding: utf-8 -*-
-module(ds_test).

-include_lib("eunit/include/eunit.hrl").

-include("config.hrl").

new_test() ->
    ?assertMatch({'T', {_Stats, _Ext}, []}, ds:new()).

-ifdef(CONFIG_MAPS).

maps_test() ->
    L = [ #{k3 => ca, k2 => ba, k1 => aa}
        , #{k4 => db, k2 => bb}
        , #{k1 => ac, k3 => cc}
        , #{k2 => be, k4 => de}
        , #{k1 => ad, k4 => dd, k2 => bd}
        ],

    Spec = lists:foldl(fun ds:add/2, ds:new(), [{V, []} || V <- L]),

    {'T', _, [{map, {Stats, [k1,k2,k3,k4]}, [Ch1, Ch2, Ch3, Ch4]}]} = Spec,
    ?assertEqual(5, ds_stats:get_count(Stats)),

    {'T', {_, []}, [{atom, {StatsK1, _}, []}]} = Ch1,
    ?assertEqual(3, ds_stats:get_count(StatsK1)),
    ?assertEqual([aa,ac,ad], [V || {V,_PV} <- ds_stats:get_samples(StatsK1)]),

    {'T', {_, []}, [{atom, {StatsK2, _}, []}]} = Ch2,
    ?assertEqual(4, ds_stats:get_count(StatsK2)),
    ?assertEqual([ba,bb,bd,be], [V || {V,_PV} <- ds_stats:get_samples(StatsK2)]),

    {'T', {_, []}, [{atom, {StatsK3, _}, []}]} = Ch3,
    ?assertEqual(2, ds_stats:get_count(StatsK3)),
    ?assertEqual([ca,cc], [V || {V,_PV} <- ds_stats:get_samples(StatsK3)]),

    {'T', {_, []}, [{atom, {StatsK4, _}, []}]} = Ch4,
    ?assertEqual(3, ds_stats:get_count(StatsK4)),
    ?assertEqual([db,dd,de], [V || {V,_PV} <- ds_stats:get_samples(StatsK4)]).

-endif.
