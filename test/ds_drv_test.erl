-module(ds_drv_test).

-define(M, ds_drv).

-include_lib("eunit/include/eunit.hrl").

-include("config.hrl").
-include("random.hrl").

-record(rec_test, {key, value1, value2, value3}).

spec_ets_set_test() ->
    ?RNDINIT,
    Tid = ets:new(ets_test, [{keypos, #rec_test.key}]),
    populate(ets, Tid, set, 100),
    spec_test_util(Tid, 100).

spec_ets_dbag_test() ->
    ?RNDINIT,
    Tid = ets:new(ets_test, [duplicate_bag, {keypos, #rec_test.key}]),
    populate(ets, Tid, dbag, 100),
    spec_test_util(Tid, 200).

spec_test_util(Tid, N) ->
    Spec0 = ?M:spec_tab(ets, Tid, #rec_test.key, 1000),
    {'T', _,
     [{number, _,
       [{integer, _,
        [{non_neg_integer, _,
         [{pos_integer, {Stats0, _Ext0}, _}]}]}]}]} = Spec0,
    ?assertEqual(N, ds_stats:get_count(Stats0)),

    Spec1 = ?M:spec_tab(ets, Tid, #rec_test.value1, 1000),
    {'T', _,
     [{number, _,
       [{integer, _,
        [{non_neg_integer, _,
         [{pos_integer, {Stats1, _Ext1}, _}]}]}]}]} = Spec1,
    ?assertEqual(N, ds_stats:get_count(Stats1)),

    Spec2 = ?M:spec_tab(ets, Tid, #rec_test.value2, 1000),
    {'T', _,
     [{number, _,
       [{float, {Stats2, _Ext2}, _}]}]} = Spec2,
    ?assertEqual(N, ds_stats:get_count(Stats2)),

    Spec3 = ?M:spec_tab(ets, Tid, #rec_test.value3, 1000),
    {'T', _,
     [{list, _,
       [{nonempty_list, {Stats3, _Ext3},
         [{'T', _, [{number, _,
                     [{integer, _,
                       [{non_neg_integer, _,
                        [{pos_integer, _,
                          [{char, _, _}]}]}]}]}]}]}]}]} = Spec3,
    ?assertEqual(N, ds_stats:get_count(Stats3)),

    SpecT = ?M:spec_tab(ets, Tid, 0, 1000),
    {'T', _,
     [{tuple, _,
       [{{record, {rec_test, 5}}, {StatsT, _ExtT}, SpecFields}]}]} = SpecT,
    ?assertEqual([Spec0, Spec1, Spec2, Spec3], SpecFields),
    ?assertEqual(N, ds_stats:get_count(StatsT)).

%% Test utility functions

populate(ets, _Tid, _StorType, 0) -> ok;
populate(ets, Tid, StorType, Count) ->
    add_rec(ets, Tid, StorType),
    populate(ets, Tid, StorType, Count-1).

add_rec(ets, Tid, StorType) ->
    ets:insert(Tid, generate_recs(StorType)).

generate_recs(set) ->
    #rec_test{ key = random(int)
             , value1 = random(int)
             , value2 = random(float)
             , value3 = random(alpha)
             };
generate_recs(dbag) ->
    K = random(int),
    [ #rec_test{ key = K
               , value1 = random(int)
               , value2 = random(float)
               , value3 = random(alpha)
               }
    , #rec_test{ key = K
               , value1 = random(int)
               , value2 = random(float)
               , value3 = random(alpha)
               }
    ].

random(int) -> ?RNDMOD:uniform(1 bsl 30) + 16#10ffff;
random(float) -> ?RNDMOD:uniform();
random(alpha) ->
    Len = ?RNDMOD:uniform(40),
    [$a + ?RNDMOD:uniform(25) || _N <- lists:seq(1, Len)].
