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
    test_spec(spec_f(ets, Tid), 100).

spec_ets_dbag_test() ->
    ?RNDINIT,
    Tid = ets:new(ets_test, [duplicate_bag, {keypos, #rec_test.key}]),
    populate(ets, Tid, dbag, 100),
    test_spec(spec_f(ets, Tid), 200).

spec_dets_set_test() ->
    ?RNDINIT,
    Tid = ets:new(ets_test, [{keypos, #rec_test.key}]),
    populate(ets, Tid, set, 100),
    DetsFile = "test_table.dets",
    file:delete(DetsFile),
    {ok, Dets} = dets:open_file(dets_test_table, [ {file, DetsFile}
                                                 , {keypos, #rec_test.key}
                                                 , {type, set}
                                                 ]),
    ok = dets:from_ets(Dets, Tid),
    test_spec(spec_f(dets, Dets), 100),
    ok = dets:close(Dets),
    ok = file:delete(DetsFile).

spec_dets_by_filename_set_test() ->
    ?RNDINIT,
    Tid = ets:new(ets_test, [{keypos, #rec_test.key}]),
    populate(ets, Tid, set, 100),
    DetsFile = "test_table.dets",
    file:delete(DetsFile),
    {ok, Dets} = dets:open_file(dets_test_table, [ {file, DetsFile}
                                                 , {keypos, #rec_test.key}
                                                 , {type, set}
                                                 ]),
    ok = dets:from_ets(Dets, Tid),
    ok = dets:close(Dets),
    test_spec(spec_f(dets, DetsFile), 100),
    ok = file:delete(DetsFile).

spec_disk_log_set_test() ->
    ?RNDINIT,
    LogFile = "test_disk_log.bin",
    file:delete(LogFile),
    {ok, Log} = disk_log:open([ {name, test_disk_log}
                              , {file, LogFile}
                              , {mode, read_write}
                              ]),
    populate(disk_log, Log, set, 100),
    ok = disk_log:close(Log),
    test_spec(spec_f(disk_log, LogFile), 100),
    ok = file:delete(LogFile).

spec_f(Type, Tid) ->
    fun(FieldSpec, Limit) -> ?M:spec(Type, Tid, FieldSpec, Limit) end.

test_spec(TestFun, NRecs) ->
    Spec0 = TestFun(#rec_test.key, 1000),
    {'T', _,
     [{number, _,
       [{integer, _,
        [{non_neg_integer, _,
         [{pos_integer, {Stats0, _Ext0}, _}]}]}]}]} = Spec0,
    ?assertEqual(NRecs, ds_stats:get_count(Stats0)),

    Spec1 = TestFun(#rec_test.value1, 1000),
    {'T', _,
     [{number, _,
       [{integer, _,
        [{non_neg_integer, _,
         [{pos_integer, {Stats1, _Ext1}, _}]}]}]}]} = Spec1,
    ?assertEqual(NRecs, ds_stats:get_count(Stats1)),

    Spec2 = TestFun(#rec_test.value2, 1000),
    {'T', _,
     [{number, _,
       [{float, {Stats2, _Ext2}, _}]}]} = Spec2,
    ?assertEqual(NRecs, ds_stats:get_count(Stats2)),

    Spec3 = TestFun(#rec_test.value3, 1000),
    {'T', _,
     [{list, _,
       [{nonempty_list, {Stats3, _Ext3},
         [{'T', _, [{number, _,
                     [{integer, _,
                       [{non_neg_integer, _,
                        [{pos_integer, _,
                          [{char, _, _}]}]}]}]}]}]}]}]} = Spec3,
    ?assertEqual(NRecs, ds_stats:get_count(Stats3)),

    SpecT = TestFun(0, 1000),
    {'T', _,
     [{tuple, _,
       [{{record, {rec_test, 5}}, {StatsT, _ExtT}, SpecFields}]}]} = SpecT,

    ?assert(ds:eq(Spec0, lists:nth(1, SpecFields))),
    ?assert(ds:eq(Spec1, lists:nth(2, SpecFields))),
    ?assert(ds:eq(Spec2, lists:nth(3, SpecFields))),
    ?assert(ds:eq(Spec3, lists:nth(4, SpecFields))),
    ?assertEqual(NRecs, ds_stats:get_count(StatsT)).

%% Test utility functions

populate(_Type, _Tid, _StorType, 0) -> ok;
populate(Type, Tid, StorType, Count) ->
    add_rec(Type, Tid, StorType),
    populate(Type, Tid, StorType, Count-1).

add_rec(ets, Tid, StorType) ->
    ets:insert(Tid, generate_recs(StorType));
add_rec(disk_log, Log, StorType) ->
    [ok = disk_log:log(Log, Rec) || Rec <- generate_recs(StorType)], ok.

generate_recs(set) ->
    [ #rec_test{ key = random(int)
               , value1 = random(int)
               , value2 = random(float)
               , value3 = random(alpha)
               }
    ];
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
