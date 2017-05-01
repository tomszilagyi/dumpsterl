%% -*- coding: utf-8 -*-
-module(ds_reports_SUITE).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ all/0
        , end_per_suite/1
        , end_per_testcase/2
        , init_per_suite/1
        , init_per_testcase/2
        , suite/0
        ]).

-export([ test_spec_attrs_dets_key_ts/1
        , test_spec_attrs_disk_log_key_ts/1
        , test_spec_attrs_ets_key_ts/1
        , test_spec_attrs_mnesia_key_ts/1
        , test_spec_attrs_mnesia_ts/1
        , test_spec_attrs_mnesia_key/1
        , test_spec_attrs_mnesia_/1
        ]).

-include_lib("common_test/include/ct.hrl").

suite() -> [{timetrap, {seconds, 60}}].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) -> Config.
end_per_testcase(_TestCase, _Config) ->
    os:cmd(lists:concat(["rm -f ", code:priv_dir(dumpsterl), "/*.png"])),
    ok.

all() ->
    [ test_spec_attrs_dets_key_ts
    , test_spec_attrs_disk_log_key_ts
    , test_spec_attrs_ets_key_ts
    , test_spec_attrs_mnesia_key_ts
    , test_spec_attrs_mnesia_ts
    , test_spec_attrs_mnesia_key
    , test_spec_attrs_mnesia_
    ].

test_spec_attrs_dets_key_ts(Config) ->
    test_spec_attrs(dets, [key, ts], Config).

test_spec_attrs_disk_log_key_ts(Config) ->
    test_spec_attrs(disk_log, [key, ts], Config).

test_spec_attrs_ets_key_ts(Config) ->
    test_spec_attrs(ets, [key, ts], Config).

test_spec_attrs_mnesia_key_ts(Config) ->
    test_spec_attrs(mnesia, [key, ts], Config).

test_spec_attrs_mnesia_ts(Config) ->
    test_spec_attrs(mnesia, [ts], Config).

test_spec_attrs_mnesia_key(Config) ->
    test_spec_attrs(mnesia, [key], Config).

test_spec_attrs_mnesia_(Config) ->
    test_spec_attrs(mnesia, [], Config).


test_spec_attrs(Type, Attributes, Config0) ->
    Config = [ {type, Type}
             , {num_records, 10000}
             , {attributes, Attributes}
             | Config0
             ],
    io:format(user, "Testing spec type=~p, attributes=~p\n\n", [Type, Attributes]),
    ds_testdb:generate_ct(Config),
    Spec0 = ds_testdb:spec_ct(Config),
    Spec = ds:postproc(Spec0),
    io:format(user, "generating report for spec node:", []),
    check_report(Spec),
    io:format(user, "\n\n", []),
    ok.

%% Check that the report page can be generated without crashing
%% both for the spec and all its sub-nodes.
check_report({Class,_Data, Children} = Spec) ->
    io:format(user, " ~p", [Class]),
    _Html = ds_reports:report_page(Spec, []),
    lists:foreach(fun(Ch) -> check_report(Ch) end, Children),
    ok.
