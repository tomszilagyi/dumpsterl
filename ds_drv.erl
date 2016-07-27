%% Driver functions for dataspec
-module(ds_drv).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ fold_tab/6
        , spec_tab/4
        , spec_tab/5
        ]).

-include_lib("eunit/include/eunit.hrl").

%% Go through a table to spec a field based on a limited number
%% of rows processed:
%%
%%   spec_tab(ets, my_table, #my_record.my_field, 1000)
%%
%% Sending 0 as FieldNo will process the whole record.
%% Sending eg. 'inf' as Limit disables the limit (traverse whole table).

spec_tab(Type, Tab, FieldNo, Limit) ->
    fold_tab(fun ds:add/2, ds:new(), Type, Tab, FieldNo, Limit).

spec_tab(Type, Tab, FieldNo, Limit, Opts) ->
    ds_opts:setopts(Opts),
    fold_tab(fun ds:add/2, ds:new(), Type, Tab, FieldNo, Limit).


%% tuple of accessor funs to iterate tables:
%%
%% {FirstFun, ReadFun, NextFun} where
%%
%%   FirstFun(Tab) -> Key
%%   ReadFun(Tab, Key) -> [Value]
%%   NextFun(Tab, Key) -> NextKey | '$end_of_table'
%%
accessors(ets) ->
    { fun ets:first/1
    , fun ets:lookup/2
    , fun ets:next/2
    };
accessors(mnesia) ->
    { fun mnesia:dirty_first/1
    , fun mnesia:dirty_read/2
    , fun mnesia:dirty_next/2
    }.


%% Generic backend function for folding through tables
fold_tab(Fun, Acc0, Type, Tab, FieldNo, Limit) ->
    {FirstF, _ReadF, _NextF} = Accessors = accessors(Type),
    Progress = ds_opts:getopt(progress),
    progress_init_output(Progress),
    fold_tab(Fun, Acc0, Accessors, Tab, FieldNo, Limit, Progress, 0, FirstF(Tab)).

fold_tab(_Fun, Acc, _Accessors, _Tab, _FieldNo, 0, Progress, Count, _Key) ->
    progress_final_output(Progress, Count),
    Acc;
fold_tab(_Fun, Acc, _Accessors, _Tab, _FieldNo, _Limit, Progress, Count, '$end_of_table') ->
    progress_final_output(Progress, Count),
    Acc;
fold_tab(Fun, Acc0, {_FirstF, ReadF, NextF} = Accessors,
         Tab, FieldNo, Limit, Progress0, Count, Key) ->
    Progress = progress_output(Progress0, Count+1),
    RecL = ReadF(Tab, Key),
    FoldF = fun(Rec, FoldAcc) ->
                    Field = elem(FieldNo, Rec),
                    Fun(Field, FoldAcc)
            end,
    Acc = lists:foldl(FoldF, Acc0, RecL),
    fold_tab(Fun, Acc, Accessors, Tab, FieldNo,
                   counter_dec(Limit), Progress, Count+1, NextF(Tab, Key)).

elem(0, Rec) -> Rec;
elem(F, Rec) -> element(F, Rec).

%% output progress information if configured to do so
progress_output(false, _Count) -> false;
progress_output(1, Count) ->
    Progress = ds_opts:getopt(progress),
    case Count div Progress rem 50 of
        0 -> io:format("~B", [Count]);
        _ -> io:put_chars(".")
    end,
    Progress;
progress_output(Progress, _Count) -> counter_dec(Progress).

progress_init_output(false)    -> false;
progress_init_output(Progress) -> io:format("progress (every ~B): ", [Progress]).

progress_final_output(false, _Count) -> false;
progress_final_output(_Prog, Count)  -> io:format("~nprocessed: ~B~n~n", [Count]).

%% decrement counters that might be disabled by being set to an atom
counter_dec(N) when is_integer(N) -> N-1;
counter_dec(A)                    -> A.


%% Tests

-record(rec_test, {key, value0, value1, value2}).

spec_ets_set_test() ->
    random:seed(erlang:now()),
    Tid = ets:new(ets_test, [{keypos, #rec_test.key}]),
    populate(ets, Tid, set, 100),
    ?assertMatch([{integer, {100, _, _}}],
                 spec_tab(ets, Tid, #rec_test.key, 1000)),
    ?assertMatch([{integer, {100, _, _}}],
                 spec_tab(ets, Tid, #rec_test.value0, 1000)),
    ?assertMatch([{float, {100, _, _}}],
                 spec_tab(ets, Tid, #rec_test.value1, 1000)),
    ?assertMatch([{str_alpha, {100, _}}],
                 spec_tab(ets, Tid, #rec_test.value2, 1000)),
    ?assertMatch([{{tuple, 5},
                   [[{atom, [{rec_test, 100}]}],
                    [{integer, {100, _, _}}],
                    [{integer, {100, _, _}}],
                    [{float, {100, _, _}}],
                    [{str_alpha, {100, _}}]]}],
                 spec_tab(ets, Tid, 0, 1000)).

spec_ets_dbag_test() ->
    random:seed(erlang:now()),
    Tid = ets:new(ets_test, [duplicate_bag, {keypos, #rec_test.key}]),
    populate(ets, Tid, dbag, 100),
    ?assertMatch([{integer, {200, _, _}}],
                 spec_tab(ets, Tid, #rec_test.key, 1000)),
    ?assertMatch([{integer, {200, _, _}}],
                 spec_tab(ets, Tid, #rec_test.value0, 1000)),
    ?assertMatch([{float, {200, _, _}}],
                 spec_tab(ets, Tid, #rec_test.value1, 1000)),
    ?assertMatch([{str_alpha, {200, _}}],
                 spec_tab(ets, Tid, #rec_test.value2, 1000)),
    ?assertMatch([{{tuple, 5},
                   [[{atom, [{rec_test, 200}]}],
                    [{integer, {200, _, _}}],
                    [{integer, {200, _, _}}],
                    [{float, {200, _, _}}],
                    [{str_alpha, {200, _}}]]}],
                 spec_tab(ets, Tid, 0, 1000)).


populate(ets, _Tid, _StorType, 0) -> ok;
populate(ets, Tid, StorType, Count) ->
    add_rec(ets, Tid, StorType),
    populate(ets, Tid, StorType, Count-1).

add_rec(ets, Tid, StorType) ->
    RecL = generate_recs(StorType),
    %io:format("add: ~p~n", [Rec]),
    ets:insert(Tid, RecL).

generate_recs(set) ->
    #rec_test{ key = random(int)
             , value0 = random(int)
             , value1 = random(float)
             , value2 = random(alpha)
             };
generate_recs(dbag) ->
    K = random(int),
    [ #rec_test{ key = K
               , value0 = random(int)
               , value1 = random(float)
               , value2 = random(alpha)
               }
    , #rec_test{ key = K
               , value0 = random(int)
               , value1 = random(float)
               , value2 = random(alpha)
               }
    ].

random(int) -> random:uniform(1 bsl 30) - 1;
random(float) -> random:uniform();
random(alpha) ->
    Len = random:uniform(40),
    [$a + random:uniform(25) || _N <- lists:seq(1, Len)].
