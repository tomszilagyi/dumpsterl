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
%% Sending 0 as FieldSpec will process the whole record.
%% Sending eg. 'inf' as Limit disables the limit (traverse whole table).
%%
%% Advanced usage exmaples:
%%   progress indicator and output dump (to retain partial results):
%%     ds_drv:spec_tab(mnesia, payment_rec, #payment_rec.primary_reference, inf, [{progress, 10000}, dump]).
%%
%%   chain field references to spec a sub-subfield on nested records:
%%     ds_drv:spec_tab(mnesia, kcase, [#kcase.payer_info, #payer_info.payer_bg], inf).
%%
%%   getter function for arbitrary data selection:
%%     FieldSpecF = fun(KC) -> KC#kcase.payer_info#payer_info.payer_bg end,
%%     ds_drv:spec_tab(mnesia, kcase, FieldSpecF, inf).
%%
%%   NB. using a getter fun is slower than a chained reference (list of field numbers),
%%   so use the fun only where a truly generic accessor is needed. Also, the fun might
%%   throw an exception in certain cases to exclude those from the spec.

spec_tab(Type, Tab, FieldSpec, Limit) ->
    fold_tab(fun ds:add/2, ds:new(), Type, Tab, FieldSpec, Limit).

spec_tab(Type, Tab, FieldSpec, Limit, Opts) ->
    ds_opts:setopts(Opts),
    fold_tab(fun ds:add/2, ds:new(), Type, Tab, FieldSpec, Limit).


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
fold_tab(Fun, Acc0, Type, Tab, FieldSpec, Limit) ->
    {FirstF, _ReadF, _NextF} = Accessors = accessors(Type),
    Progress = ds_opts:getopt(progress),
    progress_init_output(Progress),
    fold_tab(Fun, Acc0, Accessors, Tab, FieldSpec, Limit, Progress, 0, FirstF(Tab)).

fold_tab(_Fun, Acc, _Accessors, _Tab, _FieldSpec, 0, Progress, Count, _Key) ->
    progress_final_output(Progress, Count),
    dump_acc(Acc),
    Acc;
fold_tab(_Fun, Acc, _Accessors, _Tab, _FieldSpec, _Limit, Progress, Count, '$end_of_table') ->
    progress_final_output(Progress, Count),
    dump_acc(Acc),
    Acc;
fold_tab(Fun, Acc0, {_FirstF, ReadF, NextF} = Accessors,
         Tab, FieldSpec, Limit, Progress0, Count, Key) ->
    Progress = progress_output(Progress0, Acc0, Count+1),
    RecL = ReadF(Tab, Key),
    FoldF = fun(Rec, FoldAcc) ->
                    %% The element accessor might crash or throw an error
                    %% to exclude this instance from the spec.
                    try Field = elem(FieldSpec, Rec),
                        Fun(Field, FoldAcc)
                    catch _:_ -> FoldAcc
                    end
            end,
    Acc = lists:foldl(FoldF, Acc0, RecL),
    fold_tab(Fun, Acc, Accessors, Tab, FieldSpec,
             counter_dec(Limit), Progress, Count+1, NextF(Tab, Key)).

elem(0, Rec) -> Rec;
elem(F, Rec) when is_integer(F) -> element(F, Rec);
elem(F, Rec) when is_function(F, 1) -> F(Rec);
elem([], Rec) -> Rec;
elem([F|Rest], Rec) -> elem(Rest, elem(F, Rec)).

%% output progress information if configured to do so
progress_output(false, _Acc, _Count) -> false;
progress_output(1, Acc, Count) ->
    Progress = ds_opts:getopt(progress),
    case Count div Progress rem 50 of
        0 -> io:format("~B", [Count]);
        _ -> io:put_chars(".")
    end,
    dump_acc(Acc),
    Progress;
progress_output(Progress, _Acc, _Count) -> counter_dec(Progress).

progress_init_output(false)    -> false;
progress_init_output(Progress) -> io:format("progress (every ~B): ", [Progress]).

progress_final_output(false, _Count) -> false;
progress_final_output(_Prog, Count)  -> io:format("~nprocessed: ~B~n~n", [Count]).

%% dump the accumulated spec if dataspec is configured to do dumps
dump_acc(Acc) ->
    case ds_opts:getopt(dump) of
        false    -> ok;
        Filename -> ok = file:write_file(Filename, erlang:term_to_binary(Acc))
    end.

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
