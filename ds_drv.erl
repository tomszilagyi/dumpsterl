%% Driver functions for dataspec
-module(ds_drv).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ %% ets, dets, mnesia
          spec_tab/4
        , spec_tab/5
        , fold_tab/6

          %% disk_log
        , spec_disk_log/3
        , spec_disk_log/4
        , fold_disk_log/5
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
%%   data attributes:
%%     ds_drv:spec_tab(mnesia, kcase, {#kcase.ocr, [{ts, #kcase.create_date}, {key, #kcase.cid}]},
%%                     inf, [{progress, 10000}, dump]).
%%
%%   NB. using a getter fun is slower than a chained reference (list of field numbers),
%%   so use the fun only where a truly generic accessor is needed. Also, the fun might
%%   throw an exception in certain cases to exclude those from the spec.

spec_tab(Type, Tab, FieldSpec, Limit) ->
    fold_tab(fun ds:add/2, ds:new(), Type, Tab, FieldSpec, Limit).

spec_tab(Type, Tab, FieldSpec, Limit, Opts) ->
    ds_opts:setopts(Opts),
    fold_tab(fun ds:add/2, ds:new(), Type, Tab, FieldSpec, Limit).


%% Spec a disk_log; works similar to spec_tab.

spec_disk_log(Filename, FieldSpec, Limit) ->
    fold_disk_log(fun ds:add/2, ds:new(), Filename, FieldSpec, Limit).

spec_disk_log(Filename, FieldSpec, Limit, Opts) ->
    ds_opts:setopts(Opts),
    fold_disk_log(fun ds:add/2, ds:new(), Filename, FieldSpec, Limit).


%% Backend function for folding through disk_log files
%% Useful for Mnesia .DCD and .DCL
fold_disk_log(Fun, Acc0, Filename, Spec0, Limit) ->
    {ok, dlog} = disk_log:open([ {name, dlog}
                               , {file, Filename}
                               , {mode, read_only}
                               , {repair, false}
                               ]),
    Progress = ds_progress:init(),
    Spec = normalize_spec(Spec0),
    fold_disk_log(Fun, Acc0, Spec, Limit, Progress, start).

fold_disk_log(_Fun, Acc, _Spec, 0, Progress, _Cont) ->
    ok = disk_log:close(dlog),
    ds_progress:final(Progress, Acc);
fold_disk_log(Fun, Acc0, Spec, Limit, Progress0, Cont0) ->
    case disk_log:chunk(dlog, Cont0) of
        {Cont, RecL} ->
            RecN = length(RecL),
            Progress = ds_progress:update(Progress0, Acc0, RecN),
            Acc = fold_kernel(Fun, Acc0, RecL, Spec),
            fold_disk_log(Fun, Acc, Spec, counter_dec(Limit, RecN), Progress, Cont);
        {Cont, RecL, BadBytes} ->
            io:format("disk_log: skipped ~B bad bytes~n", [BadBytes]),
            RecN = length(RecL),
            Progress = ds_progress:update(Progress0, Acc0, RecN),
            Acc = fold_kernel(Fun, Acc0, RecL, Spec),
            fold_disk_log(Fun, Acc, Spec, counter_dec(Limit, RecN), Progress, Cont);
        eof -> %% trigger end clause by setting Limit to 0
            fold_disk_log(Fun, Acc0, Spec, 0, Progress0, undefined)
    end.


%% tuple of accessor funs to iterate tables:
%%
%% {FirstFun, ReadFun, NextFun} where
%%
%%   FirstFun(Tab) -> Key
%%   ReadFun(Tab, Key) -> [Value]  (may return zero or multiple records!)
%%   NextFun(Tab, Key) -> NextKey | '$end_of_table'
%%
accessors(dets) ->
    { fun dets:first/1
    , fun dets:lookup/2
    , fun dets:next/2
    };
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

%% Generic backend function for folding through tables via accessors
fold_tab(Fun, Acc0, Type, Tab, Spec0, Limit) ->
    {FirstF, _ReadF, _NextF} = Accessors = accessors(Type),
    Progress = ds_progress:init(),
    Spec = normalize_spec(Spec0),
    fold_tab(Fun, Acc0, Accessors, Tab, Spec, Limit, Progress, FirstF(Tab)).

fold_tab(_Fun, Acc, _Accessors, _Tab, _Spec, 0, Progress, _Key) ->
    ds_progress:final(Progress, Acc);
fold_tab(_Fun, Acc, _Accessors, _Tab, _Spec, _Limit, Progress, '$end_of_table') ->
    ds_progress:final(Progress, Acc);
fold_tab(Fun, Acc0, {_FirstF, ReadF, NextF} = Accessors,
         Tab, Spec, Limit, Progress0, Key) ->
    RecL = ReadF(Tab, Key),
    RecN = length(RecL),
    Progress = ds_progress:update(Progress0, Acc0, RecN),
    Acc = fold_kernel(Fun, Acc0, RecL, Spec),
    fold_tab(Fun, Acc, Accessors, Tab, Spec,
             counter_dec(Limit, RecN), Progress, NextF(Tab, Key)).

%% The kernel for folding a list of records into the accumulated spec
fold_kernel(Fun, Acc0, RecL, {FieldSpec, AttrSpecs}) ->
    FoldF =
        fun(Rec, FoldAcc) ->
            %% The element accessor might crash or throw an error
            %% to exclude this instance from the spec.
            try Field = elem(FieldSpec, Rec),
                 Attrs = [{Attr, elem(AttrSpec, Rec)} || {Attr, AttrSpec} <- AttrSpecs],
                 Fun({Field, Attrs}, FoldAcc)
            catch _:_ -> FoldAcc
            end
        end,
    lists:foldl(FoldF, Acc0, RecL).

elem(0, Rec) -> Rec;
elem(F, Rec) when is_integer(F) -> element(F, Rec);
elem(F, Rec) when is_function(F, 1) -> F(Rec);
elem([], Rec) -> Rec;
elem([F|Rest], Rec) -> elem(Rest, elem(F, Rec)).

%% take care of Spec without attributes
normalize_spec({_FieldSpec,_AttrSpecs} = Spec) -> Spec;
normalize_spec(Spec) when is_integer(Spec) orelse
                          is_function(Spec) orelse
                          is_list(Spec) -> {Spec, []};
normalize_spec(Spec) -> throw({error, {invalid_spec, Spec}}).

%% decrement counters that might be disabled by being set to an atom
counter_dec(N, C) when is_integer(N) -> max(0, N-C);
counter_dec(A,_C)                    -> A.

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
