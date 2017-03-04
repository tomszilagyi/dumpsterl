%% Driver functions for dumpsterl
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
    ds_records:init(),
    {ok, dlog} = disk_log:open([ {name, dlog}
                               , {file, Filename}
                               , {mode, read_only}
                               , {repair, false}
                               ]),
    Progress = ds_progress:init(),
    Spec = normalize_spec(Spec0),
    FoldFun = fold_kernel(Fun, Spec),
    fold_disk_log_loop(FoldFun, Acc0, Limit, Progress, start).

fold_disk_log_loop(_FoldFun, Acc, 0, Progress, _Cont) ->
    ok = disk_log:close(dlog),
    ds_progress:final(Progress, Acc);
fold_disk_log_loop(FoldFun, Acc0, Limit, Progress0, Cont0) ->
    case disk_log:chunk(dlog, Cont0) of
        eof -> %% trigger end clause by setting Limit to 0
            fold_disk_log_loop(FoldFun, Acc0, 0, Progress0, undefined);
        {error, Reason} ->
            io:format("disk_log: chunk failed: ~p~n", [Reason]),
            %% trigger end clause by setting Limit to 0
            fold_disk_log_loop(FoldFun, Acc0, 0, Progress0, undefined);
        {Cont, RecL} ->
            do_fold_disk_log(FoldFun, Acc0, Limit, Progress0, Cont, RecL);
        {Cont, RecL, BadBytes} ->
            io:format("disk_log: skipped ~B bad bytes~n", [BadBytes]),
            do_fold_disk_log(FoldFun, Acc0, Limit, Progress0, Cont, RecL)
    end.

do_fold_disk_log(FoldFun, Acc0, Limit, Progress0, Cont, RecL) ->
    case {ds_progress:get_count(Progress0), RecL} of
        %% ignore log_header entry at the start
        {0, [{log_header, dcd_log, _, _, _, _}|RestRecL]} ->
            do_fold_disk_log(FoldFun, Acc0, Limit, Progress0, Cont, RestRecL);
        _ ->
            RecN = length(RecL),
            Progress = ds_progress:update(Progress0, Acc0, RecN),
            Acc = lists:foldl(FoldFun, Acc0, RecL),
            fold_disk_log_loop(FoldFun, Acc, counter_dec(Limit, RecN), Progress, Cont)
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
    ds_records:init(),
    {FirstF, _ReadF, _NextF} = Accessors = accessors(Type),
    Progress = ds_progress:init(),
    Spec = normalize_spec(Spec0),
    FoldFun = fold_kernel(Fun, Spec),
    fold_tab(FoldFun, Acc0, Accessors, Tab, Limit, Progress, FirstF(Tab)).

fold_tab(_FoldFun, Acc, _Accessors, _Tab, 0, Progress, _Key) ->
    ds_progress:final(Progress, Acc);
fold_tab(_FoldFun, Acc, _Accessors, _Tab, _Limit, Progress, '$end_of_table') ->
    ds_progress:final(Progress, Acc);
fold_tab(FoldFun, Acc0, {_FirstF, ReadF, NextF} = Accessors,
         Tab, Limit, Progress0, Key) ->
    RecL = ReadF(Tab, Key),
    RecN = length(RecL),
    Progress = ds_progress:update(Progress0, Acc0, RecN),
    Acc = lists:foldl(FoldFun, Acc0, RecL),
    fold_tab(FoldFun, Acc, Accessors, Tab,
             counter_dec(Limit, RecN), Progress, NextF(Tab, Key)).

%% The kernel for folding a list of records into the accumulated spec
fold_kernel(Fun, {FieldSpec, AttrSpecs}) ->
    fun(Rec, FoldAcc) ->
        %% The element accessor might crash or throw an error
        %% which will exclude this instance from the spec.
        try Field = elem(FieldSpec, Rec),
             Attrs = attrs(AttrSpecs, Rec),
             Fun({Field, Attrs}, FoldAcc)
        catch _:_ -> FoldAcc
        end
    end.

%% If an attribute cannot be obtained via elem/2, just ignore it.
attrs(AttrSpecs, Rec) ->
    lists:foldl(fun({Attr, Spec}, Acc) ->
                    try [{Attr, elem(Spec, Rec)}|Acc]
                    catch _:_ -> Acc
                    end
                end, [], AttrSpecs).

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
-ifdef(TEST).

%% This test demonstrates various supported styles of field access
elem_test() ->
    SubRec = {subrec, a, b, c, d},
    Rec = {rec, field1, SubRec, field3},

    %% spec returning the whole record
    ?assertEqual(Rec, elem(0, Rec)),
    ?assertEqual(Rec, elem([], Rec)),

    %% simple integer index spec (useful with #record.field notation)
    ?assertEqual(field1, elem(2, Rec)),

    %% spec with accessor fun
    FieldSpecF = fun({rec,_F1, F2,_F3}) -> F2 end,
    ?assertEqual(SubRec, elem(FieldSpecF, Rec)),

    %% chained spec
    ?assertEqual(d, elem([3, 5], Rec)),

    %% chain may also contain accessor funs
    ?assertEqual(a, elem([FieldSpecF, 2], Rec)),

    %% If the accessor fails, we get different errors
    ?assertError(badarg, elem(5, Rec)),
    ?assertError(function_clause, elem(FieldSpecF, {smallrec, 1})).


attr_test() ->
    Rec1 = {rec, {35, 20170309}, data, etc},
    Rec2 = {rec, undefined, more_data, etc},

    KeySpec = {key, [2, 1]},
    TsSpec  = {ts, [2, 2]},
    AuxSpec = {aux, 3},
    AttrSpecs = [AuxSpec, KeySpec, TsSpec],
    ?assertEqual([ {ts, 20170309}
                 , {key, 35}
                 , {aux, data}], attrs(AttrSpecs, Rec1)),

    %% ignore missing attributes:
    ?assertEqual([{aux, more_data}], attrs(AttrSpecs, Rec2)).

-endif.
