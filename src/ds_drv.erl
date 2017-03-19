%% Driver functions for dumpsterl
-module(ds_drv).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ %% ets, dets, mnesia
          spec_table/4
        , spec_table/5

          %% disk_log
        , spec_disk_log/3
        , spec_disk_log/4
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Go through a table to spec a field based on a limited number
%% of rows processed:
%%
%%   spec_table(ets, my_table, #my_record.my_field, 1000)
%%
%% Sending 0 as FieldSpec will process the whole record.
%% Sending eg. 'inf' as Limit disables the limit (traverse whole table).
%%
%% Advanced usage exmaples:
%%   progress indicator and output dump (to retain partial results):
%%     ds_drv:spec_table(mnesia, payment_rec, #payment_rec.primary_reference,
%%                       inf, [{progress, 10000}, dump]).
%%
%%   chain field references to spec a sub-subfield on nested records:
%%     ds_drv:spec_table(mnesia, kcase, [#kcase.payer_info, #payer_info.payer_bg], inf).
%%
%%   getter function for arbitrary data selection:
%%     FieldSpecF = fun(KC) -> KC#kcase.payer_info#payer_info.payer_bg end,
%%     ds_drv:spec_table(mnesia, kcase, FieldSpecF, inf).
%%
%%   data attributes:
%%     ds_drv:spec_table(mnesia, kcase,
%%                       {#kcase.ocr, [{ts, #kcase.create_date}, {key, #kcase.cid}]},
%%                       inf, [{progress, 10000}, dump]).
%%
%%   NB. using a getter fun is slower than a chained reference (list of field numbers),
%%   so use the fun only where a truly generic accessor is needed. Also, the fun might
%%   throw an exception in certain cases to exclude those from the spec.

-record(state,
        { status       % idle | spec_table | spec_disk_log
        , handle       % #handle{}
        , field_spec
        , fold_fun
        , limit
        , current_pos  % table key | disk_log continuation
        , progress
        , spec
        }).

-record(handle,
        { type         % dets | ets | mnesia
        , table        % table name/id
        , filename
        , accessors
        }).

spec_table(Type, Tab, FieldSpec, Limit, Opts) ->
    ds_opts:setopts(Opts),
    spec_table(Type, Tab, FieldSpec, Limit).

%% For dets, we also support passing the filename as the table identifier,
%% in which case we will open and close it for ourselves:
spec_table(dets, Filename, FieldSpec, Limit) when is_list(Filename) ->
    Handle = #handle{type=dets, table=undefined, filename=Filename,
                     accessors=accessors(dets)},
    State0 = init(Handle, FieldSpec, Limit),
    run(State0);
spec_table(Type, Tab, FieldSpec, Limit) ->
    Handle = #handle{type=Type, table=Tab, accessors=accessors(Type)},
    State0 = init(Handle, FieldSpec, Limit),
    run(State0).

spec_disk_log(Filename, FieldSpec, Limit, Opts) ->
    ds_opts:setopts(Opts),
    spec_disk_log(Filename, FieldSpec, Limit).

spec_disk_log(Filename, FieldSpec, Limit) ->
    State0 = init(Filename, FieldSpec, Limit),
    run(State0).


run(#state{status=idle, spec=Spec}) -> Spec;
run(State0) ->
    State = fold(State0),
    run(State).


%% Initialize a fold through a table
init(#handle{} = Handle0, FieldSpec0, Limit) ->
    ds_records:init(),
    Handle = open_dets_table(Handle0),
    #handle{table=Tab, accessors={FirstF, _ReadF, _NextF}} = Handle,
    Progress = ds_progress:init(),
    FieldSpec = normalize_spec(FieldSpec0),
    FoldFun = fold_kernel(fun ds:add/2, FieldSpec),
    #state{status=spec_table, handle=Handle, field_spec=FieldSpec,
           fold_fun=FoldFun, limit=Limit, current_pos=FirstF(Tab),
           progress=Progress, spec=ds:new()};
%% Initialize a fold through a disk_log file. Useful for Mnesia .DCD and .DCL
init(Filename, FieldSpec0, Limit) ->
    ds_records:init(),
    {ok, dlog} = disk_log:open([ {name, dlog}
                               , {file, Filename}
                               , {mode, read_only}
                               , {repair, false}
                               ]),
    Progress = ds_progress:init(),
    FieldSpec = normalize_spec(FieldSpec0),
    FoldFun = fold_kernel(fun ds:add/2, FieldSpec),
    #state{status=spec_disk_log, handle=Filename, field_spec=FieldSpec,
           fold_fun=FoldFun, limit=Limit, current_pos=start,
           progress=Progress, spec=ds:new()}.

%% table
fold(#state{status=spec_table, current_pos='$end_of_table'} = State) ->
    finish_fold(State);
fold(#state{status=spec_table, limit=0} = State) ->
    finish_fold(State);
fold(#state{status=spec_table,
            handle=#handle{table=Tab, accessors={_FirstF, ReadF, NextF}},
            fold_fun=FoldFun, limit=Limit, current_pos=Key,
            progress=Progress0, spec=Spec0} = State) ->
    RecL = ReadF(Tab, Key),
    RecN = length(RecL),
    Progress = ds_progress:update(Progress0, Spec0, RecN),
    Spec = lists:foldl(FoldFun, Spec0, RecL),
    State#state{spec=Spec, limit=counter_dec(Limit, RecN),
                progress=Progress, current_pos=NextF(Tab, Key)};
%% disk_log
fold(#state{status=spec_disk_log, limit=0} = State) ->
    finish_fold(State);
fold(#state{status=spec_disk_log, current_pos=Cont0} = State) ->
    case disk_log:chunk(dlog, Cont0) of
        eof ->
            fold(State#state{limit=0}); % trigger end clause by setting Limit to 0
        {error, Reason} ->
            io:format("disk_log: chunk failed: ~p~n", [Reason]),
            fold(State#state{limit=0}); % trigger end clause by setting Limit to 0
        {Cont, RecL} ->
            do_fold_disk_log(State#state{current_pos=Cont}, RecL);
        {Cont, RecL, BadBytes} ->
            io:format("disk_log: skipped ~B bad bytes~n", [BadBytes]),
            do_fold_disk_log(State#state{current_pos=Cont}, RecL)
    end.

do_fold_disk_log(#state{status=spec_disk_log, fold_fun=FoldFun, limit=Limit,
                        progress=Progress0, spec=Spec0} = State, RecL) ->
    case {ds_progress:get_count(Progress0), RecL} of
        %% ignore log_header entry at the start
        {0, [{log_header, dcd_log, _, _, _, _}|RestRecL]} ->
            do_fold_disk_log(State, RestRecL);
        _ ->
            RecN = length(RecL),
            Progress = ds_progress:update(Progress0, Spec0, RecN),
            Spec = lists:foldl(FoldFun, Spec0, RecL),
            State#state{spec=Spec, progress=Progress, limit=counter_dec(Limit, RecN)}
    end.

finish_fold(#state{status=spec_table, handle=Handle0, progress=Progress,
                   spec=Spec0} = State) ->
    Handle = close_dets_table(Handle0),
    Spec = ds_progress:final(Progress, Spec0),
    State#state{status=idle, handle=Handle, spec=Spec};
finish_fold(#state{status=spec_disk_log, progress=Progress, spec=Spec0} = State) ->
    ok = disk_log:close(dlog),
    Spec = ds_progress:final(Progress, Spec0),
    State#state{status=idle, spec=Spec}.

%% If a dets table filename was supplied, we need to open it first.
open_dets_table(#handle{type=dets, table=undefined, filename=Filename} = Handle)
  when is_list(Filename) ->
    {ok, Dets} = dets:open_file(dets_table, [ {file, Filename}
                                            , {access, read}
                                            , {repair, false}
                                            , {keypos, 2} %% FIXME is this always the case?
                                            ]),
    Handle#handle{table=Dets};
open_dets_table(Handle) -> Handle.

%% If the dets table was opened by us, close it.
close_dets_table(#handle{type=dets, table=Dets, filename=Filename} = Handle)
  when is_list(Filename) ->
    ok = dets:close(Dets),
    Handle#handle{table = undefined};
close_dets_table(Handle) -> Handle.


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
