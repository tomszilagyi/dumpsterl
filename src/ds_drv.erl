%% Driver functions for dumpsterl
-module(ds_drv).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% API for manual use
-export([ spec/3
        , spec/4
        , start/3
        , start/4
        ]).

%% call by ds_shell; for spawn_monitor and spawn_link:
-export([ start/6
        , init/6
        , procs_slave/3 ]).

%%-define(DEBUG, true).
-include("debug.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Go through a table to spec a field based on a limited number
%% of rows processed (option 'limit' defaults to 1000):
%%
%%   ds_drv:spec(ets, my_table, #my_record.my_field)
%%
%% Sending 0 as FieldSpec will process the whole record.
%% Setting 'limit' to 'infinity' disables the limit (traverse whole table).
%%
%% Advanced usage exmaples:
%%   progress indicator and output dump:
%%     ds_drv:spec(mnesia, payment_rec, #payment_rec.primary_reference,
%%                 [{limit, infinity}, progress, dump]).
%%
%%   chain field references to spec a sub-subfield on nested records:
%%     ds_drv:spec(mnesia, kcase, [#kcase.payer_info, #payer_info.payer_bg]).
%%
%%   getter function for arbitrary data selection:
%%     FieldSpecF = fun(KC) -> KC#kcase.payer_info#payer_info.payer_bg end,
%%     ds_drv:spec(mnesia, kcase, FieldSpecF).
%%
%%   data attributes:
%%     ds_drv:spec(mnesia, kcase,
%%                 {#kcase.ocr, [{ts, #kcase.create_date}, {key, #kcase.cid}]},
%%                 [{limit, infinity}, {progress, 0.5}, dump]).
%%
%%   NB. using a getter fun is slower than a chained reference (list of field numbers),
%%   so use the fun only where a truly generic accessor is needed. Also, the fun might
%%   throw an exception in certain cases to exclude those from the spec.

-record(state,
        { status       % idle | spec_table | spec_disk_log
        , handle       % #handle{} | filename()
        , field_spec
        , fold_fun
        , limit
        , current_pos  % table key | disk_log continuation
        , progress
        , spec
        , n_procs      % number of parallel processes, including master
        , master_pid
        , next_pid
        }).

-record(handle,
        { type         % dets | ets | mnesia
        , table        % table name/id
        , filename
        , accessors
        }).


start(Type, Tab, FieldSpec) ->
    start(Type, Tab, FieldSpec, []).

start(Type, Tab, FieldSpec, Opts) ->
    start(Type, Tab, FieldSpec, Opts, undefined, undefined).

start(Type, Tab, FieldSpec, Opts, RecAttrs, StatusLinePid) ->
    spawn_monitor(?MODULE, init, [Type, Tab, FieldSpec, Opts, RecAttrs, StatusLinePid]).

init(Type, Tab, FieldSpec, Opts, RecAttrs, StatusLinePid) ->
    ds_opts:setopts(Opts),
    ds_records:put_attrs(RecAttrs),
    ds_shell:set_statusline_pid(StatusLinePid),
    spec(Type, Tab, FieldSpec).

spec(Type, Tab, FieldSpec, Opts) ->
    ds_opts:setopts(Opts),
    spec(Type, Tab, FieldSpec).

spec(disk_log, Filename, FieldSpec) ->
    State = init_fold(Filename, FieldSpec),
    procs_init(State);
%% For dets, we also support passing the filename as the table identifier,
%% in which case we will open and close it for ourselves:
spec(dets, Filename, FieldSpec) when is_list(Filename) ->
    Handle = #handle{type=dets, table=undefined, filename=Filename,
                     accessors=accessors(dets)},
    State = init_fold(Handle, FieldSpec),
    procs_init(State);
spec(Type, Tab, FieldSpec) ->
    Handle = #handle{type=Type, table=Tab, accessors=accessors(Type)},
    State = init_fold(Handle, FieldSpec),
    procs_init(State).


%% Entry point of parallel processing probe runner;
%% start of master that runs in-process in the drv
procs_init(#state{current_pos = Pos, limit = Limit} = State) ->
    NProcs = ds_opts:getopt(procs),
    if NProcs > 1 -> io:format("running probe on ~B parallel processes.\n", [NProcs]);
       true -> ok
    end,
    MasterPid = self(),
    ?debug("init master ~p", [MasterPid]),
    %% inhibit progress output in slaves:
    SlaveState = State#state{progress=ds_progress:init(false)},
    NextPid = procs_spawn_slave(SlaveState, MasterPid, NProcs),
    MasterPid ! {proc, Pos, Limit, 0}, % Trigger the processing
    procs_master_loop(State#state{master_pid = MasterPid, next_pid = NextPid,
                                  progress=ds_progress:init(), n_procs = NProcs}).

%% The NextPid of the last slave is MasterPid
procs_spawn_slave(_State, MasterPid, 1) -> MasterPid;
procs_spawn_slave(State, MasterPid, N) ->
    spawn_link(?MODULE, procs_slave, [State, MasterPid, N-1]).

%% Entry point of slave process
procs_slave(State, MasterPid, N) ->
    ?debug("init slave ~p", [self()]),
    NextPid = procs_spawn_slave(State, MasterPid, N),
    procs_slave_loop(State#state{master_pid = MasterPid, next_pid = NextPid}).

procs_master_loop(#state{status = idle, n_procs = 1, spec = Spec0, progress = Progress}) ->
    Spec = ds_progress:final(Progress, Spec0),
    io:format("probe finished.\n"),
    Spec;
procs_master_loop(#state{next_pid = NextPid, spec=Spec0, limit = PrevLimit,
                         progress = Progress0, n_procs = NProcs} = State0) ->
    receive
        {proc, Pos0, Limit0, AccRecN} ->
            Progress = ds_progress:update(Progress0, Spec0, AccRecN),
            State1 = State0#state{current_pos = Pos0, limit = min(PrevLimit, Limit0),
                                  progress = Progress},
            case prep_fold(State1) of
                {State2, done} ->
                    ?debug("master: prep_fold -> done", []),
                    NextPid ! get_result,
                    State3 = finish_fold(State2),
                    procs_master_loop(State3);
                {#state{current_pos=NextPos, limit=Limit} = State2, RecL, RecN} ->
                    NextPid ! {proc, NextPos, Limit, RecN},
                    State = fold(State2, RecL),
                    procs_master_loop(State)
            end;
        {done, AccRecN} ->
            ?debug("master: got 'done', AccRecN=~p", [AccRecN]),
            NextPid ! get_result,
            State = finish_fold(State0),
            Progress = ds_progress:update(Progress0, Spec0, AccRecN),
            procs_master_loop(State#state{progress=Progress});
        {result,_Pid, SlaveSpec} ->
            ?debug("master: result from ~p, n_procs: ~p", [_Pid, NProcs]),
            Spec = ds:join(Spec0, SlaveSpec),
            procs_master_loop(State0#state{spec=Spec, n_procs = NProcs-1});
        finish ->
            procs_master_loop(State0#state{limit = 0}); % trigger soft finish
        _Msg ->
            ?debug("master: got msg: ~p", [_Msg]),
            procs_master_loop(State0)
    end.

procs_slave_loop(#state{master_pid = MasterPid, next_pid = NextPid,
                        spec = Spec} = State0) ->
    receive
        {proc, Pos0, Limit0, AccRecN} ->
            State1 = State0#state{current_pos=Pos0, limit=Limit0},
            case prep_fold(State1) of
                {State2, done} ->
                    ?debug("slave_loop ~p: prep_fold -> done", [self()]),
                    MasterPid ! {done, AccRecN},
                    procs_slave_loop(State2);
                {#state{current_pos=NextPos, limit=Limit} = State2, RecL, RecN} ->
                    NextPid ! {proc, NextPos, Limit, AccRecN + RecN},
                    State = fold(State2, RecL),
                    procs_slave_loop(State)
            end;
        get_result ->
            NextPid ! get_result,
            MasterPid ! {result, self(), Spec};
        _Msg ->
            ?debug("slave ~p: got msg: ~p", [self(), _Msg]),
            procs_slave_loop(State0)
    end.


%% Initialize a fold through a table
init_fold(#handle{} = Handle0, FieldSpec0) ->
    ds_records:init(),
    Handle = open_dets_table(Handle0),
    #handle{table=Tab, accessors={FirstF, _ReadF, _NextF}} = Handle,
    FieldSpec = normalize_spec(FieldSpec0),
    FoldFun = fold_kernel(fun ds:add/2, FieldSpec),
    #state{status=spec_table,
           handle=Handle, field_spec=FieldSpec, fold_fun=FoldFun,
           spec=ds:new(), limit=ds_opts:getopt(limit), current_pos=FirstF(Tab)};
%% Initialize a fold through a disk_log file. Useful for Mnesia .DCD and .DCL
init_fold(Filename, FieldSpec0) ->
    ds_records:init(),
    {ok, dlog} = disk_log:open([ {name, dlog}
                               , {file, Filename}
                               , {mode, read_only}
                               , {repair, false}
                               ]),
    FieldSpec = normalize_spec(FieldSpec0),
    FoldFun = fold_kernel(fun ds:add/2, FieldSpec),
    #state{status=spec_disk_log,
           handle=Filename, field_spec=FieldSpec, fold_fun=FoldFun,
           spec=ds:new(), limit=ds_opts:getopt(limit), current_pos=start}.

%% Prepare a fold step based on current state. Result is either
%% - {#state{}, done} in case we are done;
%% - {#state{current_pos=NextPos, limit=Limit}, RecL, RecN} which must be processed further:
%%   NextPos, Limit and RecN are sent to the next process in the process ring,
%%   while RecL is folded into the spec built by the current process.

%% table
prep_fold(#state{status=spec_table, current_pos='$end_of_table'} = State) ->
    {State, done};
prep_fold(#state{status=spec_table, limit=0} = State) ->
    {State, done};
prep_fold(#state{status=spec_table,
                 handle=#handle{table=Tab, accessors={_FirstF, ReadF, NextF}},
                 current_pos=Key, limit=Limit} = State) ->
    RecL = ReadF(Tab, Key),
    RecN = length(RecL),
    {State#state{current_pos=NextF(Tab, Key), limit=counter_dec(Limit, RecN)}, RecL, RecN};
%% disk_log
prep_fold(#state{status=spec_disk_log, limit=0} = State) ->
    {State, done};
prep_fold(#state{status=spec_disk_log, current_pos=Cont0, limit=Limit} = State) ->
    case disk_log:chunk(dlog, Cont0) of
        eof ->
            prep_fold(State#state{limit=0}); % trigger end clause by setting Limit to 0
        {error, Reason} ->
            io:format(user, "disk_log: chunk failed: ~p~n", [Reason]),
            prep_fold(State#state{limit=0}); % trigger end clause by setting Limit to 0
        {Cont, RecL} ->
            RecN = length(RecL),
            {State#state{current_pos=Cont, limit=counter_dec(Limit, RecN)}, RecL, RecN};
        {Cont, RecL, BadBytes} ->
            io:format(user, "disk_log: skipped ~B bad bytes~n", [BadBytes]),
            RecN = length(RecL),
            {State#state{current_pos=Cont, limit=counter_dec(Limit, RecN)}, RecL, RecN}
    end.


%% Perform the fold step prepared by prep_fold.
%% table
fold(#state{status=spec_table, fold_fun=FoldFun, spec=Spec0} = State, RecL) ->
    Spec = lists:foldl(FoldFun, Spec0, RecL),
    State#state{spec=Spec};
%% disk_log
fold(#state{status=spec_disk_log, fold_fun=FoldFun, spec=Spec0,
            progress=Progress} = State, RecL) ->
    case {ds_progress:get_count(Progress), RecL} of
        %% ignore log_header entry at the start
        {0, [{log_header, dcd_log, _, _, _, _}|RestRecL]} ->
            fold(State, RestRecL);
        _ ->
            Spec = lists:foldl(FoldFun, Spec0, RecL),
            State#state{spec=Spec}
    end.

%% Finish a fold and close resources opened by us.
%% Must be called by the same (master) process as the one that called init_fold.
finish_fold(#state{status=spec_table, handle=Handle0} = State) ->
    Handle = close_dets_table(Handle0),
    State#state{status=idle, handle=Handle};
finish_fold(#state{status=spec_disk_log} = State) ->
    ok = disk_log:close(dlog),
    State#state{status=idle}.

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
