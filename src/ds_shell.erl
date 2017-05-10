%% -*- coding: utf-8 -*-
-module(ds_shell).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% API functions
-export([ start/0
        , set_statusline/1
        , set_statusline_pid/1
        ]).

%% exported for external calls & spawn_*
-export([ repl/1
        , statusline_srv/0
        ]).

-define(STATUSLINE_KEY, ds_shell_status_line).
-define(PROMPT, "ds => ").
-define(PARAMS, ["type", "table", "field", "attrs", "options"]).
-define(PARAMS_STR, string:join(?PARAMS, " ")).
-define(TYPES, ["dets", "disk_log", "ets", "mnesia"]).
-define(TYPES_STR, string:join(?TYPES, " ")).
-define(DEFAULT_OPTIONS, [dump, {progress, 0.5}]).

-record(state,
        { type
        , table
        , field      = 0
        , attrs      = [] %[{ts, 3}, {key, 2}]
        , options    = ?DEFAULT_OPTIONS
        , probe_pid
        , probe_monitor
        }).

start() ->
    io:fwrite(user,
              "\nDumpsterl interactive probe shell.\n"
              "Type 'quit' + <enter> to get back to the Erlang shell;\n"
              "     'help' + <enter> to get help.\n\n", []),
    ds_opts:setopts(?DEFAULT_OPTIONS),
    ds_records:init(),
    set_statusline_pid(spawn_link(?MODULE, statusline_srv, [])),
    DefaultType = ets,
    DefaultTable = default_table(DefaultType),
    repl(#state{type = DefaultType, table = DefaultTable}).

default_table(ets) ->
    case ets:all() of
        [] -> undefined;
        AllTables ->
            case lists:filter(fun is_atom/1, AllTables) of
                [FirstAtom|_] -> FirstAtom;
                [] -> hd(AllTables)
            end
    end;
default_table(mnesia) ->
    case catch mnesia:system_info(tables) of
        {'EXIT', {aborted, _}} -> undefined;
        [schema] -> undefined; %% can't read the schema via mnesia!
        Tables -> hd(lists:filter(fun(schema) -> false;
                                     (_) -> true
                                  end, Tables))
    end;
default_table(_Type) -> undefined.

repl(State0) ->
    Line = io:get_line(?PROMPT),
    State = wait_for_probe(0, State0),
    print_statusline(Line, State),
    case Line of
        "quit" ++ _ ->
            stop_probe(State),
            ok; %% exit
        "\n" ->
            ?MODULE:repl(State);
        _ ->
            NewState = eval_cmd(Line, State),
            io:nl(),
            ?MODULE:repl(NewState)
    end.

print_statusline(_Line, #state{probe_pid = undefined}) -> ok;
print_statusline(Line, _State) ->
    reprint_prompt_line(Line, ds_opts:getopt(term)),
    case get_statusline() of
        "" -> ok;
        StatusLine -> io:fwrite([StatusLine, $\n])
    end.

reprint_prompt_line(_Line, "dumb") -> ok;
reprint_prompt_line(Line, "vt100") ->
    %% reprint prompt one above of where it was
    io:fwrite([line_up_and_erase(), line_up_and_erase(), ?PROMPT, Line]).

eval_cmd("set " ++ ArgsStr, State) ->
    case string:tokens(ArgsStr, " \r\n\t") of
        [ParamStr|Values] ->
            set_param(ParamStr, string:join(Values, " "), State);
        _  ->
            io:format("syntax: set <param> <value>\n"),
            State
    end;
eval_cmd("show" ++ ParamsStr, State) ->
    Params0 = string:tokens(ParamsStr, " \r\n\t"),
    Params = case Params0 of
                 [] -> io:format("~10s:~n~11s~n", ["params", "-------"]),
                       ?PARAMS;
                 _  -> Params0
             end,
    [show_param(P, State) || P <- Params],
    State;
eval_cmd("run\n", State) ->
    case check_params(State) of
        true  -> run_probe(State);
        false -> State
    end;
eval_cmd("stop\n", State) -> stop_probe(State);
eval_cmd("gui\n", State) -> gui(State);
eval_cmd("gui " ++ ParamStr, State) -> gui(ParamStr, State);
eval_cmd("help\n", State) -> help(State);
eval_cmd("help " ++ ParamStr, State) -> help(ParamStr, State);
eval_cmd(Line, State) -> invalid_input(Line, State).

set_param("type"=Param, ValueStr, State) ->
    case lists:member(ValueStr, ?TYPES) of
        true  -> show_param(Param, State#state{type = list_to_atom(ValueStr)});
        false -> io:format("valid types: ~s~n", [?TYPES_STR]),
                 State
    end;
set_param("table"=Param, ValueStr, State) ->
    case parse_term(ValueStr) of
        {ok, Value} ->
            case check_param(Param, Value, State) of
                true  -> show_param(Param, State#state{table = Value});
                false -> State
            end;
        {error, ReasonStr} -> io:format("~s~n", [ReasonStr]),
                              State
    end;
set_param("field"=Param, ValueStr, State) ->
    case parse_term(ValueStr) of
        {ok, Value} -> show_param(Param, State#state{field = Value});
        {error, ReasonStr} -> io:format("~s~n", [ReasonStr]),
                              State
    end;
set_param("attrs"=Param, ValueStr, State) ->
    case parse_term(ValueStr) of
        {ok, Value} -> show_param(Param, State#state{attrs = Value});
        {error, ReasonStr} -> io:format("~s~n", [ReasonStr]),
                              State
    end;
set_param(Param, ValueStr, State) ->
    case parse_term(ValueStr) of
        {ok, Value} -> set_opt(Param, Value, State);
        {error, ReasonStr} -> io:format("~s~n", [ReasonStr]),
                              State
    end.

set_opt(OptStr, Value, #state{options=Options0} = State) ->
    OptKeys = ds_opts:keys(),
    Opt = try erlang:list_to_existing_atom(OptStr)
          catch error:badarg -> invalid_option
          end,
    case lists:member(Opt, OptKeys) of
        true  ->
            Options1 = proplists:delete(Opt, Options0),
            Options2 = case Value of
                           false -> Options1;
                           _     -> [{Opt, Value} | Options1]
                       end,
            Options = ds_opts:normalize_opts(Options2),
            ds_opts:setopts(Options),
            case Opt of
                mnesia_dir -> ds_records:reinit();
                _ -> ok
            end,
            show_param(OptStr, State#state{options = Options});
        false ->
            invalid_param(OptStr),
            State
    end.

%% Check all params that cannot be immediately validated on their input.
%% Return true if everything is ok (probe can be started);
%% return false and print message if a problem is found.
check_params(#state{table=Table} = State) ->
    check_param("table", Table, State).

%% Check if supplied parameter is valid; return true | false accordingly.
%% If parameter is invalid, print appropriate error message.
check_param("table"=_Param, schema, #state{type=mnesia}) ->
    io:format("Can't process schema table via mnesia; read it as a dets file!\n"),
    false;
check_param("table"=_Param, Value, #state{type=mnesia}) ->
    case catch mnesia:table_info(Value, attributes) of
        {'EXIT', {aborted, {no_exists, Value, _}}} ->
            io:format("~p does not look like a mnesia table!\n", [Value]),
            false;
        _Attrs -> true
    end;
check_param("table"=_Param, Value, #state{type=ets}) ->
    case lists:member(Value, ets:all()) of
        false ->
            io:format("~p does not look like an ets table!\n", [Value]),
            false;
        true -> true
    end;
check_param("table"=_Param, Value, #state{type=dets}) ->
    case filelib:is_regular(Value) andalso dets:is_dets_file(Value) of
        false ->
            case dets:info(Value) of
                undefined ->
                    io:format("~p does not look like a dets table!\n", [Value]),
                    false;
                _InfoList -> true
            end;
        true -> true
    end;
check_param("table"=_Param, Value, #state{type=disk_log}) ->
    try true = filelib:is_regular(Value),
        {ok, dlog_test} = disk_log:open([ {name, dlog_test}
                                        , {file, Value}
                                        , {mode, read_only}
                                        , {repair, false}
                                        ]),
        ok = disk_log:close(dlog_test),
        true
    catch error:{badmatch, _} ->
        io:format("~p does not look like a disk_log file!\n", [Value]),
        false
    end.


show_param("type"=Param, #state{type=Type}=State) ->
    io:format("~10s: ~p~n", [Param, Type]), State;
show_param("table"=Param, #state{table=Table}=State) ->
    io:format("~10s: ~p~n", [Param, Table]), State;
show_param("field"=Param, #state{field=Field}=State) ->
    io:format("~10s: ~p~n", [Param, Field]), State;
show_param("attrs"=Param, #state{attrs=Attrs}=State) ->
    io:format("~10s: ~p~n", [Param, Attrs]), State;
show_param("options", #state{options=Options}=State) ->
    OptKeys = ds_opts:keys(),
    io:format("\n~10s:~n~11s~n",
              ["options", "--------"]),
    [show_opt(K, Options) || K <- OptKeys],
    State;
show_param(OptStr, #state{options=Options}=State) ->
    OptKeys = ds_opts:keys(),
    Opt = opt_from_string(OptStr),
    case lists:member(Opt, OptKeys) of
        true  -> show_opt(Opt, Options);
        false -> invalid_param(OptStr)
    end,
    State.

show_opt(Key, Options) ->
    Value = ds_opts:getopt(Key, Options),
    io:format("~10s: ~p~n", [Key, Value]).

help(State) ->
    io:format("Commands:\n"
              "  help [<command|param|option>]\n"
              "  show [<param|option> ...]\n"
              "      params: ~s\n"
              "      options: ~s\n"
              "  set <param|option> <value>\n"
              "      see above for params and options\n"
              "  run\n"
              "  stop\n"
              "  gui [<dump>]\n"
              "  quit\n",
              [?PARAMS_STR, opts_str()]),
    State.

help("help"++_, State) ->
    io:format("Display help for dumpsterl commands, parameters and options.\n"
              "Type 'help <keyword>' to get help specific to a keyword.\n"
              "Type 'show' to get a list of valid parameters and options.\n"),
    State;
%% TODO help text for all commands, params and options
help(Str, State) -> invalid_input(Str, State).

opt_from_string(OptStr) ->
    try erlang:list_to_existing_atom(OptStr)
    catch error:badarg -> invalid_option
    end.

opts_str() ->
    string:join([atom_to_list(Key) || Key <- ds_opts:keys()], " ").

invalid_input(Str, State) ->
    io:format("Unrecognized input: ~s" % Str ends with \n
              "Type 'help' to get a list of commands.\n", [Str]),
    State.

invalid_param(Param) ->
    io:format("~10s: invalid parameter or option~n"
              "~10s: ~s~n"
              "~10s: ~s~n",
              [Param, "params", ?PARAMS_STR, "options", opts_str()]).

parse_term(Str) ->
    case erl_scan:string(Str ++ ".") of
        {error, ErrorInfo, _ErrorLocation} ->
            {error, error_string(ErrorInfo)};
        {ok, Tokens, _EndLocation} ->
            case erl_parse:parse_term(Tokens) of
                {error, ErrorInfo} ->
                    {error, error_string(ErrorInfo)};
                {ok, Term} -> {ok, Term}
            end
    end.

error_string({_Loc, erl_scan,_Error}) -> "parse error";
error_string({_Loc,_Mod, Error}) -> Error.


gui(#state{options = Options} = State) ->
    DumpFile = ds_opts:getopt(dump, Options),
    start_gui(DumpFile, State).

gui(ParamStr, State) ->
    case parse_term(ParamStr) of
        {ok, DumpFile} -> start_gui(DumpFile, State);
        {error, ReasonStr} -> io:format("~s~n", [ReasonStr]),
                              State
    end.

start_gui(DumpFile, State) ->
    case filelib:is_regular(DumpFile) of
        true -> ds_gui:start(DumpFile);
        false -> io:format("Cannot access dump file: ~p~n", [DumpFile])
    end,
    State.


run_probe(#state{type = Type, table = Table, field = Field, attrs = Attrs,
                 options = Options} = State) ->
    FieldSpec = {Field, Attrs},
    RecAttrs = ds_records:get_attrs(),
    {ProbePid, ProbeMon} = ds_probe:start(Type, Table, FieldSpec, Options,
                                          RecAttrs, get_statusline_pid()),
    State#state{probe_pid = ProbePid, probe_monitor = ProbeMon}.

stop_probe(#state{probe_pid = undefined} = State) ->
    io:format("probe is not running\n"),
    State;
stop_probe(#state{probe_pid = Pid} = State) ->
    Pid ! finish,
    wait_for_probe(infinity, State).

wait_for_probe(_Timeout, #state{probe_pid = undefined} = State) -> State;
wait_for_probe(Timeout, #state{probe_pid = Pid, probe_monitor = Monitor} = State) ->
    receive {'DOWN', Monitor, process, Pid, _Reason} ->
            State#state{probe_pid = undefined, probe_monitor = undefined}
    after Timeout ->
            State
    end.

get_statusline_pid() -> get(statusline_pid).

get_statusline() ->
    case get_statusline_pid() of
        undefined -> "";
        StatusLinePid -> get_statusline(StatusLinePid)
    end.

get_statusline(StatusLinePid) ->
    StatusLinePid ! {self(), get_statusline},
    receive
        {statusline, StatusLine} -> StatusLine
    end.

%% Mini-server to keep track of last status line received from probe
statusline_srv() ->
    statusline_loop("").

statusline_loop(StatusLine0) ->
    receive
        {statusline, StatusLine} ->
            statusline_loop(StatusLine);
        {From, get_statusline} ->
            From ! {statusline, StatusLine0},
            statusline_loop(StatusLine0)
    end.

set_statusline_pid(StatusLinePid) ->
    put(statusline_pid, StatusLinePid).

set_statusline(IoData) ->
    case get_statusline_pid() of
        undefined -> ok;
        Pid  -> Pid ! {statusline, IoData}
    end,
    io:fwrite([line_up_and_erase(), IoData, $\n]).

line_up_and_erase() -> line_up_and_erase(ds_opts:getopt(term)).

%% This is the only terminal-specific bit.
line_up_and_erase("vt100") -> "\e[A\e[2K";
line_up_and_erase("dumb")  -> "".
