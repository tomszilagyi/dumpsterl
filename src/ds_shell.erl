%% -*- coding: utf-8 -*-

%% @private
-module(ds_shell).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% API functions
-export([ start/0
        , set_statusline/1
        , set_statusline_pid/1
        ]).

%% exported so we can `make' doc/shell_ref.txt
-export([ help/2 ]).

%% exported for external calls & spawn_*
-export([ repl/1
        , statusline_srv/0
        ]).

-define(STATUSLINE_KEY, ds_shell_status_line).
-define(PROMPT, "ds => ").
-define(COMMANDS, ["gui", "help", "quit", "run", "set", "show", "stop"]).
-define(PARAMS, ["type", "table", "field", "attrs", "options"]).
-define(PARAMS_STR, string:join(?PARAMS, " ")).
-define(TYPES, ["dets", "disk_log", "ets", "mnesia"]).
-define(TYPES_STR, string:join(?TYPES, " ")).

-record(state,
        { type
        , table
        , field      = 0
        , attrs      = [] %[{ts, 3}, {key, 2}]
        , options
        , probe_pid
        , probe_monitor
        }).

start() ->
    io:fwrite("\nDumpsterl interactive probe shell.\n"
              "Type 'quit' + <enter> to get back to the Erlang shell;\n"
              "     'help' + <enter> to get help.\n\n", []),
    DefaultType = ets,
    DefaultTable = default_table(DefaultType),
    DefaultOptions = [ dump
                     , {mnesia_dir, ds_utils:cut_cwd(ds_records:mnesia_dir())}
                     , {progress, 0.5}
                     ],
    ds_opts:setopts(DefaultOptions),
    ds_records:init(),
    set_statusline_pid(spawn_link(?MODULE, statusline_srv, [])),
    repl(#state{type = DefaultType,
                table = DefaultTable,
                options = DefaultOptions}).

default_table(ets) ->
    case tables(ets) of
        [] -> undefined;
        AllTables ->
            case lists:filter(fun is_atom/1, AllTables) of
                [FirstAtom|_] -> FirstAtom;
                [] -> hd(AllTables)
            end
    end.

tables(ets) ->
    ets:all();
tables(mnesia) ->
    case catch mnesia:system_info(tables) of
        {'EXIT', {aborted, _}} -> [];
        Tables -> lists:filter(fun(schema) -> false;
                                  (_) -> true
                               end, Tables)
    end.

repl(State0) ->
    ok = io:setopts([{expand_fun, mk_expand_fun(State0)}]),
    Line = io:get_line(?PROMPT),
    State = wait_for_probe(0, State0),
    print_statusline(Line, State),
    case Line of
        "quit" ++ _ ->
            _ = stop_probe(State),
            ok; %% exit
        "\n" ->
            ?MODULE:repl(State);
        _ ->
            NewState = eval_cmd(Line, State),
            io:nl(),
            ?MODULE:repl(NewState)
    end.

%% Return an expand fun used to perform tab-completion (expansion).
%% See the doc for io:setopts/1, option {expand_fun, expand_fun()}.
mk_expand_fun(State) ->
    fun(ReversePrefix) ->
        expand("", lists:reverse(ReversePrefix), State)
    end.

expansions("",_State) -> ?COMMANDS;
%% commands
expansions("help",_State) ->
    Exps = ["all", "commands", "params"] % NB.: "options" is part of ?PARAMS
        ++ ?COMMANDS ++ ?PARAMS ++ opts(),
    [" " ++ E || E <- Exps];
expansions("show",_State) ->
    [" " ++ E || E <- ?PARAMS ++ opts()];
expansions("set",_State) ->
    Exps = (?PARAMS ++ opts()) -- ["options"],
    [" " ++ E || E <- Exps];
%% parameters
expansions("set hll_b",_State) ->
    [" false" | [lists:concat([" ", B]) || B <- lists:seq(4, 16)]];
expansions("set limit",_State) ->
    [" infinity", " 100000", " 200000", " 500000"];
expansions("set progress",_State) ->
    [" false", " 0.5", " 1", " 2", " 5"];
expansions("set rec_attrs",_State) ->
    [" false", " true", " force"];
expansions("set samples",_State) ->
    [" false", " infinity", " 8", " 16", " 24", " 32", " 48", " 64"];
expansions("set type",_State) ->
    [" " ++ E || E <- ?TYPES];
expansions("set table", #state{type=ets}) ->
    [lists:concat([" ", E]) || E <- tables(ets)];
expansions("set table", #state{type=mnesia}) ->
    [lists:concat([" ", E]) || E <- tables(mnesia)];
%% TODO expansions for dets and disk_log
%% starting from mnesia_dir, files that end in DAT and DCD
expansions("set term",_State) ->
    [" dumb", " vt100"];
expansions(_Prefix,_State) ->
    [].

expand(Acc, "", State) ->
    {yes, "", expansions(Acc, State)};
expand(Acc, Prefix, State) ->
    Expansions = expansions(Acc, State),
    FullMatchExps = [E || E <- Expansions, string:str(Prefix, E) =:= 1],
    case FullMatchExps of
        [Exp] ->
            %% one of the possible expansions has been entered in full;
            %% recurse to the next expansion level
            expand(Acc ++ Exp, string:substr(Prefix, length(Exp)+1), State);
        [] ->
            %% try to expand based on partially entered form
            expand_part(Prefix, Expansions)
    end.

expand_part(Prefix, Expansions) ->
    PartMatchExps = [E || E <- Expansions, string:str(E, Prefix) =:= 1],
    case PartMatchExps of
        [] -> % prefix does not match any of the expansions
            {yes, "", []};
        [MatchStr] -> % prefix of a single expansion; expand the remaining part
            {no, string:substr(MatchStr, length(Prefix) + 1), []};
        [MatchH|MatchT] = Matches -> % common prefix of multiple possible expansions
            CommonPrefix =
                lists:foldl(fun ds_utils:common_prefix/2, MatchH, MatchT),
            {yes, string:substr(CommonPrefix, length(Prefix) + 1), Matches}
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
    io:put_chars([line_up_and_erase(), line_up_and_erase(), ?PROMPT, Line]).

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
    _ = [show_param(P, State) || P <- Params],
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
            Options =
                case {Opt, Value} of
                    {rec_attrs, true} ->
                        %% special semantics
                        proplists:delete(rec_attrs, Options0);
                    _ ->
                        case ds_opts:normalize_opts([{Opt, Value}]) of
                            [] -> Options0;
                            [NewOptTuple] ->
                                Options1 = proplists:unfold(Options0),
                                lists:keystore(Opt, 1, Options1, NewOptTuple)
                        end
                end,
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
    io:format("Can't process schema table via mnesia; "
              "please read it as a dets file instead!\n"),
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
    io:format("\nCommands:\n"
              "  help [<command|param|option>]\n"
              "      Type 'help help' for useful pointers.\n"
              "  show [<param|option> ...]\n"
              "      params: ~s\n"
              "      options: ~s\n"
              "  set <param|option> <value>\n"
              "      See above for params and options.\n"
              "  run\n"
              "  stop\n"
              "  gui [<dumpfile>]\n"
              "  quit\n",
              [?PARAMS_STR, opts_str()]),
    State.


%% all:
help("all"++_, State0) ->
    io:fwrite("\nCommands:\n"),
    State1 = help("commands", State0),
    io:fwrite("\n\nParams:\n"),
    State2 = help("params", State1),
    io:fwrite("\n\nOptions:\n"),
    help("options", State2);

%% commands:
help("commands"++_, State) ->
    lists:foldl(fun help/2, State, ?COMMANDS);
help("gui"++_, State) ->
    io:fwrite(
      "\ngui [<dumpfile>]\n"
      "  Launch gui reading the currently configured dump file,\n"
      "  or the dump file specified by the argument, if present.\n"),
    State;
help("help"++_, State) ->
    io:fwrite(
      "\nhelp [<keyword>]\n"
      "  Display help for dumpsterl commands, params and options.\n"
      "  Type:\n"
      "  - 'help' to get a list of valid commands;\n"
      "  - 'help <keyword>' to get help specific to a keyword;\n"
      "  - 'help commands' to get help for all valid commands;\n"
      "  - 'help params' to get help for all valid params;\n"
      "  - 'help options' to get help for all valid options;\n"
      "  - 'help all' to print all help output.\n"
),
    State;
help("quit"++_, State) ->
    io:fwrite(
      "\nquit\n"
      "  Quit the Dumpsterl shell, giving control back to the Erlang shell.\n"
      "  The probe, if running, will be stopped cleanly.\n"),
    State;
help("run"++_, State) ->
    io:fwrite(
      "\nrun\n"
      "  Run the probe with current parameters and options.\n"),
    State;
help("set"++_, State) ->
    io:fwrite(
      "\nset <param|option> <value>\n"
      "  Set given param or option to supplied value.\n"
      "  Type 'help params' to get help for all valid params.\n"
      "  Type 'help options' to get help for all valid options.\n"),
    State;
help("show"++_, State) ->
    io:fwrite(
      "\nshow [<param|option> ...]\n"
      "  Show currently configured parameter and option values.\n"
      "  Issued without arguments, 'show' will print a table with all\n"
      "  configuration values. When given a list of param or option names,\n"
      "  output will be restricted to those only.\n"),
    State;
help("stop"++_, State) ->
    io:fwrite(
      "\nstop\n"
      "  If the probe is running, stop it cleanly. Even if the probe is stopped\n"
      "  this way before reaching its set `limit' or end of table, the spec will\n"
      "  be properly closed, so it will contain all information corresponding to\n"
      "  the final reported record count.\n"),
    State;

%% params:
help("params"++_, State) ->
    lists:foldl(fun help/2, State, lists:sort(?PARAMS) -- ["options"]);
help("attrs"++_, State) ->
    io:fwrite(
      "\nattrs\n"
      "  Attribute selector list to specify metadata attributes.\n"
      "  Syntax: [{Attr, FieldSelector}, ...] where FieldSelector selects\n"
      "          the source of data to use for attribute Attr.\n"
      "          See param `field' for the syntax of FieldSelector.\n"
      "  Attributes currently utilized by the gui:\n"
      "  - 'key': Record key to associate with data from the record;\n"
      "  -  'ts': Timestamp to associate with data from the record.\n"),
    State;
help("field"++_, State) ->
    io:fwrite(
      "\nfield\n"
      "  Record/tuple field to select for inspection. Supported formats:\n"
      "  - 0 or []: spec the whole record;\n"
      "  - pos_integer(): spec the selected field;\n"
      "  - [pos_integer()]: list of chained field references to select data\n"
      "       from embedded tuples or records. E.g. [3,2] is interpreted as:\n"
      "       ``take the third field from the record read from the table, then\n"
      "         take the second field of it to be processed by dumpsterl.''\n"
      "  If the referenced field does not exist, the record is skipped from the\n"
      "  spec (but counts toward the limit and shows up in the output count).\n"),
    State;
help("table"++_, State) ->
    io:fwrite(
      "\ntable\n"
      "  The table to read. Its accepted format depends on the `type' setting:\n"
      "  - mnesia or ets table name (atom);\n"
      "  - ets table id (integer);\n"
      "  - dets table name (any term) for an already opened dets table;\n"
      "  - filename of a dets table or disk_log file.\n"),
    State;
help("type"++_, State) ->
    io:fwrite(
      "\ntype\n"
      "  Type of table. This selects the table driver for the probe.\n"
      "  Valid types: dets | disk_log | ets | mnesia\n"),
    State;

%% options:
help("options"++_, State) ->
    lists:foldl(fun help/2, State, opts());
help(OptStr0, State) when is_list(OptStr0) ->
    OptStr = string:strip(OptStr0, right, $\n),
    case opt_from_string(OptStr) of
        invalid_option -> invalid_param(OptStr);
        Opt -> io:nl(), io:fwrite(ds_opts:help(Opt))
    end,
    State;

help(Str, State) -> invalid_input(Str, State).

opt_from_string(OptStr) ->
    try erlang:list_to_existing_atom(OptStr)
    catch error:badarg -> invalid_option
    end.

opts() -> [atom_to_list(Key) || Key <- ds_opts:keys()].

opts_str() -> string:join(opts(), " ").

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
    _ = case filelib:is_regular(DumpFile) of
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
    _ = case get_statusline_pid() of
            undefined -> ok;
            Pid  -> Pid ! {statusline, IoData}
        end,
    io:put_chars([line_up_and_erase(), IoData, $\n]).

line_up_and_erase() -> line_up_and_erase(ds_opts:getopt(term)).

%% This is the only terminal-specific bit.
line_up_and_erase("vt100") -> "\e[A\e[2K";
line_up_and_erase(_)       -> "".
