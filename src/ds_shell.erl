-module(ds_shell).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ start/0
        , set_statusline/1
        , repl/1
        ]).

-define(STATUSLINE_KEY, ds_shell_status_line).
-define(PROMPT, "ds => ").
-define(PARAMS, ["type", "table", "field", "attrs", "options"]).
-define(PARAMS_STR, string:join(?PARAMS, " ")).
-define(TYPES, ["dets", "disk_log", "ets", "mnesia"]).
-define(TYPES_STR, string:join(?TYPES, " ")).

-record(state,
        { type       = disk_log % ets
        , table      = "kred_data/kcase.DCD"
        , field      = 0
        , attrs      = [] %[{ts, 3}, {key, 2}]
        , options    = [dump, {progress, 0.5}]
        , probe_pid
        , probe_monitor
        }).

start() ->
    io:fwrite(user,
              "\nDumpsterl interactive probe shell.\n"
              "Type 'quit' + <enter> to get back to the Erlang shell;\n"
              "     'help' + <enter> to get help.\n\n", []),
    repl(#state{}).

repl(State0) ->
    io:fwrite([get_statusline(), $\n]),
    Line = io:get_line(?PROMPT),
    %% reprint prompt one above of where it was
    io:fwrite([line_up_and_erase(), line_up_and_erase(), ?PROMPT, Line]),
    State1 = wait_for_probe(0, State0),
    case Line of
        "quit" ++ _ ->
            %% TODO if probe is running, stop it first!
            ok; %% exit
        "\n" ->
            ?MODULE:repl(State1);
        _ ->
            State = eval_cmd(Line, State1),
            io:nl(),
            ?MODULE:repl(State)
    end.

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
eval_cmd("run\n",
         #state{type = Type, table = Table, field = Field, attrs = Attrs,
                options = Options} = State) ->
    FieldSpec = {Field, Attrs},
    ProbePid = spawn_link(ds_drv, spec, [Type, Table, FieldSpec, Options]),
    ProbeMon = monitor(process, ProbePid),
    State#state{probe_pid = ProbePid, probe_monitor = ProbeMon};
eval_cmd("stop\n", #state{probe_pid = undefined} = State) ->
    io:format("probe is not running!\n"),
    State;
eval_cmd("stop\n", #state{probe_pid = ProbePid} = State) ->
    ProbePid ! finish,
    wait_for_probe(infinity, State);
eval_cmd("help\n", State) -> help(State);
eval_cmd("help " ++ ParamStr, State) -> help(ParamStr, State);
eval_cmd(Line, State) -> invalid(Line, State).

set_param("type"=Param, ValueStr, State) ->
    case lists:member(ValueStr, ?TYPES) of
        true  -> show_param(Param, State#state{type = list_to_atom(ValueStr)});
        false -> io:format("valid types: ~s~n", [?TYPES_STR]),
                 State
    end;
set_param("table"=Param, ValueStr, State) ->
    case parse_term(ValueStr) of
        {ok, Value} -> show_param(Param, State#state{table = Value});
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
            show_param(OptStr, State#state{options = Options});
        false ->
            invalid_param(OptStr),
            State
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
              "  help [<keyword>]\n"
              "  show [<param>|<option> ...]\n"
              "      params: ~s\n"
              "  set <param> <value>\n"
              "      params as above, plus individual options\n"
              "      (type 'help options' for valid options)\n"
              "  run\n"
              "  stop\n"
              "  quit\n",
              [?PARAMS_STR]),
    State.

help("help"++_, State) ->
    io:format("Display help for dumpsterl commands, parameters and options.\n"
              "Type 'help <keyword>' to get help specific to a keyword.\n"
              "Type 'show' to get a list of valid parameters and options.\n"),
    State;
%% TODO help text for all commands, params and options
help(Str, State) -> invalid(Str, State).

opt_from_string(OptStr) ->
    try erlang:list_to_existing_atom(OptStr)
    catch error:badarg -> invalid_option
    end.

invalid(Str, State) ->
    io:format("Unrecognized input: ~s" % Str ends with \n
              "Type 'help' to get a list of commands.\n", [Str]),
    State.

invalid_param(Param) ->
    OptKeys = ds_opts:keys(),
    OptsStr = string:join([atom_to_list(Key) || Key <- OptKeys], " "),
    io:format("~10s: invalid parameter or option~n"
              "~10s: ~s~n"
              "~10s: ~s~n",
              [Param, "params", ?PARAMS_STR, "options", OptsStr]).

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

error_string({_Loc,_Mod, Error}) -> Error.

wait_for_probe(_Timeout, #state{probe_pid = undefined} = State) -> State;
wait_for_probe(Timeout, #state{probe_pid = Pid, probe_monitor = Monitor} = State) ->
    receive {'DOWN', Monitor, process, Pid, _Reason} ->
            io:format("probe stopped.\n"),
            State#state{probe_pid = undefined, probe_monitor = undefined}
    after Timeout ->
            State
    end.

get_statusline() ->
    case get(?STATUSLINE_KEY) of
        undefined  -> "";
        StatusLine -> StatusLine
    end.

%% FIXME this is called from ds_drv, which runs in another process!
set_statusline(IoData) ->
    put(?STATUSLINE_KEY, IoData),
    io:fwrite([line_up_and_erase(), IoData, $\n]).

%% This is the only terminal-specific bit.
line_up_and_erase() -> line_up_and_erase(ds_opts:getopt(term)).

line_up_and_erase("vt100") -> "\e[A\e[2K";
line_up_and_erase("dumb")  -> "".
