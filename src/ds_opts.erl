%% -*- coding: utf-8 -*-
-module(ds_opts).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% Options handling for dumpsterl

-export([ keys/0
        , getopt/1
        , getopt/2
        , getopts/0
        , getopts_all/0
        , setopts/1
        , normalize_opts/1
        , help/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("config.hrl").

opts() ->
    %% Dumpsterl operation can be controlled to some extent via options.
    %% The following table specifies the options interpreted.
    %%
    %% Columns:
    %%      name: name of option
    %% undefined: value to use if option is missing
    %%   novalue: value to use if option is set to true
    %%            (implicit if the option is supplied with no value;
    %%             also used if supplied value fails to validate)
    %%  type str: string description of permitted values or types
    %%  help str: help string describing the option

    %% name          undefined     novalue
    [ {dump,         false,        "ds.bin",
       "filename() | 'false'",
       "  dump the accumulated spec on each progress update, but at most once\n"
       "  every ten seconds (if progress output is enabled), and at the end.\n"
       "  - 'false': no dump is written;\n"
       "  - filename(): the accumulated spec is dumped to this file.\n"
       "  NB. in case of parallel probe execution (see `procs' option), only\n"
       "  the master process dumps its spec. With procs=N, only 1/N of the\n"
       "  whole spec is periodically dumped. Naturally, the entire result\n"
       "  is collected and dumped on completion (or in response to `stop').\n"
      }
    , {hll_b,        8,            8,
       "4..16 | 'false'",
       "  Parameter 'b' (base 2 log of substream count) of the hyperloglog\n"
       "  cardinality estimator. Larger values exponentially increase the\n"
       "  storage requirement and marginally improve estimation precision.\n"
       "  A setting of 'false' disables cardinality estimation.\n"
      }
    , {limit,        1000,         1000,
       "pos_integer() | 'infinity'",
       "  Limit the number of records to process. In case reads are done in\n"
       "  chunks of several records (such as with disk_logs), the limit might\n"
       "  be slightly exceeded. Defaults to 1000, which is a safely low value.\n"
       "  A setting of 'infinity' enables processing the whole table.\n"
      }
    , {mnesia_dir,   undefined,    undefined,
       "dirname()",
       "  The name of the mnesia directory where table data files are stored.\n"
       "  This option is useful if the Erlang node running dumpsterl does not\n"
       "  run a Mnesia instance, but has access to the database filesystem.\n"
      }
    , {procs,        cores(),      cores(),
       "pos_integer()",
       "  Number of parallel processes to use when running the probe.\n"
       "  Defaults to the number of logical processors as reported by ERTS.\n"
      }
    , {progress,     false,        1,
       "number() | 'false'",
       "  Output progress information and (if dump is enabled) write interim\n"
       "  dumps.\n"
       "  - 'false': no output\n"
       "  - T: update progress info every T seconds\n"
       "       (achieved update frequency is limited by read granularity)\n"
      }
    , {rec_attrs,    true,         force,
       "'true' | 'false' | 'force'",
       "  Collect record attributes and include them in the spec with the\n"
       "  data referencing it.\n"
       "  - 'true':  collect data once at the beginning of first run;\n"
       "  - 'false': turn off completely;\n"
       "  - 'force': force (re)collection even if attributes have been already\n"
       "             collected; useful if code has been changed in the system.\n"
      }
    , {samples,      16,           16,
       "pos_integer() | 'false' | 'infinity'",
       "  Maximum number of samples to collect at each spec node.\n"
       "  NB.: using 'infinity' is definitely not recommended, as the spec\n"
       "  size will grow without bounds unless the number of processed\n"
       "  records is limited to a small number!\n"
      }
    , {term,         term(),       "vt100",
       "'dumb' | 'vt100'",
       "  Terminal setting useful to override the $TERM environment variable;\n"
       "  set it to 'dumb' to forcibly disable progress line rewrites.\n"
      }
    ].

keys() -> [Opt || {Opt,_Undefined,_Novalue,_TypeStr,_HelpStr} <- opts()].

%% NB. using the process dict is ugly; passing Opts around is uglier.
-define(PROCDICT_KEY, dumpsterl_opts).

setopts(Opts) -> put(?PROCDICT_KEY, normalize_opts(Opts)).

getopts() ->
    case get(?PROCDICT_KEY) of
        undefined -> [];
        Opts      -> Opts
    end.

%% All options, either explicitly set or as default:
getopts_all() -> [{K, getopt(K)} || K <- keys()].

normalize_opts(Opts) ->
    lists:filtermap(fun(Opt) -> normalize_opt_f(Opt, Opts) end, Opts).

normalize_opt_f(Opt, Opts) ->
    [Key] = proplists:get_keys([Opt]),
    Value0 = try getopt(Key, Opts)
             catch error:badarg -> true % unknown option with no value
             end,
    try case normalize_opt(Key, Value0) of
            {true, NewValue} -> {true, {Key, NewValue}};
            R -> R
        end
    catch _:_ ->
            Value = default_opt(Key, Value0),
            {true, proplists:property(Key, Value)}
    end.

default_opt(Key, Value) ->
    Default = getopt(Key, [Key]),
    io:format("** option ~p: invalid value ~p; using default ~p~n",
              [Key, Value, Default]),
    Default.

%% Return true to keep or {true, {Key, NewValue}} to normalize (change) the
%% value of an option; return false to skip/ignore; throw errors or exceptions
%% if data is invalid.
normalize_opt(dump, S) ->
    true = is_list(S) andalso filelib:is_dir(filename:dirname(S)), true;
normalize_opt(hll_b, false) -> true;
normalize_opt(hll_b, I) ->
    true = is_integer(I) andalso I >= 4 andalso I =< 16, true;
normalize_opt(limit, infinity) -> true;
normalize_opt(limit, N) ->
    true = is_integer(N) andalso N > 0, true;
normalize_opt(mnesia_dir, D) ->
    true = filelib:is_dir(D),
    true = filelib:is_regular(filename:join(D, "schema.DAT")),
    true;
normalize_opt(procs, N) ->
    true = is_integer(N) andalso N > 0, true;
normalize_opt(progress, false) -> true;
normalize_opt(progress, P) ->
    true = is_number(P) andalso P > 0, true;
normalize_opt(rec_attrs, A) ->
    true = lists:member(A, [true, false, force]), true;
normalize_opt(samples, false) -> true;
normalize_opt(samples, infinity) ->
    io:format("** Setting samples to 'infinity' is dangerous and "
              "not recommended!\n"),
    true;
normalize_opt(samples, N) ->
    true = is_integer(N) andalso N > 0, true;
normalize_opt(term, T) when is_atom(T) -> {true, atom_to_list(T)};
normalize_opt(term, T) -> true = is_list(T), true;
normalize_opt(Key, _Value) ->
    io:format("** ignoring unknown option: ~p~n", [Key]),
    false.

%% getter for individual options handling defaults and special cases;
%% see table in opts() above
getopt(Opt) -> getopt(Opt, getopts()).

getopt(Opt, Opts) ->
    case proplists:get_value(Opt, Opts) of
        undefined -> element(2, lists:keyfind(Opt, 1, opts()));
        true      -> element(3, lists:keyfind(Opt, 1, opts()));
        Value     -> Value
    end.

help(Opt) ->
    case lists:keyfind(Opt, 1, opts()) of
        false ->
            lists:concat([Opt, ": unrecognized option\n"]);
        {Opt,_UndefVal,_NoVal, TypeStr, HelpStr} ->
            lists:concat([Opt, ": ", TypeStr, "\n", HelpStr])
    end.

%% Functions for dynamic defaults

cores() -> erlang:system_info(logical_processors).

%% Try to find out if we have a usable terminal or not.
%% All terminals tested to be usable are categorized as 'vt100',
%% since that is the compatibility baseline we rely on.
%% Otherwise we treat the terminal as 'dumb'.
term() ->
    case os:getenv("TERM") of
        "rxvt"        -> "vt100";
        "screen" ++ _ -> "vt100";
        "vt" ++ _     -> "vt100";
        "xterm" ++ _  -> "vt100";
        _             -> "dumb"
    end.

%% Tests
-ifdef(TEST).

undefined_test() ->
    [?assertEqual(UndefValue, getopt(Opt, [])) ||
        {Opt, UndefValue,_NoValue,_TypeStr,_HelpStr} <- opts()].

novalue_test() ->
    [?assertEqual(NoValue, getopt(Opt, [Opt])) ||
        {Opt,_UndefValue, NoValue,_TypeStr,_HelpStr} <- opts()].

unknown_option_test() ->
    ?assertError(badarg, getopt(no_such_option, [])),
    ?assertEqual([], normalize_opts([no_such_option])).

bad_value_test() ->
    %% A value that fails to validate is equivalent to supplying the
    %% option without a value.
    ?assertEqual([{rec_attrs, getopt(rec_attrs, [rec_attrs])}],
                 normalize_opts([{rec_attrs, {12345, invalid_value}}])),
    ?assertEqual([{mnesia_dir, getopt(mnesia_dir, [mnesia_dir])}],
                 normalize_opts([{mnesia_dir, this_is_not_a_dir}])).

normalize_opts_test() ->
    Opts = normalize_opts([ {samples, 100}
                          , dump
                          , {no, [such], <<"option">>}
                          , {term, vt100}
                          , hll_b
                          ]),
    ?assertEqual(100, getopt(samples, Opts)),
    ?assertEqual("ds.bin", getopt(dump, Opts)),
    ?assertEqual("vt100", getopt(term, Opts)),
    ?assertEqual(8, getopt(hll_b, Opts)).

-endif.
