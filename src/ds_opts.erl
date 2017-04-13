%% -*- coding: utf-8 -*-
-module(ds_opts).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% Options handling for dumpsterl

-export([ keys/0
        , getopt/1
        , getopt/2
        , getopts_all/0
        , setopts/1
        , normalize_opts/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("config.hrl").

-ifdef(CONFIG_LISTS_FILTERMAP).
-define(filtermap(Fun, List), lists:filtermap(Fun, List)).
-else.
-define(filtermap(Fun, List), filtermap(Fun, List)).
filtermap(Fun, List) ->
    lists:foldr(fun(Elem, Acc) ->
                        case Fun(Elem) of
                            false -> Acc;
                            true -> [Elem|Acc];
                            {true, Value} -> [Value|Acc]
                        end
                end, [], List).
-endif.

%% dumpsterl operation can be controlled to some extent. Supported options:
%%
%% options influencing spec semantics:
%%
%%   hll_b : 'false' | integer() between 4 and 16
%%     parameter 'b' (bit size of substream count) of hyperloglog
%%     cardinality estimator
%%
%%   rec_attrs: 'true' | 'false' | 'force'
%%     collect record attributes and enrich the spec with those
%%       referenced in the data.
%%     - true:  collect data once at the beginning of first run
%%     - false: turn off completely
%%     - force: force (re)collection even if data has been collected;
%%              useful if code has been changed in the system
%%              NB. just including rec_attrs (with no value) in the
%%              options list is equivalent to {rec_attrs, force}.
%%
%%   samples: 'false' | N (positive integer) | 'infinity'
%%       Maximum number of samples to collect.
%%       NB. using 'infinity' is definitely not recommended, as the spec
%%       size will grow without bounds unless the number of processed
%%       records is limited to a small number!
%%
%% other options:
%%
%%   dump: filename() | 'false'
%%     dump the accumulated spec on each progress update, but at most
%%       once every ten seconds (if progress output is enabled),
%%       and at the end.
%%     - false: no dump is written
%%     - Filename: the accumulated spec is dumped as an Erlang binary
%%         to this filename on each progress update and when finished.
%%         Defaults to "ds.bin" if option is set with no value.
%%       NB. in case of parallel probe execution (see `procs` option), only
%%         the master process dumps its spec. With procs=N, only 1/N of the
%%         whole spec is periodically dumped. Naturally, the entire result
%%         is collected and dumped on completion (or ds-shell interruption).
%%
%%   limit: 'infinity' | pos_integer()
%%     Limit the number of records to process. In case reads are done in
%%     chunks of several records (such as with disk_logs), the limit might
%%     be slightly exceeded. Defaults to 1000, which is a safely low value.
%%
%%   mnesia_dir: dirname()
%%     The name of the mnesia directory where table data files are stored.
%%     This option is useful if the Erlang node running dumpsterl does not
%%     run a Mnesia instance where the tables being read belong, but has
%%     access to the database filesystem.
%%
%%   procs: pos_integer()
%%     Number of parallel processes to use when running the probe.
%%     Defaults to the number of logical processors as reported by ERTS.
%%
%%   progress: number() | false
%%     Output progress information and (if dump is enabled) write interim
%%       dumps.
%%     - false: no output
%%     - T: update progress info every T seconds
%%          (achieved update frequency is limited by read granularity)
%%
%%   term: atom() | string()
%%     terminal setting useful to override the $TERM environment variable
%%     (set it to 'dumb' to forcibly disable progress line rewrites)

opts() ->
    %% The following table specifies the options interpreted.
    %%
    %%      name: name of option
    %% undefined: value to use if option is missing
    %%   novalue: value to use if option is set to true
    %%            (implicit if the option is supplied with no value;
    %%             also used if supplied value fails to validate)

    %% name          undefined     novalue
    [ {dump,         false,        "ds.bin"}
    , {hll_b,        8,            8}
    , {limit,        1000,         1000}
    , {mnesia_dir,   undefined,    undefined}
    , {procs,        cores(),      cores()}
    , {progress,     false,        1}
    , {rec_attrs,    true,         force}
    , {samples,      16,           16}
    , {term,         term(),       "vt100"}
    ].

keys() -> [Opt || {Opt, _Undefined, _Novalue} <- opts()].

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
    ?filtermap(fun(Opt) -> normalize_opt_f(Opt, Opts) end, Opts).

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
    true = filelib:is_dir(D), true;
normalize_opt(procs, N) ->
    true = is_integer(N) andalso N > 0, true;
normalize_opt(progress, false) -> true;
normalize_opt(progress, P) ->
    true = is_number(P) andalso P > 0, true;
normalize_opt(rec_attrs, A) ->
    true = lists:member(A, [true, false, force]), true;
normalize_opt(samples, false) -> true;
normalize_opt(samples, infinity) -> true;
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
    [?assertEqual(UndefValue, getopt(Name, [])) ||
        {Name, UndefValue, _NoValue} <- opts()].

novalue_test() ->
    [?assertEqual(NoValue, getopt(Name, [Name])) ||
        {Name, _UndefValue, NoValue} <- opts()].

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
                          , {mnesia_dir, "."}
                          , {term, vt100}
                          , hll_b
                          ]),
    ?assertEqual(100, getopt(samples, Opts)),
    ?assertEqual("ds.bin", getopt(dump, Opts)),
    ?assertEqual(".", getopt(mnesia_dir, Opts)),
    ?assertEqual("vt100", getopt(term, Opts)),
    ?assertEqual(8, getopt(hll_b, Opts)).

-endif.
