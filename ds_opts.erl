%% Options handling for dataspec

-module(ds_opts).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ getopt/1
        , setopts/1
        ]).

%% dataspec operation can be controlled to some extent. Supported options:
%%
%% options influencing spec semantics:
%%
%%   mag: integer()
%%     subgrouping of numerals by magnitude
%%       0: turn off subgrouping;
%%       N: subgroup N orders of magnitude into one (defaults to 3)
%%
%%   samples: N (must be an integer power of 2)
%%       sample buffer size for collecting example data
%%
%%   strlen: true | false
%%     subgrouping of strings by length
%%
%% other options:
%%
%%   dump: filename() | false
%%     dump the accumulated spec on each progress tick;
%%     only works in case progress output is enabled.
%%       false: no dump is written
%%       Filename: the accumulated spec is dumped as an Erlang binary
%%         to this filename on each progress tick (dot or number).
%%         Defaults to "dataspec.bin" if option is set with no value.
%%
%%   progress: pos_integer() | false
%%     output progress information
%%       false: no output
%%       N: output progress info every N samples

opts() ->
    %% The following table specifies the options interpreted.
    %%
    %%      name: name of option
    %% undefined: value to use if option is missing
    %%   novalue: value to use if option is set to true
    %%            (implicit if the option is supplied with no value;
    %%             also used if supplied value fails to validate)

    %% name          undefined     novalue
    [ {dump,         false,        "dataspec.bin"}
    , {mag,          0,            3}
    , {progress,     false,        100000}
    , {samples,      16,           16}
    , {strlen,       false,        true}
    ].

%% NB. using the process dict is ugly; passing Opts around is uglier.
setopts(Opts) -> put(dataspec_opts, normalize_opts(Opts)).

getopts() ->
    case get(dataspec_opts) of
        undefined -> [];
        Opts      -> Opts
    end.


normalize_opts(Opts) -> [do_normalize_opts(Opt, Opts) || Opt <- Opts].

do_normalize_opts(Opt, Opts) ->
    [Key] = proplists:get_keys([Opt]),
    Value0 = getopt(Key, Opts),
    Value = try normalize_opt(Key, Value0)
            catch _:_ -> default_opt(Key, Value0)
            end,
    proplists:property(Key, Value).

default_opt(Key, Value) ->
    Default = getopt(Key, [Key]),
    io:format("** option ~p: invalid value ~p; using default ~p~n",
              [Key, Value, Default]),
    Default.

%% Normalize (change) the value of options;
%% throw errors or exceptions if data is invalid.
normalize_opt(dump, S) ->
    true = is_list(S) andalso filelib:is_dir(filename:dirname(S)), S;
normalize_opt(mag, I) ->
    true = is_integer(I) andalso I >= 0, I;
normalize_opt(progress, P) ->
    true = (P =:= false) orelse (is_integer(P) andalso P > 0), P;
normalize_opt(samples, N) ->
    true = is_integer(N) andalso is_power_of_2(N), N;
normalize_opt(strlen, B) ->
    true = is_boolean(B), B;
normalize_opt(_Key, Value) -> Value.

is_power_of_2(2) -> true;
is_power_of_2(N) when N rem 2 =:= 0 -> is_power_of_2(N div 2);
is_power_of_2(_) -> false.

%% getter for individual options handling defaults and special cases;
%% see table in opts() above
getopt(Opt) -> getopt(Opt, getopts()).

getopt(Opt, Opts) ->
    case proplists:get_value(Opt, Opts) of
        undefined -> element(2, lists:keyfind(Opt, 1, opts()));
        true      -> element(3, lists:keyfind(Opt, 1, opts()));
        Value     -> Value
    end.
