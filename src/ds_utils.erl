%% -*- coding: utf-8 -*-

%% @private
-module(ds_utils).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ common_prefix/2
        , cut_cwd/1
        , convert_timestamp/2
        , decode_timestamp/1
        , hash/1
        , index/2
        , integer_to_sigfig/1
        , join/2
        , timediff_str/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Longest common prefix of two strings
common_prefix(S1, S2) ->
    common_prefix(S1, S2, "").

common_prefix([C|T1], [C|T2], Acc) ->
    common_prefix(T1, T2, [C|Acc]);
common_prefix(_S1,_S2, Acc) ->
    lists:reverse(Acc).


%% If Filename is an absolute path that starts with the CWD,
%% return a relative (to CWD) version of it.
%% In any other case, return Filename as is.
cut_cwd(Filename) ->
    case filelib:is_file(Filename) of
        false -> Filename;
        true  ->
            case filename:pathtype(Filename) of
                absolute -> do_cut_cwd(Filename);
                _ -> Filename
            end
    end.

do_cut_cwd(AbsFilename) ->
    {ok, Cwd} = file:get_cwd(),
    case string:str(AbsFilename, Cwd) of
        1 -> % Cwd is a prefix of AbsFilename
            AbsComps = filename:split(AbsFilename),
            AbsCwd = filename:split(Cwd),
            do_cut_cwd(AbsComps, AbsCwd);
        _ ->
            AbsFilename
    end.

do_cut_cwd(AbsComps, []) -> filename:join(AbsComps);
do_cut_cwd([Comp|RestAbs], [Comp|RestCwd]) -> do_cut_cwd(RestAbs, RestCwd).


%% UNIX epoch in gregorian seconds
%% calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}})
-define(EPOCH, 62167219200).

%% Arbitrarily chosen end timestamp to limit integer formats
%% calendar:datetime_to_gregorian_seconds({{2100,1,1},{0,0,0}})
-define(GDATE_END, 66269664000).

%% Decode a timestamp from any of the plausible data formats into the
%% standard internal format (gregorian seconds).
%% Return 'undefined' if the supplied data is not recognized as a timestamp.
decode_timestamp(GSecs)
  when is_integer(GSecs), GSecs >= ?EPOCH, GSecs =< ?GDATE_END ->
    GSecs;
decode_timestamp(GMilliSecs)
  when is_integer(GMilliSecs),
       GMilliSecs >= 1000 * ?EPOCH,
       GMilliSecs =< 1000 * ?GDATE_END ->
    GMilliSecs div 1000;
decode_timestamp(UnixTimestamp)
  when is_integer(UnixTimestamp),
       UnixTimestamp >= 0, UnixTimestamp < ?GDATE_END - ?EPOCH ->
    UnixTimestamp + ?EPOCH;
decode_timestamp({Year, Month, Day} = Date)
  when Year >= 1900, Year < 2100,
       Month >= 1, Month =< 12,
       Day >= 1, Day =< 31 ->
    calendar:datetime_to_gregorian_seconds({Date, {0,0,0}});
decode_timestamp({{Year, Month, Day}, {Hour, Minute, Second}} = DateTime)
  when Year >= 1900, Year < 2100,
       Month >= 1, Month =< 12,
       Day >= 1, Day =< 31,
       Hour >= 0, Hour < 24,
       Minute >= 0, Minute < 60,
       Second >= 0, Second < 60 ->
    calendar:datetime_to_gregorian_seconds(DateTime);
decode_timestamp({MegaSecs, Secs, MicroSecs} = Now)
  when MegaSecs >= 0, MegaSecs =< (?GDATE_END - ?EPOCH) div 1000000,
       Secs >= 0, Secs < 1000000,
       MicroSecs >= 0, MicroSecs < 1000000 ->
    DateTime = calendar:now_to_datetime(Now),
    calendar:datetime_to_gregorian_seconds(DateTime);
decode_timestamp(_UnknownFormat) ->
    undefined.


%% Convert the internal timestamp format to one of the standard
%% external formats:
%%      gsecs: gregorian seconds
%%     gmsecs: gregorian milliseconds
%%       unix: unix timestamp
%%       date: {Year, Month, Day}
%%   datetime: {{Year, Month, Day}, {Hour, Minute, Seconds}}
%%        now: Erlang now() / os:timestamp() compatible
%%     string: YYYY-MM-DD  or  YYYY-MM-DD HH:MM:SS
%% string_pad: like string, but padded to match the widest variant
convert_timestamp(undefined, _) -> undefined;
convert_timestamp(GSecs, gsecs) -> GSecs;
convert_timestamp(GSecs, gmsecs) -> 1000 * GSecs;
convert_timestamp(GSecs, unix) -> max(0, GSecs - ?EPOCH);
convert_timestamp(GSecs, date) ->
    {Date,_Time} = calendar:gregorian_seconds_to_datetime(GSecs),
    Date;
convert_timestamp(GSecs, datetime) ->
    calendar:gregorian_seconds_to_datetime(GSecs);
convert_timestamp(GSecs, now) ->
    UnixTS = GSecs - ?EPOCH,
    MegaSecs = UnixTS div 1000000,
    Secs = UnixTS rem 1000000,
    {MegaSecs, Secs, 0};
convert_timestamp(GSecs, string) ->
    case calendar:gregorian_seconds_to_datetime(GSecs) of
        {{Y,Mo,D}, {0,0,0}} ->
            io_lib:format("~4..0B-~2..0B-~2..0B", [Y,Mo,D]);
        {{Y,Mo,D}, {H,M,0}} ->
            io_lib:format("~4..0B-~2..0B-~2..0B ~2..0B:~2..0B",
                          [Y,Mo,D, H,M]);
        {{Y,Mo,D}, {H,M,S}} ->
            io_lib:format("~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B",
                          [Y,Mo,D, H,M,S])
    end.


%% Hash function used by the statistics modules.
%% Note: the hash value must be truncated to 32 bits.
hash(Term) ->
    <<Hash:32, _/binary>> = crypto:hash(md4, term_to_binary(Term)),
    Hash.


%% Return the first index N so that lists:nth(N, List) would return E
%% or 'error' if E is not a member of List.
index(E, List) -> index(E, List, 1).

index(_E, [],_N) -> error;
index(E, [E|_], N) -> N;
index(E, [_|Rest], N) -> index(E, Rest, N+1).


%% return an iolist of the integer printed with significant figures separated by commas
integer_to_sigfig(N) when is_integer(N) ->
    integer_to_sigfig(lists:reverse(integer_to_list(N)), "").

integer_to_sigfig([], Acc) -> Acc;
integer_to_sigfig([A,B,C|Rest], []) -> integer_to_sigfig(Rest, [C,B,A]);
integer_to_sigfig([A,B,C|Rest], Acc) -> integer_to_sigfig(Rest, [C,B,A,$,|Acc]);
integer_to_sigfig(R, []) -> lists:reverse(R);
integer_to_sigfig(R, Acc) -> [lists:reverse(R),$,|Acc].


%% lists:join implementation (so we can use it also before R19)
join(_Sep, []) -> [];
join(Sep, [H|T]) -> join(Sep, T, [H]).

join(_Sep, [], Acc) -> lists:reverse(Acc);
join(Sep, [H|T], Acc) -> join(Sep, T, [H,Sep|Acc]).


%% return a string specifying elapsed time
timediff_str(FloatSecs) ->
    IntSecs = trunc(FloatSecs),
    FracSecs = FloatSecs - IntSecs,
    S = IntSecs rem 60,
    M0 = IntSecs div 60,
    M = M0 rem 60,
    H0 = M0 div 60,
    H = H0 rem 24,
    D = H0 div 24,
    case {D, H, M, S+FracSecs} of
        {0, 0, 0, Sf} -> io_lib:format("~.1f", [Sf]);
        {0, 0, M, Sf} -> io_lib:format("~B:~4.1.0f", [M, Sf]);
        {0, H, M, Sf} -> io_lib:format("~B:~2..0B:~4.1.0f", [H, M, Sf]);
        {1, H, M, Sf} -> io_lib:format("~B day, ~2..0B:~2..0B:~4.1.0f", [D, H, M, Sf]);
        {D, H, M, Sf} -> io_lib:format("~B days, ~2..0B:~2..0B:~4.1.0f", [D, H, M, Sf])
    end.

%% Tests
-ifdef(TEST).

common_prefix_test() ->
    ?assertEqual("", common_prefix("", "")),
    ?assertEqual("", common_prefix("abc", "")),
    ?assertEqual("", common_prefix("abc", "def")),
    ?assertEqual("p", common_prefix("pabc", "pdef")),
    ?assertEqual("pr", common_prefix("prefabc", "pr")),
    ?assertEqual("pref", common_prefix("prefabc", "prefdef")).

cut_cwd_test() ->
    {ok, Cwd} = file:get_cwd(),
    Rel = filename:join([doc, guide]),
    Abs = filename:join(Cwd, Rel),
    ?assertEqual(Rel, cut_cwd(Abs)),
    ?assertEqual(Rel, cut_cwd(Rel)),
    RelNoexist = filename:join([this, does, 'not', exist]),
    AbsNoexist = filename:join(Cwd, RelNoexist),
    ?assertEqual(AbsNoexist, cut_cwd(AbsNoexist)).

convert_timestamp_test() ->
    F = fun lists:flatten/1,
    ?assertEqual(62962922096,              convert_timestamp(62962922096, gsecs)),
    ?assertEqual(62962922096000,           convert_timestamp(62962922096, gmsecs)),
    ?assertEqual(795702896,                convert_timestamp(62962922096, unix)),
    ?assertEqual({795, 702896, 0},         convert_timestamp(62962922096, now)),
    ?assertEqual({{1995,3,20},{12,34,56}}, convert_timestamp(62962922096, datetime)),
    ?assertEqual({1995,3,20},              convert_timestamp(62962922096, date)),
    ?assertEqual("1995-03-20 12:34:56",  F(convert_timestamp(62962922096, string))),
    ?assertEqual("1995-03-20 12:34",     F(convert_timestamp(62962922040, string))),
    ?assertEqual("1995-03-20",           F(convert_timestamp(62962876800, string))),
    ?assertEqual(undefined,                convert_timestamp(undefined, gsecs)).

decode_timestamp_test() ->
    ?assertEqual(62962922096, decode_timestamp(62962922096)),
    ?assertEqual(62962922096, decode_timestamp(62962922096000)),
    ?assertEqual(62962922096, decode_timestamp(795702896)),
    ?assertEqual(62962922096, decode_timestamp({795, 702896, 123456})),
    ?assertEqual(62962922096, decode_timestamp({{1995,3,20},{12,34,56}})),
    ?assertEqual(62962876800, decode_timestamp({1995,3,20})),
    ?assertEqual(undefined,   decode_timestamp({what, is, this})).

index_test() ->
    ?assertEqual(error, index(aaa, [])),
    ?assertEqual(error, index(aaa, [a, "bbb", {c,999}])),
    ?assertEqual(1, index(a, [a, "bbb", {c,999}])),
    ?assertEqual(2, index("bbb", [a, "bbb", {c,999}])),
    ?assertEqual(3, index({c,999}, [a, "bbb", {c,999}])).

integer_to_sigfig_test() ->
    ?assertEqual(           "12", lists:flatten(integer_to_sigfig(12))),
    ?assertEqual(          "123", lists:flatten(integer_to_sigfig(123))),
    ?assertEqual(        "1,034", lists:flatten(integer_to_sigfig(1034))),
    ?assertEqual(       "12,345", lists:flatten(integer_to_sigfig(12345))),
    ?assertEqual(      "123,006", lists:flatten(integer_to_sigfig(123006))),
    ?assertEqual(    "1,034,567", lists:flatten(integer_to_sigfig(1034567))),
    ?assertEqual(   "12,345,678", lists:flatten(integer_to_sigfig(12345678))),
    ?assertEqual(  "123,456,789", lists:flatten(integer_to_sigfig(123456789))),
    ?assertEqual("1,234,567,890", lists:flatten(integer_to_sigfig(1234567890))).

join_test() ->
    ?assertEqual([], join(x, [])),
    ?assertEqual([a], join(x, [a])),
    ?assertEqual([a,x,b], join(x, [a,b])),
    ?assertEqual([a,x,b,x,c], join(x, [a,b,c])).

timediff_str_test() ->
    ?assertEqual("6.1",
                 lists:flatten(timediff_str(6.125))),
    ?assertEqual("4:06.1",
                 lists:flatten(timediff_str(4 * 60 + 6.125))),
    ?assertEqual("2:04:06.1",
                 lists:flatten(timediff_str((2 * 60 + 4) * 60 + 6.125))),
    ?assertEqual("1 day, 02:04:06.1",
                 lists:flatten(timediff_str(((1 * 24 +  2) * 60 + 4) * 60 + 6.125))),
    ?assertEqual("5 days, 12:34:56.1",
                 lists:flatten(timediff_str(((5 * 24 + 12) * 60 + 34) * 60 + 56.125))).

-endif.
