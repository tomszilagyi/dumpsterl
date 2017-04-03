%% Progress indicator and result dumper for dumpsterl
-module(ds_progress).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ init/0
        , init/1
        , get_count/1
        , update/3
        , final/2
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Minimum interval between dumps in seconds to prevent excessive disk usage
-define(DUMP_INTERVAL_MIN, 10).

%% Progress state
-record(progress, { interval = false % number | false (disable output)
                  , count = 0        % count of received items
                  , start_ts         % timestamp when progress was initialized
                  , end_ts           % latest (eventually, last) timestamp
                  , next_tick_count  % next count when progress will be printed
                  , last_dump_ts     % last timestamp when dump was written
                  }).

init() -> init(ds_opts:getopt(progress)).

init(false) -> #progress{};
init(Interval) ->
    io:fwrite("\n"),
    StartTS = os:timestamp(),
    #progress{interval=Interval, start_ts=StartTS, end_ts=StartTS,
              last_dump_ts=StartTS, next_tick_count=1}.

get_count(#progress{count=Count}) -> Count.

%% output progress information if configured to do so
update(#progress{interval=false}=P, _Acc, _Incr) -> P;
update(#progress{count=Count, next_tick_count=NextTickCount}=P,_Acc, Incr)
  when Count < NextTickCount ->
    P#progress{count = Count + Incr};
update(#progress{interval=Interval, start_ts=StartTS, last_dump_ts=LastDumpTS0,
                 count=Count0}=P,
       Acc, Incr) ->

    Count = Count0 + Incr,
    EndTS = os:timestamp(),
    DeltaT = max(time_diff(StartTS, EndTS), 0.000001),
    %% estimate the count at the next progress update
    NextTickCount = Count * (1 + Interval / DeltaT),

    print_status(Count, StartTS, EndTS),

    %% do not dump more frequently than once every ten seconds
    DeltaSinceLastDump = time_diff(LastDumpTS0, EndTS),
    LastDumpTS = if DeltaSinceLastDump > ?DUMP_INTERVAL_MIN ->
                         dump_acc(Acc),
                         EndTS;
                    true -> LastDumpTS0
                 end,

    P#progress{count=Count, end_ts=EndTS, last_dump_ts=LastDumpTS,
               next_tick_count=NextTickCount}.

final(#progress{interval=false}, Acc) ->
    dump_acc(Acc, true),
    Acc;
final(#progress{count=Count, start_ts=StartTS}, Acc) ->
    EndTS = os:timestamp(),
    print_status(Count, StartTS, EndTS),
    dump_acc(Acc, true),
    Acc.

%% dump the accumulated spec if dumpsterl is configured to do dumps
dump_acc(Acc) -> dump_acc(Acc, false).

dump_acc(Acc, Verbose) ->
    case ds_opts:getopt(dump) of
        false    -> ok;
        Filename ->
            ok = file:write_file(Filename, erlang:term_to_binary(Acc)),
            if Verbose ->
                    io:format("spec dump written to ~s~n", [Filename]);
               true -> ok
            end
    end.

print_status(Count, StartTS, EndTS) ->
    DeltaT = time_diff(StartTS, EndTS),
    CountStr = integer_to_sigfig(Count),
    TimeStr = time_str(DeltaT),
    SpeedStr = speed_str(DeltaT, Count),
    Status = io_lib:format("processed ~s in ~s (~s records/sec)",
                           [CountStr, TimeStr, SpeedStr]),
    ds_shell:set_statusline(Status).

%% return an iolist of the integer printed with significant figures separated by commas
integer_to_sigfig(N) ->
    integer_to_sigfig(lists:reverse(integer_to_list(N)), "").

integer_to_sigfig([], Acc) -> Acc;
integer_to_sigfig([A,B,C|Rest], []) -> integer_to_sigfig(Rest, [C,B,A]);
integer_to_sigfig([A,B,C|Rest], Acc) -> integer_to_sigfig(Rest, [C,B,A,$,|Acc]);
integer_to_sigfig(R, []) -> lists:reverse(R);
integer_to_sigfig(R, Acc) -> [lists:reverse(R),$,|Acc].

%% return a string specifying elapsed time
time_str(FloatSecs) ->
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


%% return a string specifying records per second
speed_str(0.0, _Count) -> "N/A";
speed_str(DeltaT, Count) ->
    Speed = Count / DeltaT,
    case Speed of
        N when N < 1000 -> io_lib:format("~.2f", [N]);
        N when N < 1000000 -> io_lib:format("~.2fk", [N / 1000.0]);
        N -> io_lib:format("~.2fM", [N / 1000000.0])
    end.

time_diff({StartMegaSecs, StartSecs, StartUSecs},
          {EndMegaSecs, EndSecs, EndUSecs}) ->
    (EndMegaSecs - StartMegaSecs) * 1000000
        + EndSecs - StartSecs
        + (EndUSecs - StartUSecs) / 1.0E6.


%% Tests
-ifdef(TEST).

integer_to_sigfigs_test() ->
    ?assertEqual("12", lists:flatten(integer_to_sigfig(12))),
    ?assertEqual("123", lists:flatten(integer_to_sigfig(123))),
    ?assertEqual("1,034", lists:flatten(integer_to_sigfig(1034))),
    ?assertEqual("12,345", lists:flatten(integer_to_sigfig(12345))),
    ?assertEqual("123,006", lists:flatten(integer_to_sigfig(123006))),
    ?assertEqual("1,034,567", lists:flatten(integer_to_sigfig(1034567))),
    ?assertEqual("12,345,678", lists:flatten(integer_to_sigfig(12345678))),
    ?assertEqual("123,456,789", lists:flatten(integer_to_sigfig(123456789))),
    ?assertEqual("1,234,567,890", lists:flatten(integer_to_sigfig(1234567890))),
    ok.

time_str_test() ->
    ?assertEqual("6.1",
                 lists:flatten(time_str(6.125))),
    ?assertEqual("4:06.1",
                 lists:flatten(time_str(4 * 60 + 6.125))),
    ?assertEqual("2:04:06.1",
                 lists:flatten(time_str((2 * 60 + 4) * 60 + 6.125))),
    ?assertEqual("1 day, 02:04:06.1",
                 lists:flatten(time_str(((1 * 24 +  2) * 60 + 4) * 60 + 6.125))),
    ?assertEqual("5 days, 12:34:56.1",
                 lists:flatten(time_str(((5 * 24 + 12) * 60 + 34) * 60 + 56.125))).

-endif.
