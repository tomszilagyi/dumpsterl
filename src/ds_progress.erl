%% -*- coding: utf-8 -*-
-module(ds_progress).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% Progress indicator and result dumper for dumpsterl

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
                  , last_dump_size   % last written dump size in bytes
                  }).

init() -> init(ds_opts:getopt(progress)).

init(false) -> #progress{start_ts = os:timestamp()};
init(Interval) ->
    io:fwrite("\n"),
    StartTS = os:timestamp(),
    #progress{interval=Interval, start_ts=StartTS, end_ts=StartTS,
              last_dump_ts=StartTS, next_tick_count=1}.

get_count(#progress{count=Count}) -> Count.

%% output progress information if configured to do so
update(#progress{interval=false, count=Count}=P, _Acc, Incr) ->
    P#progress{count = Count + Incr};
update(#progress{count=Count, next_tick_count=NextTickCount}=P,_Acc, Incr)
  when Count < NextTickCount ->
    P#progress{count = Count + Incr};
update(#progress{interval=Interval, start_ts=StartTS, last_dump_ts=LastDumpTS0,
                 last_dump_size=LastDumpSize0, count=Count0}=P0,
       Acc, Incr) ->

    Count = Count0 + Incr,
    EndTS = os:timestamp(),
    DeltaT = max(time_diff(StartTS, EndTS), 0.000001),
    %% estimate the count at the next progress update
    NextTickCount = Count * (1 + Interval / DeltaT),
    P = P0#progress{count=Count, end_ts=EndTS, next_tick_count=NextTickCount},

    %% do not dump more frequently than once every ten seconds
    DeltaSinceLastDump = time_diff(LastDumpTS0, EndTS),
    {LastDumpSize, LastDumpTS} =
        if DeltaSinceLastDump > ?DUMP_INTERVAL_MIN ->
                {_AccWithMeta, DumpSize} = dump_acc(P, Acc),
                {DumpSize, EndTS};
           true ->
                {LastDumpSize0, LastDumpTS0}
        end,

    print_status(Count, StartTS, EndTS, LastDumpSize),
    P#progress{last_dump_ts=LastDumpTS, last_dump_size=LastDumpSize}.

final(#progress{count=Count, start_ts=StartTS, last_dump_size=LastDumpSize}=P,
      Acc0) ->
    EndTS = os:timestamp(),
    print_status(Count, StartTS, EndTS, LastDumpSize),
    {Acc, _DumpSize} = dump_acc(P#progress{end_ts=EndTS}, Acc0, true),
    Acc.

%% Dump the accumulated spec if dumpsterl is configured to do dumps.
%% Return a tuple {Spec, DumpSize} with the spec enriched with probe metadata,
%% and the size of the dump in bytes, or undefined.
dump_acc(Progress, Acc) -> dump_acc(Progress, Acc, false).

dump_acc(Progress, Acc0, Verbose) ->
    case ds_opts:getopt(dump) of
        false    -> {Acc0, undefined};
        Filename ->
            Acc = add_metadata(Progress, Acc0),
            Binary = erlang:term_to_binary(Acc),
            DumpSize = size(Binary),
            ok = file:write_file(Filename, Binary),
            if Verbose ->
                    io:format("spec dump: ~s bytes written to ~s~n",
                              [ds_utils:integer_to_sigfig(DumpSize), Filename]);
               true -> ok
            end,
            {Acc, DumpSize}
    end.

%% Save metadata about the probe run into the toplevel node's extra data.
add_metadata(#progress{count = Count, start_ts = StartTS, end_ts = EndTS},
             {Class, {Stats, Ext}, Children}) ->
    Meta = [ {processed, Count}
           , {node, node()}
           , {options, ds_opts:getopts_all()}
           , {start_ts, StartTS}
           , {end_ts, EndTS}
           ],
    {Class, {Stats, [{meta, Meta} | Ext]}, Children}.

print_status(Count, StartTS, EndTS, LastDumpSize) ->
    DeltaT = time_diff(StartTS, EndTS),
    CountStr = ds_utils:integer_to_sigfig(Count),
    TimeDiffStr = ds_utils:timediff_str(DeltaT),
    SpeedStr = speed_str(DeltaT, Count),
    LastDumpStr = dump_size_str(LastDumpSize),
    Status = io_lib:format("processed ~s in ~s (~s records/sec)~s",
                           [CountStr, TimeDiffStr, SpeedStr, LastDumpStr]),
    ds_shell:set_statusline(Status).

dump_size_str(undefined) -> "";
dump_size_str(N) ->
    lists:concat([", dumped ", ds_utils:integer_to_sigfig(N), " bytes"]).

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

-endif.
