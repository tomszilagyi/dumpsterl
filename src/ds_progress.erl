%% Progress indicator and result dumper for dumpsterl
-module(ds_progress).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ init/0
        , get_count/1
        , update/3
        , final/2
        ]).

%% Progress state
-record(progress, { major = 50       % one major tick = this many minor ticks
                  , minor = false    % integer | false (disable output)
                  , count_recv  = 0  % count of received items
                  , count_minor = 0  % number of minor ticks so far
                  , count_major = 0  % number of major ticks so far
                  , start_ts         % timestamp when initializing progress
                  }).

init() -> init(ds_opts:getopt(progress)).

init(false) -> #progress{};
init(Minor) ->
    io:format("progress (every ~B): ", [Minor]),
    #progress{minor=Minor, start_ts=os:timestamp()}.

get_count(#progress{count_recv=Count}) -> Count.

%% output progress information if configured to do so
update(#progress{minor=false}=P, _Acc, _Incr) -> P;
update(#progress{major=Major, minor=Minor,
                 count_minor=CMi0, count_major=CMa0, count_recv=Count0}=P,
       Acc, Incr) ->

    %% minor ticks
    Count = Count0 + Incr,
    CMi = Count div Minor,
    NewTicks = CMi - CMi0,
    emit_ticks(NewTicks, Acc),

    %% major ticks
    CMa = CMi div Major,
    if CMa > CMa0 -> io:format("~B", [Count]);
       true -> ok
    end,

    P#progress{count_recv=Count, count_minor=CMi, count_major=CMa}.

emit_ticks(0,_Acc) -> ok;
emit_ticks(N, Acc) -> dump_acc(Acc),
                      io:put_chars(string:chars($., N)).

final(#progress{minor=false}, Acc) ->
    dump_acc(Acc),
    Acc;
final(#progress{count_recv=Count, start_ts=StartTS}, Acc) ->
    dump_acc(Acc),
    {StartMegaSecs, StartSecs, StartUSecs} = StartTS,
    {EndMegaSecs, EndSecs, EndUSecs} = os:timestamp(),
    DeltaT = (EndMegaSecs - StartMegaSecs) * 1000000 +
        EndSecs - StartSecs + (EndUSecs - StartUSecs) / 1.0E6,
    Speed = case DeltaT of
                0.0 -> inf;
                _ -> Count / DeltaT
            end,
    SpeedStr =
        case Speed of
            inf -> "N/A";
            N when N < 1000 -> io_lib:format("~.2f", [N]);
            N when N < 1000000 -> io_lib:format("~.2fk", [N / 1000.0]);
            N -> io_lib:format("~.2fM", [N / 1000000.0])
        end,
    io:format("~nprocessed: ~B in ~.2f seconds (~s records/sec)~n~n",
              [Count, DeltaT, SpeedStr]),
    Acc.

%% dump the accumulated spec if dumpsterl is configured to do dumps
dump_acc(Acc) ->
    case ds_opts:getopt(dump) of
        false    -> ok;
        Filename -> ok = file:write_file(Filename, erlang:term_to_binary(Acc))
    end.
