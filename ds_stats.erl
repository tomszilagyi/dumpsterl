-module(ds_stats).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ stats_data/0
        , stats_data/2
        , sample_data/0
        , sample_data/1
        , sample_data/2
        , get_count/1
        , get_samples/1
        , pvs_new/1
        , pvs_add/2
        , join/2
        ]).

-import(ds_opts, [ getopt/1
                 , setopts/1
                 ]).

-include_lib("eunit/include/eunit.hrl").

%% general per-node statistics data
-record(stats, { count = 0,
                 min_pvs,
                 max_pvs,
                 sample_data = sample_data()
               }).

stats_data() -> #stats{}.

stats_data(V, #stats{count = Count,
                     min_pvs = MinPVS,
                     max_pvs = MaxPVS,
                     sample_data = SD}) ->
    #stats{count = Count+1,
           min_pvs = update_min(V, MinPVS),
           max_pvs = update_max(V, MaxPVS),
           sample_data = sample_data(V, SD)}.

get_count(#stats{count=Count}) -> Count.

%% Join two statistics into one
join(#stats{count = Count0,
            min_pvs = MinPVS0,
            max_pvs = MaxPVS0,
            sample_data = SD0},
     #stats{count = Count1,
            min_pvs = MinPVS1,
            max_pvs = MaxPVS1,
            sample_data = SD1}) ->
    #stats{count = Count0 + Count1,
           min_pvs = join_pvs(MinPVS0, MinPVS1),
           max_pvs = join_pvs(MaxPVS0, MaxPVS1),
           sample_data = join_sample_data(SD0, SD1)}.

join_pvs(undefined, PVS) -> PVS;
join_pvs(PVS, undefined) -> PVS;

join_pvs({min, Min, [{count, Count0}, {timespan, TSpMin0, TSpMax0}]},
         {min, Min, [{count, Count1}, {timespan, TSpMin1, TSpMax1}]}) ->
    {min, Min, [{count, Count0+Count1},
                {timespan, min(TSpMin0, TSpMin1), max(TSpMax0, TSpMax1)}]};
join_pvs({min, M0, PVS0}, {min, M1,_PVS1}) when M0 < M1 -> {min, M0, PVS0};
join_pvs({min, M0,_PVS0}, {min, M1, PVS1}) when M0 > M1 -> {min, M1, PVS1};

join_pvs({max, Max, [{count, Count0}, {timespan, TSpMin0, TSpMax0}]},
         {max, Max, [{count, Count1}, {timespan, TSpMin1, TSpMax1}]}) ->
    {max, Max, [{count, Count0+Count1},
                {timespan, min(TSpMin0, TSpMin1), max(TSpMax0, TSpMax1)}]};
join_pvs({max, M0, PVS0}, {max, M1,_PVS1}) when M0 > M1 -> {max, M0, PVS0};
join_pvs({max, M0,_PVS0}, {max, M1, PVS1}) when M0 < M1 -> {max, M1, PVS1}.


update_min({V, Attrs}, undefined) ->
    {min, V, pvs_new({V, Attrs})};
update_min({V, Attrs}, {min, Min,_MinPVS0}) when V < Min ->
    {min, V, pvs_new({V, Attrs})};
update_min({V, Attrs}, {min, Min, MinPVS0}) when V =:= Min ->
    {min, V, pvs_add({V, Attrs}, MinPVS0)};
update_min({_V,_Attrs}, MinStats) ->
    MinStats.

update_max({V, Attrs}, undefined) ->
    {max, V, pvs_new({V, Attrs})};
update_max({V, Attrs}, {max, Max,_MaxPVS0}) when V > Max ->
    {max, V, pvs_new({V, Attrs})};
update_max({V, Attrs}, {max, Max, MaxPVS0}) when V =:= Max ->
    {max, V, pvs_add({V, Attrs}, MaxPVS0)};
update_max({_V,_Attrs}, MaxStats) ->
    MaxStats.

%% General per-value statistics keeping track of
%% - count of values seen
%% - approx count of unique values (hyperloglog?)
%% - time span of values annotated with keys

%% Per-Value Stats:
%%   for a given value of interest (eg. min, max), maintain:
%%   [{count, Count}, {timespan, {{Ts_Min, Key}, {Ts_Max, Key}}}]
pvs_new({_V, Attrs}) ->
    Ts = proplists:get_value(ts, Attrs),
    Key = proplists:get_value(key, Attrs),
    [ {count, 1}
    , {timespan, {Ts, Key}, {Ts, Key}}
    ].

pvs_add({_V, Attrs}, [{count, Count}, {timespan, TSpMin0, TSpMax0}]) ->
    Ts = proplists:get_value(ts, Attrs),
    Key = proplists:get_value(key, Attrs),
    [ {count, Count+1}
    , {timespan, min(TSpMin0, {Ts, Key}), max(TSpMax0, {Ts, Key})}
    ].


%% sample_data:
%%
%% Aim: as a (potentially unlimited, unknown length) stream of samples
%% comes in, retain a limited selection of samples taken uniformly
%% across the already received (finite) part of the stream. We want to
%% do this in a computationally cheap manner.
%%
%% Algorithm: we have a certain capacity of 2^N samples in sample_data,
%% configurable with the option 'samples'. When this buffer is filled,
%% we throw every other element away.  From then on, we put only every
%% other incoming sample in the list.  When sample_data is filled
%% again, we again throw every other away.  From then on, only every
%% fourth incoming sample is put in the list, etc.

sample_data() ->
    %% divisor, n_received, size, capacity, sample_data
    {0, 0, 0, getopt(samples), []}.

sample_data(Data) ->
    %% divisor, n_received, size, capacity, sample_data
    {0, 1, 1, getopt(samples), [Data]}.

sample_data(_Data, {Div, N, Size, Cap, Samples}) when (N+1) band Div =/= 0 ->
    {Div, N+1, Size, Cap, Samples};
sample_data(_Data, {Div, N, Size, Cap, Samples}) when Size =:= Cap ->
    {Div bsl 1 + 1, N+1, Size bsr 1, Cap, drop2(Samples)};
sample_data(Data, {Div, N, Size, Cap, Samples}) ->
    {Div, N+1, Size+1, Cap, [Data|Samples]}.

drop2(L) -> drop2(L, false, []).

drop2([],_DropThis, Acc) -> lists:reverse(Acc);
drop2([E|R], false, Acc)  -> drop2(R, true, [E|Acc]);
drop2([_E|R], true, Acc)  -> drop2(R, false, Acc).

join_sample_data(SD0, SD1) ->
    S0 = get_samples(SD0),
    S1 = get_samples(SD1),
    %% TODO we only care about the sample count and list of samples.
    {0, length(S0)+length(S1), 0, 0, S0++S1}.

%% get the list of samples collected from the sample_data tuple
get_samples({_Div,_N,_Size,_Cap, Samples}) -> Samples.


%% Tests

drop2_test() ->
    ?assertEqual([],        drop2([])),
    ?assertEqual([1],       drop2([1])),
    ?assertEqual([1],       drop2([1, 2])),
    ?assertEqual([1, 3],    drop2([1, 2, 3])),
    ?assertEqual([1, 3],    drop2([1, 2, 3, 4])),
    ?assertEqual([1, 3, 5], drop2([1, 2, 3, 4, 5])),
    ?assertEqual([1, 3, 5], drop2([1, 2, 3, 4, 5, 6])).

sample_data_test() ->
    setopts([{samples, 8}]),
    SD10 = sample_data(1),
    ?assertEqual(SD10, sample_data(1, sample_data())),
    SD11 = lists:foldl(fun sample_data/2, SD10, lists:seq(2, 8)),
    ?assertEqual({0,8,8,8,[8,7,6,5,4,3,2,1]}, SD11),
    SD12 = lists:foldl(fun sample_data/2, SD11, lists:seq(9, 16)),
    ?assertEqual({1,16,8,8,[16,14,12,10,8,6,4,2]}, SD12),
    SD13 = lists:foldl(fun sample_data/2, SD12, lists:seq(17, 32)),
    ?assertEqual({3,32,8,8,[32,28,24,20,16,12,8,4]}, SD13),
    SD14 = lists:foldl(fun sample_data/2, SD13, lists:seq(33, 64)),
    ?assertEqual({7,64,8,8,[64,56,48,40,32,24,16,8]}, SD14),

    setopts([{samples, 16}]),
    SD20 = sample_data(1),
    SD21 = lists:foldl(fun sample_data/2, SD20, lists:seq(2, 16)),
    ?assertEqual({0,16,16,16,[16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1]}, SD21),
    SD22 = lists:foldl(fun sample_data/2, SD21, lists:seq(17, 32)),
    ?assertEqual({1,32,16,16,[32,30,28,26,24,22,20,18,16,14,12,10,8,6,4,2]}, SD22),
    SD23 = lists:foldl(fun sample_data/2, SD22, lists:seq(33, 64)),
    ?assertEqual({3,64,16,16,[64,60,56,52,48,44,40,36,32,28,24,20,16,12,8,4]}, SD23),
    SD24 = lists:foldl(fun sample_data/2, SD23, lists:seq(65, 128)),
    ?assertEqual({7,128,16,16,[128,120,112,104,96,88,80,72,64,56,48,40,32,24,16,8]}, SD24),

    ?assertEqual([128,120,112,104,96,88,80,72,64,56,48,40,32,24,16,8], get_samples(SD24)).
