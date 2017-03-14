-module(ds_stats).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ new/0
        , add/2
        , join/2
        , get_count/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% general per-node statistics data
-record(stats, { count = 0,
                 min_pvs,
                 max_pvs,
                 sampler,
                 hyperloglog
               }).

%% @doc API note:
%%
%% By convention, data structure modules need to handle the special
%% case of being disabled. In this case, their new/1 function receives
%% 'false' as argument and the atom 'undefined' must be returned.
%%
%% Other API functions add/2, join/2 etc.  must handle an 'undefined'
%% instance in an appropriate way (possibly returning 'undefined' as
%% the resulting version).

new() ->
    Sampler = ds_sampler:new(ds_opts:getopt(samples)),
    Hyperloglog = ds_hyperloglog:new(ds_opts:getopt(hll_b)),
    #stats{sampler = Sampler, hyperloglog = Hyperloglog}.

add({V,_A}=VA,
    #stats{count = Count,
           min_pvs = MinPVS,
           max_pvs = MaxPVS,
           sampler = Sampler,
           hyperloglog = Hyperloglog}) ->
    Hash = erlang:phash2(V, 1 bsl 32),
    #stats{count = Count+1,
           min_pvs = update_min(VA, MinPVS),
           max_pvs = update_max(VA, MaxPVS),
           sampler = ds_sampler:add_hash({Hash, VA}, Sampler),
           hyperloglog = ds_hyperloglog:add_hash(Hash, Hyperloglog)
          }.

get_count(#stats{count=Count}) -> Count.

%% Join two statistics into one
join(#stats{count = Count0,
            min_pvs = MinPVS0,
            max_pvs = MaxPVS0,
            sampler = Sampler0,
            hyperloglog = HLL0},
     #stats{count = Count1,
            min_pvs = MinPVS1,
            max_pvs = MaxPVS1,
            sampler = Sampler1,
            hyperloglog = HLL1}) ->
    #stats{count = Count0 + Count1,
           min_pvs = join_pvs(MinPVS0, MinPVS1),
           max_pvs = join_pvs(MaxPVS0, MaxPVS1),
           sampler = ds_sampler:join(Sampler0, Sampler1),
           hyperloglog = ds_hyperloglog:join(HLL0, HLL1)}.


join_pvs(undefined, PVS) -> PVS;
join_pvs(PVS, undefined) -> PVS;

join_pvs({min, M0, PVS0}, {min, M1, PVS1}) when M0 == M1 ->
    %% Use '==' in guard in case M0 and M1 are float and int with same value
    {min, M0, ds_pvattrs:join(PVS0, PVS1)};
join_pvs({min, M0, PVS0}, {min, M1,_PVS1}) when M0 < M1 -> {min, M0, PVS0};
join_pvs({min, M0,_PVS0}, {min, M1, PVS1}) when M0 > M1 -> {min, M1, PVS1};

join_pvs({max, M0, PVS0}, {max, M1, PVS1}) when M0 == M1 ->
    %% Use '==' in guard in case M0 and M1 are float and int with same value
    {max, M0, ds_pvattrs:join(PVS0, PVS1)};
join_pvs({max, M0, PVS0}, {max, M1,_PVS1}) when M0 > M1 -> {max, M0, PVS0};
join_pvs({max, M0,_PVS0}, {max, M1, PVS1}) when M0 < M1 -> {max, M1, PVS1}.


update_min({V, Attrs}, undefined) ->
    {min, V, ds_pvattrs:new(Attrs)};
update_min({V, Attrs}, {min, Min,_MinPVS0}) when V < Min ->
    {min, V, ds_pvattrs:new(Attrs)};
update_min({V, Attrs}, {min, Min, MinPVS0}) when V == Min ->
    {min, V, ds_pvattrs:add(Attrs, MinPVS0)};
update_min({_V,_Attrs}, MinStats) ->
    MinStats.

update_max({V, Attrs}, undefined) ->
    {max, V, ds_pvattrs:new(Attrs)};
update_max({V, Attrs}, {max, Max,_MaxPVS0}) when V > Max ->
    {max, V, ds_pvattrs:new(Attrs)};
update_max({V, Attrs}, {max, Max, MaxPVS0}) when V == Max ->
    {max, V, ds_pvattrs:add(Attrs, MaxPVS0)};
update_max({_V,_Attrs}, MaxStats) ->
    MaxStats.


%% Tests
-ifdef(TEST).

-endif.
