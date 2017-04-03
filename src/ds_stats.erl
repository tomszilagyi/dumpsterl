-module(ds_stats).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ new/0
        , add/2
        , join/2
        , get_count/1
        ]).

-ifdef(TEST).
-export([ eq/2 ]).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% general per-node statistics data
-record(stats, { count = 0,
                 pts,
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
    Pts = pts_new(),
    Sampler = ds_sampler:new(ds_opts:getopt(samples)),
    Hyperloglog = ds_hyperloglog:new(ds_opts:getopt(hll_b)),
    #stats{pts = Pts, sampler = Sampler, hyperloglog = Hyperloglog}.

add({V,_A}=VA,
    #stats{count = Count,
           pts = Pts,
           sampler = Sampler,
           hyperloglog = Hyperloglog}) ->
    Hash = erlang:phash2(V, 1 bsl 32),
    #stats{count = Count + 1,
           pts = pts_add(VA, Pts),
           sampler = ds_sampler:add_hash({Hash, VA}, Sampler),
           hyperloglog = ds_hyperloglog:add_hash(Hash, Hyperloglog)}.

get_count(#stats{count=Count}) -> Count.

%% Join two statistics into one
join(#stats{count = Count0,
            pts = Pts0,
            sampler = Sampler0,
            hyperloglog = HLL0},
     #stats{count = Count1,
            pts = Pts1,
            sampler = Sampler1,
            hyperloglog = HLL1}) ->
    #stats{count = Count0 + Count1,
           pts = pts_join(Pts0, Pts1),
           sampler = ds_sampler:join(Sampler0, Sampler1),
           hyperloglog = ds_hyperloglog:join(HLL0, HLL1)}.


%% Points of interest statistics are represented as:
%%
%%     [{Pt, Value, PVAttrs}]
%%
%% where
%%       Pt: point of interest, i.e., min, max
%%    Value: value of Pt
%%  PVAttrs: per-value attributes as maintained by ds_pvattrs
pts_new() ->
    [ {min, undefined, undefined}
    , {max, undefined, undefined}
    ].

pts_add(VA, PVStats) ->
    [pt_add(VA, PVS) || PVS <- PVStats].

pts_join(PVS0, PVS1) ->
    lists:zipwith(fun pt_join/2, PVS0, PVS1).


pt_add({V, Attrs}, {Pt, undefined, undefined}) ->
    {Pt, V, ds_pvattrs:new(Attrs)};
pt_add({V, Attrs}, {Pt, PtV, PV}) when V == PtV ->
    %% Use '==' in guard in case V and PtV are float and int with same value
    {Pt, V, ds_pvattrs:add(Attrs, PV)};
pt_add({V, Attrs}, {min, Min,_PV}) when V < Min ->
    {min, V, ds_pvattrs:new(Attrs)};
pt_add({V, Attrs}, {max, Max,_PV}) when V > Max ->
    {max, V, ds_pvattrs:new(Attrs)};
pt_add({_V,_Attrs}, PtStats) ->
    PtStats.

pt_join({Pt, undefined, undefined}, {Pt,_V,_PV} = PVS) -> PVS;
pt_join({Pt,_V,_PV}=PVS, {Pt, undefined, undefined}) -> PVS;
pt_join({Pt, V0, PV0}, {Pt, V1, PV1}) when V0 == V1 ->
    %% Use '==' in guard in case V0 and V1 are float and int with same value
    {Pt, V0, ds_pvattrs:join(PV0, PV1)};
pt_join({min, V0,_PV0}, {min, V1, PV1}) when V0 > V1 -> {min, V1, PV1};
pt_join({max, V0,_PV0}, {max, V1, PV1}) when V0 < V1 -> {max, V1, PV1};
pt_join(PVS0, _PVS1) -> PVS0.


%% Tests
-ifdef(TEST).

%% Return true iff two stats instances are equivalent.
%% The actual term-level representation (sampler gb_trees, etc.)
%% may be different depending on the order of add/join operations
%% that yielded the two instances.
eq(#stats{count = Count, pts = Pts, sampler = Sampler0, hyperloglog = HLL},
   #stats{count = Count, pts = Pts, sampler = Sampler1, hyperloglog = HLL}) ->
    ds_sampler:get_samples(Sampler0) =:= ds_sampler:get_samples(Sampler1);
eq(_Stats0, _Stats1) -> false.

-endif.
