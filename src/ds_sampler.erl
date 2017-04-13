%% -*- coding: utf-8 -*-
-module(ds_sampler).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% Statistically representative stream sampler
%% See chapter 4.2. Sampling data in a stream
%% in Mining of Massive Datasets, Second Edition, 2014
%% by J. Leskovec, A. Rajaraman and J. D. Ullman

-export([ new/1
        , add/2
        , add_hash/2
        , join/2
        , get_samples/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(sampler,
        { capacity    % The maximum number of samples to store
        , size        % Number of currently stored samples
        , tree        % gb_trees structure of samples
        , max_hash    % Current upper limit on hash value
        }).

%% Initialize a sampler with given capacity.
new(false) -> undefined;
new(Capacity) when is_integer(Capacity), Capacity > 0;
                   Capacity =:= infinity ->
    #sampler{capacity = Capacity,
             size = 0,
             tree = gb_trees:empty(),
             max_hash = 0};
new(T) ->
    error(badarg, [T]).

%% Add a value with attributes to the sampler.
%% Attributes are passed to the per-value statistics module
%% that maintains stats for each sampled value.
add(_VA, undefined) -> undefined;
add({V, A}, Sampler) ->
    Hash = erlang:phash2(V, 1 bsl 32),
    add_hash({Hash, {V, A}}, Sampler).

%% The sampler algorithm requires a hash that maps to 32-bit words.
%% This function is provided as an entry point in case you have already
%% hashed your terms with a suitable function and want to spare another
%% hash computation. In this case, please make sure to use a suitable
%% hash function (erlang:phash2 with the Range set to 2^32 is recommended).
%% In case of doubt, just use add/2 above.
add_hash(_Data, undefined) -> undefined;
add_hash({Hash, {V, A}}, #sampler{capacity = Capacity,
                                  size = Size0, tree=Tree0,
                                  max_hash = MaxHash0} = Sampler)
  when Size0 < Capacity ->
    {Tree, Size} = update_tree(Hash, {V, A}, Tree0, Size0),
    MaxHash = max(Hash, MaxHash0),
    Sampler#sampler{size = Size, max_hash = MaxHash, tree = Tree};
add_hash({Hash, {_V,_A}}, #sampler{max_hash = MaxHash} = Sampler)
  when Hash > MaxHash ->
    Sampler;
add_hash({Hash, {V, A}}, #sampler{max_hash = MaxHash,
                                  tree = Tree0} = Sampler)
  when Hash =:= MaxHash ->
    {Hash, {V, PV0}} = gb_trees:largest(Tree0),
    PV = ds_pvattrs:add(A, PV0),
    Tree = gb_trees:update(Hash, {V, PV}, Tree0),
    Sampler#sampler{tree = Tree};
add_hash({Hash, {V, A}}, #sampler{capacity = Capacity,
                                  size = Size0, tree = Tree0} = Sampler) ->
    {Tree1, Size1} = update_tree(Hash, {V, A}, Tree0, Size0),
    if Size1 > Capacity ->
            {_, Value, Tree} = gb_trees:take_largest(Tree1),
            Size = case Value of
                       {_Vmax,_PV} -> Size1 - 1;
                       Bucket -> Size1 - length(Bucket)
                   end,
            {MaxHash, _} = gb_trees:largest(Tree),
            Sampler#sampler{size = Size, max_hash = MaxHash, tree = Tree};
       true ->
            Sampler#sampler{size = Size1, tree = Tree1}
    end.

update_tree(Hash, {V, A}, Tree0, Size0) ->
    case gb_trees:lookup(Hash, Tree0) of
        none ->
            PV = ds_pvattrs:new(A),
            {gb_trees:insert(Hash, {V, PV}, Tree0), Size0 + 1};
        {value, {V, PV0}} ->
            PV = ds_pvattrs:add(A, PV0),
            {gb_trees:update(Hash, {V, PV}, Tree0), Size0};
        {value, {V1, PV1}} ->
            %% hash collision with another single value, convert to list bucket
            PV = ds_pvattrs:new(A),
            Bucket = [{V, PV}, {V1, PV1}],
            {gb_trees:update(Hash, Bucket, Tree0), Size0 + 1};
        {value, Bucket0} ->
            %% collision occurred earlier on this hash
            {Bucket, Size} = update_bucket(Bucket0, {V, A}, Size0),
            {gb_trees:update(Hash, Bucket, Tree0), Size}
    end.

update_bucket(Bucket, {V, A}, Size) ->
    case lists:keyfind(V, 1, Bucket) of
        false ->
            {[{V, ds_pvattrs:new(A)} | Bucket], Size + 1};
        {V, PV0} ->
            PV = ds_pvattrs:add(A, PV0),
            {lists:keystore(V, 1, Bucket, {V, PV}), Size}
    end.


%% The capacity of the joined sampler will be the maximum of the two.
join(undefined, Sampler) -> Sampler;
join(Sampler, undefined) -> Sampler;
join(#sampler{capacity = Capacity1} = Sampler1,
     #sampler{capacity = Capacity2} = Sampler2) when Capacity2 > Capacity1 ->
    join(Sampler2, Sampler1);
join(Sampler1, #sampler{tree = Tree2}) ->
    lists:foldl(fun({_Hash, {_V,_A}}=Data, Sampler) ->
                        add_hash(Data, Sampler);
                   ({Hash, Bucket}, Sampler) when is_list(Bucket) ->
                        lists:foldl(fun(VA, Acc) ->
                                            add_hash({Hash, VA}, Acc)
                                    end, Sampler, Bucket)
                end, Sampler1, gb_trees:to_list(Tree2)).

%% Return a list of {V, PV}
get_samples(undefined) -> [];
get_samples(#sampler{tree = Tree}) -> lists:flatten(gb_trees:values(Tree)).


%% Tests
-ifdef(TEST).

%%-define(DEBUG, true).
-ifdef(DEBUG).
print(#sampler{capacity = Capacity, size = Size, tree = Tree,
               max_hash = MaxHash}, Name) ->
    io:format(user,
              "~nSampler ~s with capacity=~B, size=~B, max_hash=~B~n"
              "sampled values with per-value stats:~n~n",
              [Name, Capacity, Size, MaxHash]),
    [io:format(user, "~10B: ~6w  ~p~n", [Hash, V, PV])
     || {Hash, {V, PV}} <- gb_trees:to_list(Tree)],
    ok.
-define(print(Sampler), print(Sampler, ??Sampler)).
-else.
-define(print(Sampler), ok).
-endif.

new_test() ->
    ?assertError(badarg, new(0)),
    ?assertError(badarg, new(nan)),
    ?assertEqual(#sampler{capacity = 10, size = 0,
                          max_hash = 0, tree = gb_trees:empty()},
                 new(10)).

sampler_test() ->
    S = lists:foldl(fun add/2, new(50),
                    [{N, []} || N <- lists:seq(0, 9999)]),
    ?print(S),
    Hist = lists:foldl(fun(N, Acc) ->
                               orddict:update_counter(N div 100, 1, Acc)
                       end, orddict:new(), [N || {N, _PV} <- get_samples(S)]),
    %% assert some level of uniformity
    ?assert(length(Hist) > 35),
    ?assert(lists:max([Count || {_Bin, Count} <- Hist]) =< 4),
    ok.

collisions_test() ->
    NonColliders = lists:seq(0, 9999),
    Colliders =
        %% Each row contains numbers that hash to the same value.
        [   22722,  266086
        ,   26544,  217817
        ,   33702,   44741
        ,   38988,  125056
        ,   47282,   81624
        ,  125915,  283130
        ,  300486,  879671
        ,  302126,  905421
        ,  302781,  868970
        ,  302800,  362107
        %% triple collisions:
        ,   70024, 1918936, 4696183
        , 1074149, 2805927, 9580072
        , 1190377, 1534289, 6156731
        , 1235514, 4795238, 6886479
        , 2378846, 3760671, 5137463
        , 2671427, 7709636, 8682435
        , 3635546, 5201779, 5527149
        , 4604703, 4879230, 9248585
        , 5557231, 5988521, 6524388
        , 6161979, 6660282, 7669964
        ],

    S0 = lists:foldl(fun add/2, new(infinity), [{N, []} || N <- NonColliders]),
    S1 = lists:foldl(fun add/2, S0, [{N, []} || N <- Colliders]),
    SampleCount = length(NonColliders) + length(Colliders),
    ?assertEqual(SampleCount, length(get_samples(S1))),
    SampleValues = lists:sort(Colliders ++ NonColliders),
    ?assertEqual(SampleValues, [V || {V,_PV} <- lists:sort(get_samples(S1))]).

duplicates_test() ->
    Sd = lists:foldl(fun add/2, new(10),
                     [{N rem 25, [{key, N}]} || N <- lists:seq(0, 9999)]),
    ?print(Sd),
    Counts = [ds_pvattrs:get_count(PV) || {_N, PV} <- get_samples(Sd)],
    ?assertEqual(10, length(Counts)),
    [?assertEqual(400, Count) || Count <- Counts].

join_test() ->
    S1 = lists:foldl(fun add/2, new(8),
                     [{N, [{ts, 10*N}]} || N <- lists:seq(0, 999)]),
    ?print(S1),

    S2 = lists:foldl(fun add/2, new(24),
                     [{N, [{key, N}]} || N <- lists:seq(1000, 1999)]),
    ?print(S2),

    Sj1 = join(S1, S2),
    %% verify that the joined set contains samples from both S1 and S2
    {JoinedFromS1, JoinedFromS2} =
        lists:partition(fun({V,_PV}) -> V < 1000 end, get_samples(Sj1)),
    ?assert(length(JoinedFromS1) > 0),
    ?assert(length(JoinedFromS2) > 0),
    ?print(Sj1),

    Sj2 = join(S1, Sj1),
    %% verify that items from S1 have count 2 in Sj2
    CountsFromS1 =
        [ds_pvattrs:get_count(PV) || {N, PV} <- get_samples(Sj2), N < 1000],
    [?assertEqual(2, Count) || Count <- CountsFromS1],
    ?print(Sj2).

join_with_collisions_test() ->
    L1 = [ % collide with self
             22722,  266086
         ,   26544,  217817
         ,   33702,   44741
         ,   38988,  125056
          % collide with L2
         ,  302781
         ,  302800
         ,   70024, 1918936
         , 1074149, 2805927
         ],
    L2 = [ % collide with self
            47282,   81624
         ,  125915,  283130
         ,  300486,  879671
         ,  302126,  905421
           % collide with L1
         ,  868970
         ,  362107
         , 1918936, 4696183
         , 2805927, 9580072
         ],

    %% we want all samples to fit into one sampler
    TotalCap = length(L1) + length(L2),

    S1 = lists:foldl(fun add/2, new(TotalCap), [{N, []} || N <- L1]),
    ?print(S1),
    S2 = lists:foldl(fun add/2, new(TotalCap), [{N, []} || N <- L2]),
    ?print(S2),

    Sj = join(S1, S2),
    ?print(Sj),

    RefSamples = lists:usort(L1 ++ L2),
    JoinedSamples = get_samples(Sj),
    ?assertEqual(length(RefSamples), length(JoinedSamples)),
    ?assertEqual(RefSamples, lists:sort([V || {V,_PV} <- JoinedSamples])).

-endif.
