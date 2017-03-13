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
new(Capacity) when is_integer(Capacity), Capacity > 0 ->
    #sampler{capacity = Capacity,
             size = 0,
             tree = gb_trees:empty(),
             max_hash = 0};
new(T) ->
    error(badarg, [T]).

%% Add a value with attributes to the sampler.
%% Attributes are passed to the per-value statistics module
%% that maintains stats for each sampled value.
add({V, A}, Sampler) ->
    Hash = erlang:phash2(V, 1 bsl 32),
    add_hash({Hash, {V, A}}, Sampler).

%% The sampler algorithm requires a hash that maps to 32-bit words.
%% This function is provided as an entry point in case you have already
%% hashed your terms with a suitable function and want to spare another
%% hash computation. In this case, please make sure to use a suitable
%% hash function (erlang:phash2 with the Range set to 2^32 is recommended).
%% In case of doubt, just use add/2 above.
add_hash({Hash, VA}, #sampler{capacity = Capacity,
                              size = Size0, tree=Tree0,
                              max_hash = MaxHash0} = Sampler)
  when Size0 < Capacity ->
    {Tree, Size} = update_tree(Hash, VA, Tree0, Size0),
    MaxHash = max(Hash, MaxHash0),
    Sampler#sampler{size = Size, max_hash = MaxHash, tree = Tree};
add_hash({Hash, _VA}, #sampler{max_hash = MaxHash} = Sampler)
  when Hash > MaxHash ->
    Sampler;
add_hash({Hash, {V, A}}, #sampler{max_hash = MaxHash,
                                  tree = Tree0} = Sampler)
  when Hash =:= MaxHash ->
    {Hash, {V, PVS0}} = gb_trees:largest(Tree0),
    PVS = ds_pvattrs:add(A, PVS0),
    Tree = gb_trees:update(Hash, {V, PVS}, Tree0),
    Sampler#sampler{tree = Tree};
add_hash({Hash, VA}, #sampler{capacity = Capacity,
                              size = Size0, tree = Tree0} = Sampler) ->
    {Tree1, Size1} = update_tree(Hash, VA, Tree0, Size0),
    if Size1 > Capacity ->
            {_, _, Tree} = gb_trees:take_largest(Tree1),
            {MaxHash, _} = gb_trees:largest(Tree),
            Sampler#sampler{size = Size1 - 1, max_hash = MaxHash, tree = Tree};
       true ->
            Sampler#sampler{size = Size1, tree = Tree1}
    end.

update_tree(Hash, {V, A}, Tree0, Size0) ->
    case gb_trees:lookup(Hash, Tree0) of
        {value, {V, PVS0}} ->
            PVS = ds_pvattrs:add(A, PVS0),
            {gb_trees:update(Hash, {V, PVS}, Tree0), Size0};
        none ->
            PVS = ds_pvattrs:new(A),
            {gb_trees:insert(Hash, {V, PVS}, Tree0), Size0 + 1}
    end.

%% The capacity of the joined sampler will be the maximum of the two.
join(#sampler{capacity = Capacity1} = Sampler1,
     #sampler{capacity = Capacity2} = Sampler2) when Capacity2 > Capacity1 ->
    join(Sampler2, Sampler1);
join(Sampler1, #sampler{tree = Tree2}) ->
    lists:foldl(fun add_hash/2, Sampler1, gb_trees:to_list(Tree2)).

%% Return a list of {V, PVS}
get_samples(#sampler{tree = Tree}) ->
    [Value || {_Hash, Value} <- gb_trees:to_list(Tree)].


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
    [io:format(user, "~10B: ~6w  ~p~n", [Hash, V, PVS])
     || {Hash, {V, PVS}} <- gb_trees:to_list(Tree)],
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
                       end, orddict:new(), [N || {N, _PVS} <- get_samples(S)]),
    %% assert some level of uniformity
    ?assert(length(Hist) > 35),
    ?assert(lists:max([Count || {_Bin, Count} <- Hist]) =< 4),
    ok.

duplicates_test() ->
    Sd = lists:foldl(fun add/2, new(10),
                     [{N rem 25, [{key, N}]} || N <- lists:seq(0, 9999)]),
    ?print(Sd),
    Counts = [ds_pvattrs:get_count(PVS) || {_N, PVS} <- get_samples(Sd)],
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
        lists:partition(fun({V,_PVS}) -> V < 1000 end, get_samples(Sj1)),
    ?assert(length(JoinedFromS1) > 0),
    ?assert(length(JoinedFromS2) > 0),
    ?print(Sj1),

    Sj2 = join(S1, Sj1),
    %% verify that items from S1 have count 2 in Sj2
    CountsFromS1 =
        [ds_pvattrs:get_count(PVS) || {N, PVS} <- get_samples(Sj2), N < 1000],
    [?assertEqual(2, Count) || Count <- CountsFromS1],
    ?print(Sj2).

-endif.
