-module(ds).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% `dumpsterl' derives a spec of data based on a stream of values.
%% It can be used to eg. `discover' the data type stored in a table
%% (column, etc.)

-export([ add/2
        , new/0
        , new/1
        , new/2
        , join/2
        , compact/1
        , join_up/1
        ]).

-ifdef(TEST).
-export([ eq/2 ]).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% A data spec is a list of type specs.
%% It is most often hierarchically nested, hence we have
%% a hierarchical tree of type classes.
%% Nodes of this tree are represented as tuples:
%%
%% {Class, Data, SubSpec}
%%
%% - Class is a term (in most cases an atom) describing
%%   the type this node represents in the type hierarchy.
%%   Toplevel type examples: integer, atom, list.
%% - Data is node-specific data as a tuple:
%%
%%   {Stats, Ext}
%%
%%   - Stats is a tuple of general statistics data, eg.:
%%     - an integer count of data items covered by this type class.
%%     - an even sampling of the data values
%%   - Ext is class-specific extra/extended data to hold
%%     further statistics.
%%
%% - SubSpec is a list of child nodes. Depending on the kind of type
%%   denoted by Class, this may be a list of subtypes or in case of
%%   compound types, a list of specs corresponding to the elements
%%   or fields of this type. The exact semantics of SubSpec are left
%%   up to Class, but the tree is uniformly recursive through SubSpecs.

%% Initialize a new data spec
new() -> new('T').

new(Class) ->
    Data = {ds_stats:new(), ds_types:ext_new(Class)},
    {Class, Data, []}.

%% Initialize, immediately adding one data term
new(Class, Data) -> add(Data, new(Class)).

%% Add the value V with attributes A to Spec.
%% add/2 is written so it can be used as a function to lists:foldl/3.
add({V,_A}=VA, {Class, Data, SubSpec}) ->
    add(VA, {Class, Data, SubSpec}, ds_types:subtype(V, Class)).

add(VA, {Class, Data0, SubSpec}, '$null') -> % leaf type
    {Class, update(VA, Class, Data0), SubSpec};
add({_V, A}=VA, {Class, Data0, SubSpec}, {'$fields', Fields}) -> % many subtypes
    {Class, update(VA, Class, Data0), merge_fields({Fields, A}, SubSpec)};
add({_V, A}=VA, {Class, Data0, SubSpec}, {'$elements', Items}) -> % one subtype
    {Class, update(VA, Class, Data0), merge_items({Items, A}, SubSpec)};
add({_V, A}=VA, {Class, Data0, SubSpec}, {'$improper_list', Items, Tail}) ->
    %% one subtype for list items, one for tail
    {Class, update(VA, Class, Data0), merge_improper({Items, Tail, A}, SubSpec)};
add(VA, {Class, Data0, SubSpec}, SubType) -> % abstract type
    {Class, Data0, merge(VA, SubType, SubSpec)}.

update(VA, Class, {Stats, Ext}) ->
    {ds_stats:add(VA, Stats), ds_types:ext_add(VA, Class, Ext)}.

%% choose subspec given by Class or create it from scratch,
%% add V to it and return the resulting Spec.
merge(VA, Class, Spec) ->
    case lists:keyfind(Class, 1, Spec) of
        false   -> [new(Class, VA) | Spec];
        SubSpec -> lists:keystore(Class, 1, Spec, add(VA, SubSpec))
    end.

%% merge the per-field sub-specs of compound types
merge_fields({Vs, A}, []) ->
    lists:map(fun(V) -> new('T', {V, A}) end, Vs);
merge_fields({Vs, A}, SubSpec) ->
    lists:map(fun({V, S}) -> add({V, A}, S) end, lists:zip(Vs, SubSpec)).

%% merge elements of container types into the single inferior spec
merge_items({Vs, A}, []) ->
    [lists:foldl(fun(V, Spec) -> add({V, A}, Spec) end, new('T'), Vs)];
merge_items({Vs, A}, [SubSpec]) ->
    [lists:foldl(fun(V, Spec) -> add({V, A}, Spec) end, SubSpec, Vs)].

%% merge spec for improper list
merge_improper(VA, []) ->
    merge_improper(VA, [new('T'), new('T')]);
merge_improper({Vs, Vt, A}, [ListSpec0, TailSpec0]) ->
    [ListSpec] = merge_items({Vs, A}, [ListSpec0]),
    TailSpec = add({Vt, A}, TailSpec0),
    [ListSpec, TailSpec].


%% Join two spec trees into one.
%% Depending on class kind, subspec trees are either joined (by class)
%% or zipped (by position).
join({Class, {Stats1, Ext1}, ChildL1},
     {Class, {Stats2, Ext2}, ChildL2}) ->
    ChildL = case ds_types:kind(Class) of
                 generic -> lists:zipwith(fun join/2, ChildL1, ChildL2);
                 _       -> join_specs(ChildL1, ChildL2)
             end,
    {Class,
     {ds_stats:join(Stats1, Stats2), ds_types:ext_join(Class, Ext1, Ext2)},
     ChildL}.

join_specs(Acc, []) -> Acc;
join_specs(Acc0, [{Class,_Data,_ChildL}=Spec | Rest]) ->
    Acc = case lists:keyfind(Class, 1, Acc0) of
              false   -> [Spec | Acc0];
              AccSpec -> lists:keystore(Class, 1, Acc0, join(AccSpec, Spec))
          end,
    join_specs(Acc, Rest).

%% Compact the tree by cutting unnecessary abstract types
%% (those having a single child and no terms captured themselves)
%% from the tree. E.g. if all terms are tuples of three, the tree
%%   'T' -> tuple -> {tuple, 3} -> ...
%% will be simplified to
%%   {tuple, 3} -> ...
%% without any loss of information.
compact({Class, {Stats,_Ext} = Data, [SubSpec1]}) ->
    Kind = ds_types:kind(Class),
    Count = ds_stats:get_count(Stats),
    if Kind =/= generic andalso Count =:= 0 ->
            compact(SubSpec1);
       true ->
            {Class, Data, [compact(SubSpec1)]}
    end;
compact({Class, Data, SubSpec}) ->
    {Class, Data, [compact(SSp) || SSp <- SubSpec]}.

%% For performance, abstract type nodes do not update their data
%% when collecting terms, since the same information will be stored
%% further down the tree. Before evaluating/visualizing the tree,
%% this function should be used to propagate the data upwards so
%% the abstract nodes also have all the statistics.
join_up({Class, Data0, SubSpec0}) ->
    SubSpec = [join_up(SSp) || SSp <- SubSpec0],
    Data = case ds_types:kind(Class) of
               generic -> Data0; % don't cross type domains with the join
               _       -> join_data(Data0, SubSpec)
           end,
    {Class, Data, SubSpec}.

join_data(Data, SubSpec) -> lists:foldl(fun join_data_f/2, Data, SubSpec).

%% NB. do not propagate Ext upwards, since that is class-specific.
join_data_f({_Class, {Stats1,_Ext1}, _SubSpec}, {Stats0, Ext0}) ->
    {ds_stats:join(Stats0, Stats1), Ext0}.


%% Tests
-ifdef(TEST).

%% Return true iff two spec instances are equivalent.
%% The actual term-level representation may be different, hence this function.
eq({Class, {Stats1, Ext}, ChL1},
   {Class, {Stats2, Ext}, ChL2}) ->

    ChildrenEq =
        case lists:usort(lists:zipwith(fun eq/2,
                                       lists:sort(ChL1), lists:sort(ChL2))) of
            [] -> true;
            [true] -> true;
            _ -> false
        end,
    ds_stats:eq(Stats1, Stats2) andalso
        length(ChL1) =:= length(ChL2) andalso
        ChildrenEq;
eq(_Spec0, _Spec1) -> false.

-endif.
