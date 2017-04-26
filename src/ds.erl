%% -*- coding: utf-8 -*-
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
%%     further attributes and/or statistics.
%%
%% - SubSpec is a list of child nodes. Depending on the kind of type
%%   denoted by Class, this may be a list of subtypes or in case of
%%   compound types, a list of specs corresponding to the elements
%%   or fields of this type. The exact semantics of SubSpec are left
%%   up to Class, but the tree is uniformly recursive through SubSpecs.

%% Initialize a new data spec
new() -> new(term).

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
add({_V, A}=VA, {Class, Data0, SubSpec0}, {'$attrs', Dict}) -> % dict of subtypes
    %% The attribute spec is stored in Ext, merge_attrs needs to modify it.
    {Stats, Ext0} = update(VA, Class, Data0),
    {Ext, SubSpec} = merge_attrs({Dict, A}, Ext0, SubSpec0),
    {Class, {Stats, Ext}, SubSpec};
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
    lists:map(fun(V) -> new(term, {V, A}) end, Vs);
merge_fields({Vs, A}, SubSpec) ->
    lists:map(fun({V, S}) -> add({V, A}, S) end, lists:zip(Vs, SubSpec)).

%% merge elements of container types into the single inferior spec
merge_items({Vs, A}, []) ->
    [lists:foldl(fun(V, Spec) -> add({V, A}, Spec) end, new(term), Vs)];
merge_items({Vs, A}, [SubSpec]) ->
    [lists:foldl(fun(V, Spec) -> add({V, A}, Spec) end, SubSpec, Vs)].

%% merge a [{Key, Value}] dictionary into a list of sub-specs.
%% Attrs contains a key list [Key] so that if the N-th item of
%% Attrs is Key, then the N-th item of SpecL corresponds to the
%% values associated with Key.
%% Return {Attrs, SpecL} with updated data.
merge_attrs({Dict, A}, Attrs, SpecL) ->
    lists:foldl(fun({K, V}, Acc) -> merge_attr({K, V}, A, Acc) end,
                {Attrs, SpecL}, Dict).

merge_attr({K, V}, A, {Attrs0, SpecL0}) ->
    case ds_utils:index(K, Attrs0) of
        error ->
            Attrs = lists:sort([K|Attrs0]),
            Index = ds_utils:index(K, Attrs),
            {SpecL1, SpecL2} = lists:split(Index-1, SpecL0),
            SpecL = lists:append([SpecL1, [new(term, {V, A})], SpecL2]),
            {Attrs, SpecL};
        Index ->
            {SpecL1, [Spec0|SpecL2]} = lists:split(Index-1, SpecL0),
            Spec = add({V, A}, Spec0),
            SpecL = lists:append([SpecL1, [Spec], SpecL2]),
            {Attrs0, SpecL}
    end.

%% merge spec for improper list
merge_improper(VA, []) ->
    merge_improper(VA, [new(term), new(term)]);
merge_improper({Vs, Vt, A}, [ListSpec0, TailSpec0]) ->
    [ListSpec] = merge_items({Vs, A}, [ListSpec0]),
    TailSpec = add({Vt, A}, TailSpec0),
    [ListSpec, TailSpec].


%% Join two spec trees into one.
%% This clause is for maps where the children lists are joined
%% based on attributes stored in Ext.
join({map=Class, {Stats1, Ext1}, ChildL1},
         {Class, {Stats2, Ext2}, ChildL2}) ->
    Stats = ds_stats:join(Stats1, Stats2),
    Ext = lists:usort(Ext1 ++ Ext2), % joined attribute list
    AttrDict1 = lists:zip(Ext1, ChildL1),
    AttrDict2 = lists:zip(Ext2, ChildL2),
    ChildL =
        %% For each attribute in joined Ext, look it up in both children
        %% and join appropriately into the result child spec list.
        lists:foldl(
          fun(Attr, Acc) ->
                  case {lists:keyfind(Attr, 1, AttrDict1),
                        lists:keyfind(Attr, 1, AttrDict2)} of
                      { {Attr, Spec1}, {Attr, Spec2} } ->
                          [join(Spec1, Spec2) | Acc];
                      { {Attr, Spec1}, false } ->
                          [Spec1 | Acc];
                      { false, {Attr, Spec2} } ->
                          [Spec2 | Acc]
                  end
          end, [], lists:reverse(Ext)),
    {Class, {Stats, Ext}, ChildL};
%% Subspec (child) trees are joined in a class-specific manner:
%%   - joined by attributes stored in Ext for maps (see above clause);
%%   - zipped (by position) for other generic types;
%%   - joined (by class) for all other types.
join({Class, {Stats1, Ext1}, ChildL1},
     {Class, {Stats2, Ext2}, ChildL2}) ->
    Stats = ds_stats:join(Stats1, Stats2),
    Ext = ds_types:ext_join(Class, Ext1, Ext2),
    ChildL = case ds_types:kind(Class) of
                 generic -> lists:zipwith(fun join/2, ChildL1, ChildL2);
                 _       -> join_specs(ChildL1, ChildL2)
             end,
    {Class, {Stats, Ext}, ChildL}.

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
%%   term -> tuple -> {tuple, 3} -> ...
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
