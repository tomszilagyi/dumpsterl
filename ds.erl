-module(ds).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% `dataspec' derives a spec of data based on a stream of values.
%% It can be used to eg. `discover' the data type stored in a table
%% (column, etc.)

-export([ add/2
        , new/0
        , new/1
        , new/2
        ]).

-include_lib("eunit/include/eunit.hrl").

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
    Data = {ds_stats:stats_data(), ds_types:ext_data(Class)},
    {Class, Data, []}.

%% Initialize, immediately adding one data term
new(Class, Data) -> add(Data, new(Class)).

%% Add the value V to Spec. add/2 is written so it can be
%% used as a function to lists:foldl/3.

%% TODO maybe require V to have a normalized Attrs here
%% (ds_drv to pre-process) so we can do simple pattern matching
%% instead of orddict:find/2 each time we want ts and key

%% TODO provide attribute 'keep' to signify that a particular
%% piece of data satisfies certain criteria to be 'interesting'.
%% This attribute would ensure that the data is kept stored
%% regardless of sampling allowances.
add({V,_A}=VA, {Class, Data, SubSpec}) ->
    add(VA, {Class, Data, SubSpec}, ds_types:subtype(V, Class)).

add(VA, {Class, Data0, SubSpec}, '$null') -> % leaf type
    {Class, update(VA, Class, Data0), SubSpec};
add({_V, A}=VA, {Class, Data0, SubSpec}, {'$fields', Fields}) -> % many subtypes
    {Class, update(VA, Class, Data0), merge_fields({Fields, A}, SubSpec)};
add({_V, A}=VA, {Class, Data0, SubSpec}, {'$elements', Items}) -> % one subtype
    {Class, update(VA, Class, Data0), merge_items({Items, A}, SubSpec)};
add(VA, {Class, Data0, SubSpec}, SubType) -> % abstract type
    {Class, Data0, merge(VA, SubType, SubSpec)}.

update(VA, Class, {Stats, Ext}) ->
    {ds_stats:stats_data(VA, Stats), ds_types:ext_data(VA, Class, Ext)}.

-ifdef(discarded).
%% choose the appropriate subtype based on the filters
%% in the type hierarchy, or dynamically generate subtype.
subtype(_VA, []) -> '$null';
subtype({V,_A}=VA, [{'$dynamic', SubTag, SubFun} | Rest]) ->
    case SubFun(V) of
        false -> subtype(VA, Rest);
        Data  -> {SubTag, Data}
    end;
subtype({V,_A}=VA, [{SubType, FilterFun} | Rest]) ->
    case FilterFun(V) of
        false -> subtype(VA, Rest);
        true  -> SubType
    end.
-endif.

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
