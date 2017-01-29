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

-import(ds_opts, [ getopt/1
                 , setopts/1
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
%%   {Stats, Priv}
%%
%%   - Stats is a tuple of general statistics data, eg.:
%%     - an integer count of data items covered by this type class.
%%     - an even sampling of the data values
%%   - Priv is private data (specific to the class), holds
%%     further statistics. For recursive types (lists and tuples)
%%     it is a list of specs for the individual elements.
%% - SubSpec is a list of subtype nodes, if any, or [].
%%   It is up to Class to further categorize a piece of data
%%   into subtypes. For leaf nodes, this is always [].


%% Initialize a new data spec
new() -> new('T').

new(Class) ->
    Data = {ds_stats:stats_data(), ds_types:priv_data(Class)},
    {Class, Data, []}.

%% Initialize, immediately adding one data term
new(Class, Data) -> add(Data, new(Class)).

%% Add the value V to Spec. add/2 is written so it can be
%% used as a function to lists:foldl/3.
%%
%% When adding a value to the spec, each type class knows
%  which subtype the data fits in.
%% Adding a new value is a recursive process:
%% - starting from 'T' (root type) class, each class
%%   - accounts for the new value itself;
%%   - chooses appropriate subtype (if any) and
%%   - passes the value to that subtype.
%%
%% Eg. when adding the value 100, 'T' accounts
%% for this (increases counter of values, etc),
%% determines that it is a numeric type, and so
%% passes it to 'numeric' which, in turn, determines
%% it is an integer and passes to 'integer', which
%% might still subtype it based on its magnitude.
%% On all levels of the hierarchy, counters will be
%% increased and samples will be collected.

%% TODO maybe require V to have a normalized Attrs here
%% (ds_drv to pre-process) so we can do simple pattern matching
%% instead of orddict:find/2 each time we want ts and key

%% TODO provide attribute 'keep' to signify that a particular
%% piece of data satisfies certain criteria to be 'interesting'.
%% This attribute would ensure that the data is kept stored
%% regardless of sampling allowances.
add(V, {Class, Data0, SubSpec}) ->
    case lists:keyfind(Class, 1, ds_types:types()) of
        false -> %% leaf type
            {Class, update_data(V, Class, Data0), SubSpec};
        {Class, SubTypes} -> %% abstract type
            case subtype(V, SubTypes) of
                '$null' -> %% no subtype -- treat it as a leaf type
                    {Class, update_data(V, Class, Data0), SubSpec};
                SubType -> %% merge data into subtype spec
                    {Class, Data0, merge(V, SubType, SubSpec)}
            end
    end.

update_data(V, Class, {StatsData, PrivData}) ->
    {ds_stats:stats_data(V, StatsData),
     ds_types:priv_data(V, Class, PrivData)}.

%% choose the appropriate subtype based on the filters
%% in the type hierarchy, or dynamically generate subtype.
subtype(_V, []) -> '$null';
subtype({V, A}, [{'$dynamic', SubTag, SubFun} | Rest]) ->
    case SubFun(V) of
        false -> subtype({V, A}, Rest);
        Data  -> {SubTag, Data}
    end;
subtype({V, A}, [{SubType, FilterFun} | Rest]) ->
    case FilterFun(V) of
        false -> subtype({V, A}, Rest);
        true  -> SubType
    end.

%% choose subspec given by Class or create it from scratch,
%% add V to it and return the resulting Spec.
merge(V, Class, Spec) ->
    case lists:keyfind(Class, 1, Spec) of
        false   -> [new(Class, V) | Spec];
        SubSpec -> lists:keystore(Class, 1, Spec, add(V, SubSpec))
    end.
