-module(ds).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% `dataspec' derives a spec of data based on a stream of values.
%% It can be used to eg. `discover' the data type stored in a table
%% (column, etc.)

-export([ add/2
        , new/0
        , new/1
        , sample_data/0
        , sample_data/1
        , sample_data/2
        , get_samples/1
        ]).

-include_lib("eunit/include/eunit.hrl").

%% Option handling: interface to ds_opts module
getopt(Opt) -> ds_opts:getopt(Opt).
setopts(Opts) -> ds_opts:setopts(Opts).

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
%%   {Count, SampleData, Priv}
%%
%%   - Count is an integer count of data items covered by
%%     this type class.
%%   - SampleData contains an even sampling of the data values
%%   - Priv is private data (specific to the class), holds
%%     further statistics. For recursive types (lists and tuples)
%%     it is a list of specs for the individual elements.
%% - SubSpec is a list of subtype nodes, if any, or [].
%%   It is up to Class to further categorize a piece of data
%%   into subtypes. For leaf nodes, this is always [].

%% The type hierarchy is defined by types()
%% - subtypes are mutually exclusive
%% - dynamically created subtypes denoted by
%%     {'$dynamic', Tag, Fun}
%%   where the actual subtype is derived as {Tag, Fun(Data)}
%% - dynamic subtypes must be last in the list of subtypes

types() ->
    [ {'T'
      , [ {numeric, fun erlang:is_number/1}
        , {atom, fun erlang:is_atom/1}
        , {list, fun erlang:is_list/1}
        , {tuple, fun erlang:is_tuple/1}
        ]}
    , {numeric
      , [ {integer, fun erlang:is_integer/1}
        , {float, fun erlang:is_float/1}
        ]}
    , {list
      , [ {empty, fun(L) -> L =:= [] end}
        , {str_printable, fun is_str_printable/1}
        , {'$dynamic', list, fun length/1}
        ]}
    , {str_printable
      , [ {str_alnum, fun is_str_alnum/1}
        ] ++ opt_strlen_stub(getopt(strlen)) }
    , {str_alnum
      , [ {str_integer, fun is_str_integer/1}
        , {str_alpha, fun is_str_alpha/1}
        ] ++ opt_strlen_stub(getopt(strlen)) }
    , {tuple
      , [ {empty, fun(T) -> T =:= {} end}
        , {'$dynamic', tuple, fun size/1}
        ]}
    ] ++ opt_mag(getopt(mag))
      ++ opt_strlen(getopt(strlen)).

opt_mag(0) -> [];
opt_mag(N) ->
    [ {integer, [ {'$dynamic', mag, fun(V) -> mag(V, N) end} ]}
    , {float,   [ {'$dynamic', mag, fun(V) -> mag(V, N) end} ]}
    ].

opt_strlen(false) -> [];
opt_strlen(true) ->
    [ {str_integer, [ {'$dynamic', len, fun length/1} ]}
    , {str_alpha,   [ {'$dynamic', len, fun length/1} ]}
    ].

opt_strlen_stub(false) -> [];
opt_strlen_stub(true)  -> [{'$dynamic', len, fun length/1}].


%% Initialize a new data spec
new() -> new('T').

new(Class) -> {Class, {0, sample_data(), []}, []}.

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

add(V, {Class, {Count, SD, PrivData}, SubSpec}) ->
    Data = {Count+1, sample_data(V, SD), pdata(V, Class, PrivData)},
    case lists:keyfind(Class, 1, types()) of
        false -> %% leaf type
            {Class, Data, SubSpec};
        {Class, SubTypes} -> %% abstract type
            SubType = subtype(V, SubTypes),
            {Class, Data, merge(V, SubType, SubSpec)}
    end.

%% choose the appropriate subtype based on the filters
%% in the type hierarchy, or dynamically generate subtype.
subtype(_V, []) -> untyped;
subtype(V, [{'$dynamic', SubTag, SubFun} | _Rest]) ->
    {SubTag, SubFun(V)};
subtype(V, [{SubType, FilterFun} | Rest]) ->
    case FilterFun(V) of
        true  -> SubType;
        false -> subtype(V, Rest)
    end.

%% choose subspec given by Class or create it from scratch,
%% add V to it and return the resulting Spec.
merge(V, Class, Spec) ->
    case lists:keyfind(Class, 1, Spec) of
        false   -> [new(Class, V) | Spec];
        SubSpec -> lists:keystore(Class, 1, Spec, add(V, SubSpec))
    end.

%% class-specific private data
pdata(V, atom, PD) -> pdata_atom(V, PD);
pdata(V, {list, _N}, PD) -> pdata_recur(V, PD);
pdata(V, {tuple, _N}, PD) -> pdata_recur(tuple_to_list(V), PD);
pdata(V, numeric, PD) -> pdata_numeric(V, PD);
pdata(V, integer, PD) -> pdata_numeric(V, PD);
pdata(V, float, PD) -> pdata_numeric(V, PD);
pdata(V, {mag, _N}, PD) -> pdata_numeric(V, PD);
pdata(_V, _Class, PrivData) -> PrivData.

%% For atoms, maintain a histogram of value occurrence
pdata_atom(V, PD) ->
    orddict:update_counter(V, 1, PD).

%% For numeric types, maintain statistics about numbers seen
pdata_numeric(V, PD0) ->
    PD1 = orddict:update(min, fun(Min) -> min(V, Min) end, V, PD0),
    PD2 = orddict:update(max, fun(Max) -> max(V, Max) end, V, PD1),
    PD2.

%% For lists and tuples, maintain a list of type specs for
%% each list/tuple element position.
pdata_recur(V, []) ->
    lists:map(fun(D) -> new('T', D) end, V);
pdata_recur(V, PrivData) ->
    lists:map(fun({D, S}) -> add(D, S) end, lists:zip(V, PrivData)).

%% string classifier functions
is_str_integer(S) -> string_in_ranges(S, [{$0, $9}]).
is_str_alpha(S) -> string_in_ranges(S, [{$A, $Z}, {$a, $z}]).
is_str_alnum(S) -> string_in_ranges(S, [{$0, $9}, {$A, $Z}, {$a, $z}]).
is_str_printable(S) -> string_in_ranges(S, [{32, 127}]).

%% check that string consists of chars that fit in the given ranges
string_in_ranges([], _Ranges)   -> true;
string_in_ranges([C|L], Ranges) ->
    case char_in_ranges(C, Ranges) of
        false -> false;
        true  -> string_in_ranges(L, Ranges)
    end.

%% check that char C is in one of the ranges spec'd as [{Min, Max}].
char_in_ranges(_C, []) -> false;
char_in_ranges(C, [{Min, Max} | _]) when C >= Min, C =< Max -> true;
char_in_ranges(C, [{_Min, _Max} | Rest]) -> char_in_ranges(C, Rest).

%% order of magnitude for integers and floats
mag(X, N) -> trunc(math:log10(abs(X))) div N * N.

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

drop2([], _DropThis, Acc) -> lists:reverse(Acc);
drop2([E|R], false, Acc)  -> drop2(R, true, [E|Acc]);
drop2([_E|R], true, Acc)  -> drop2(R, false, Acc).

%% get the list of samples collected from the sample_data tuple
get_samples({_Div, _N, _Size, _Cap, Samples}) -> Samples.


%% Tests

char_in_ranges_test() ->
    ?assertNot(char_in_ranges($A, [])),
    ?assertNot(char_in_ranges($A, [{$a, $z}])),
    ?assert(char_in_ranges($A, [{$A, $Z}])),
    ?assert(char_in_ranges($A, [{$A, $Z}, {$a, $z}])).

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
