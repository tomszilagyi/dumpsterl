-module(ds_types).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ types/0
        , priv_data/1
        , priv_data/3
        ]).

-import(ds_opts, [ getopt/1
                 ]).

-include_lib("eunit/include/eunit.hrl").

%% The type hierarchy is defined by types()
%% - subtypes are mutually exclusive
%% - dynamically created subtypes denoted by
%%     {'$dynamic', Tag, Fun}
%%   where the actual subtype is derived as {Tag, Fun(Data)}
%%   (if Fun(Data) returns false, the next subtype is tried)
%% - dynamic subtypes must be last in the list of subtypes

types() ->
    [ {'T'
      , [ {numeric, fun erlang:is_number/1}
        , {atom, fun erlang:is_atom/1}
        , {list, fun erlang:is_list/1}
        , {tuple, fun erlang:is_tuple/1}
        , {binary, fun erlang:is_binary/1}
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
    , {tuple
      , [ {empty, fun(T) -> T =:= {} end}
        , {'$dynamic', record, fun dyn_record/1}
        , {'$dynamic', tuple, fun size/1}
        ]}
    , {str_printable
      , [ {str_alnum, fun is_str_alnum/1}
        ] ++ opt_strlen_stub(getopt(strlen)) }
    , {str_alnum
      , [ {str_integer, fun is_str_integer/1}
        , {str_alpha, fun is_str_alpha/1}
        ] ++ opt_strlen_stub(getopt(strlen)) }
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

%% dynamically classify records; also tagged with size because
%% we can never be sure that a tuple is actually a record.
%% This way, we might tag ordinary tuples as records (if they
%% have an atom as the first element) but will at least always
%% group different record types separately.
dyn_record(V) when is_tuple(V) andalso size(V) > 1 ->
    RecName = element(1, V),
    case is_atom(RecName) of
        true  -> {RecName, size(V)};
        false -> false
    end;
dyn_record(_V) -> false.

%% string classifier functions
is_str_integer(S) -> string_in_ranges(S, [{$0, $9}]).
is_str_alpha(S) -> string_in_ranges(S, [{$A, $Z}, {$a, $z}]).
is_str_alnum(S) -> string_in_ranges(S, [{$0, $9}, {$A, $Z}, {$a, $z}]).
is_str_printable(S) -> string_in_ranges(S, [{32, 127}]).

%% check that string consists of chars that fit in the given ranges
string_in_ranges([],_Ranges)   -> true;
string_in_ranges([C|L], Ranges) ->
    case char_in_ranges(C, Ranges) of
        false -> false;
        true  -> string_in_ranges(L, Ranges)
    end.

%% check that char C is in one of the ranges spec'd as [{Min, Max}].
char_in_ranges(_C, []) -> false;
char_in_ranges(C, [{Min, Max} | _]) when C >= Min, C =< Max -> true;
char_in_ranges(C, [{_Min,_Max} | Rest]) -> char_in_ranges(C, Rest).

%% order of magnitude for integers and floats
mag(X, N) -> trunc(math:log10(abs(X))) div N * N.


%% class-specific private data

%% class-specific initializer
priv_data(_Class) -> [].

priv_data(V, atom, PD) -> priv_data_atom(V, PD);
priv_data(V, {list,_N}, PD) -> priv_data_recur(V, PD);
priv_data({V, Attrs}, {tuple,_N}, PD) ->
    priv_data_recur({tuple_to_list(V), Attrs}, PD);
priv_data({V, Attrs}, {record,{_RecName,_Size}}, PD) ->
    priv_data_recur({tl(tuple_to_list(V)), Attrs}, PD);
priv_data(_V,_Class, PrivData) -> PrivData.

%% For atoms, maintain a dictionary of per-value stats for each value
priv_data_atom({V, Attrs}, PD) ->
    PVS = case orddict:find(V, PD) of
              error -> ds_stats:pvs_new({V, Attrs});
              {ok, PVS0} -> ds_stats:pvs_add({V, Attrs}, PVS0)
          end,
    orddict:store(V, PVS, PD).

%% For lists and tuples, maintain a list of type specs for
%% each list/tuple element position.
priv_data_recur({V, Attrs}, PD) when is_tuple(V) ->
    priv_data_recur({tuple_to_list(V), Attrs}, PD);
priv_data_recur({V, Attrs}, []) ->
    lists:map(fun(Vi) -> ds:new('T', {Vi, Attrs}) end, V);
priv_data_recur({V, Attrs}, PD) ->
    lists:map(fun({Vi, S}) -> ds:add({Vi, Attrs}, S) end, lists:zip(V, PD)).


%% Tests

char_in_ranges_test() ->
    ?assertNot(char_in_ranges($A, [])),
    ?assertNot(char_in_ranges($A, [{$a, $z}])),
    ?assert(char_in_ranges($A, [{$A, $Z}])),
    ?assert(char_in_ranges($A, [{$A, $Z}, {$a, $z}])).
