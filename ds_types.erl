-module(ds_types).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ kind/1
        , subtype/2
        , ext_data/1
        , ext_data/3
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

%% TODO NB. something like this would be the input to the
%% 'decision tree compiler' that outputs code equivalent to
%% subtype/2.
-ifdef(discarded).
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
-endif.

%% Category, or "kind", of type.
%% Each type belongs to one of three kinds:
%% - abstract: a type which can be broken into subtypes;
%% - complex:  a type which is a leaf type, but can be broken down
%%             into elements e.g. lists, tuples, records;
%% - leaf:     a leaf type with no further sub-categorization.
%%
%% For abstract types, the node's SubSpec is a list of subtypes;
%% for complex types, SubSpec is a list of type specs that map to
%% the individual elements. In other words, those are leaf nodes
%% from a type hierarchy point of view; their children belong to
%% another type.
kind(nonempty_list) -> complex;
kind({tuple, _}) -> complex;
kind({record, _}) -> complex;
kind(_T) -> abstract.

%% This should be auto-generated by a 'decision tree compiler'
%% that creates compiled Erlang code from the above (or similar)
%% declarative type hierarchy specification.
%% Compilation should also optimize away the getopt/1 calls,
%% taking a concrete options configuration into account.
subtype([], 'T') -> [];
subtype({}, 'T') -> {};
subtype(V, 'T') when is_number(V) -> number;
subtype(V, 'T') when is_atom(V) -> atom;
subtype(V, 'T') when is_list(V) -> list;
subtype(V, 'T') when is_tuple(V) -> tuple;
subtype(V, 'T') when is_binary(V) -> binary;

subtype(N, number) when is_integer(N) -> integer;
subtype(N, number) when is_float(N) -> float;

subtype(I, integer) when I >= 0 -> non_neg_integer;
subtype(I, integer) when I < 0 -> neg_integer;

subtype(0, non_neg_integer) -> 0;
subtype(_I, non_neg_integer) -> pos_integer;
subtype(I, pos_integer) when I =< 16#10ffff -> char;
subtype(I, char) when I =< 255 -> byte;

subtype(I, IntType) when IntType =:= pos_integer;
                         IntType =:= non_neg_integer;
                         IntType =:= neg_integer ->
    case getopt(mag) of
        0 -> '$null';
        N -> {IntType, {mag, mag(I, N)}}
    end;

subtype(F, float) ->
    case getopt(mag) of
        0 -> '$null';
        N -> {float, {mag, mag(F, N)}}
    end;

subtype(L, list) ->
    case is_improper(L) of
        false -> nonempty_list;
        true -> improper_list
    end;

subtype(L, nonempty_list) -> {'$elements', L};
    %% %% FIXME can't have a complex type with potential subtype!
    %% case is_str_printable(L) of
    %%     true -> str_printable;
    %%     false -> {'$elements', L}
    %% end;
subtype(L, improper_list) -> improper_list(L);

subtype(T, tuple) ->
    case dyn_record(T) of
        {RecName, Size} -> {record, {RecName, Size}};
        false -> {tuple, size(T)}
    end;

subtype(S, str_printable) ->
    case is_str_alnum(S) of
        true -> str_alnum;
        false ->
            case getopt(strlen) of
                true -> {str_printable, {len, length(S)}};
                false -> '$null'
            end
    end;

subtype(S, str_alnum) ->
    case is_str_integer(S) of
        true -> str_integer;
        false ->
            case is_str_alpha(S) of
                true -> str_alpha;
                false ->
                    case getopt(strlen) of
                        true -> {str_alnum, {len, length(S)}};
                        false -> '$null'
                    end
            end
    end;

subtype(S, str_integer) ->
    case getopt(strlen) of
        true -> {str_integer, {len, length(S)}};
        false -> '$null'
    end;

subtype(S, str_alpha) ->
    case getopt(strlen) of
        true -> {str_alpha, {len, length(S)}};
        false -> '$null'
    end;

%% compound types where we want to recurse on their elements:
subtype(T, {tuple,_N}) -> {'$fields', tuple_to_list(T)};
subtype(R, {record, {_RecName,_Size}}) -> {'$fields', tl(tuple_to_list(R))};

subtype(_V, _Class) -> '$null'.

-ifdef(discarded).
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
-endif.

%% Find out whether given (nonempty) list is improper or not.
is_improper([]) -> false;
is_improper([_H|T]) -> is_improper(T);
is_improper(_T) -> true.

improper_list(L) -> improper_list(L, []).

improper_list([], _Acc) -> nonempty_list;
improper_list([H|T], Acc) -> improper_list(T, [H|Acc]);
improper_list(T, Acc) -> {'$improper_list', Acc, T}.

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
is_str_printable(S) -> string_in_ranges(S, [$\r, $\n, $\t, {32, 126}, {160, 255}]).

%% check that string consists of chars that fit in the given ranges
string_in_ranges([],_Ranges)   -> true;
string_in_ranges([C|L], Ranges) ->
    case char_in_ranges(C, Ranges) of
        false -> false;
        true  -> string_in_ranges(L, Ranges)
    end.

%% check that char C is in one of the ranges spec'd as [{Min, Max}|Char].
char_in_ranges(_C, []) -> false;
char_in_ranges(C, [{Min, Max} | _]) when C >= Min, C =< Max -> true;
char_in_ranges(C, [{_Min,_Max} | Rest]) -> char_in_ranges(C, Rest);
char_in_ranges(C, [C | _]) -> true;
char_in_ranges(C, [_Char | Rest]) -> char_in_ranges(C, Rest).

%% order of magnitude for integers and floats
mag(X, N) -> trunc(math:log10(abs(X))) div N * N.


%% class-specific extra data

%% class-specific initializers
ext_data({record, RecId}) ->
    case ds_records:lookup(RecId) of
        false -> [];
        RAs -> RAs
    end;
ext_data(_Class) -> [].

%% class-specific per-term updaters
ext_data(VA, atom, Ext) -> ext_data_atom(VA, Ext);
ext_data(_V,_Class, Ext) -> Ext.


%% For atoms, maintain a dictionary of per-value stats for each value
ext_data_atom({V, Attrs}, Ext) ->
    PVS = case orddict:find(V, Ext) of
              error -> ds_stats:pvs_new({V, Attrs});
              {ok, PVS0} -> ds_stats:pvs_add({V, Attrs}, PVS0)
          end,
    orddict:store(V, PVS, Ext).

%% Tests

char_in_ranges_test() ->
    ?assertNot(char_in_ranges($A, [])),
    ?assertNot(char_in_ranges($A, [{$a, $z}])),
    ?assertNot(char_in_ranges($\n, [{$a, $z}])),
    ?assert(char_in_ranges($\n, [{$a, $z}, $\n])),
    ?assert(char_in_ranges($A, [{$A, $Z}])),
    ?assert(char_in_ranges($A, [{$A, $Z}, {$a, $z}])).
