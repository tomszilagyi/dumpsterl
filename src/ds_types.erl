-module(ds_types).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ kind/1
        , type_to_string/1
        , attributes/2
        , attribute_to_string/2
        , subtype/2
        , ext_new/1
        , ext_add/3
        , ext_join/3
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("config.hrl").

-ifdef(CONFIG_MAPS).
-define(is_map(X), is_map(X)).
-else.
-define(is_map(X), false).
-endif.

%% Category, or "kind", of type.
%% Each type belongs to one of three kinds:
%% - abstract: a type which can be broken into subtypes;
%% - generic:  a type which is parameterized by further types.
%%             E.g. a list is really a list of T where T is the type
%%             of all its elements.
%%             Generic types include lists, tuples, records.
%% - leaf:     a leaf type with no further sub-categorization.
%%
%% For abstract types, the node's SubSpec is a list of subtypes;
%% for generic types, SubSpec is a list of type specs that map to
%% the individual type elements. In other words, those are leaf nodes
%% from a strict type hierarchy point of view, since their children
%% belong to another type domain.
kind(nonempty_list) -> generic;
kind(improper_list) -> generic;
kind({tuple, _})    -> generic;
kind({record, _})   -> generic;
kind(map)           -> generic;
kind(_T) -> abstract. %% incomplete for now, but we only really care
%% about the difference between generic and non-generic types.

%% Convert type to a string representing it in a GUI.
type_to_string({record, {RecName, RecSize}}) ->
    io_lib:format("#~s/~B", [RecName, RecSize]);
type_to_string({tuple, Size}) ->
    io_lib:format("tuple/~B", [Size]);
type_to_string(Type) ->
    io_lib:format("~p", [Type]).

%% For generic types, this function returns a list of attributes.
%% The list contains {No, Attribute} tuples, where No is an integer
%% and Attribute is a (possibly empty) string.
%% NB. In case of records, the fields are numbered from 2.
attributes({tuple, N}, _Data) ->
    Ns = lists:seq(1, N),
    lists:zip(Ns, ["" || _F <- Ns]);
attributes({record, {_RecName, RecSize}}, {_Stats, Ext}) ->
    Ns = lists:seq(2, RecSize),
    Attributes =
        case Ext of
            [{Attrs,_Locations}|_] -> [atom_to_list(A) || A <- Attrs];
            [] -> ["" || _F <- Ns]
        end,
    lists:zip(Ns, Attributes);
attributes(map, {_Stats, Attrs}) ->
    Ns = lists:seq(1, length(Attrs)),
    Attributes = [io_lib:format("~p", [A]) || A <- Attrs],
    lists:zip(Ns, Attributes);
attributes(nonempty_list,_Data) ->
    [{1, "items"}];
attributes(improper_list,_Data) ->
    [{1, "items"}, {2, "tail"}].

%% Convert attribute to a string representing it in a GUI.
attribute_to_string({record, {RecName,_RecSize}}, {No, Attribute}) ->
    io_lib:format("E~B #~s.~s", [No, RecName, Attribute]);
attribute_to_string({tuple,_Size}, {No,_Attribute}) ->
    io_lib:format("E~B", [No]);
attribute_to_string(_Class, {_No, Attribute}) ->
    Attribute.

%% This should be auto-generated by a 'decision tree compiler'
%% that creates compiled Erlang code from a declarative type
%% hierarchy specification.
subtype([], 'T') -> [];
subtype({}, 'T') -> {};
subtype(<<>>, 'T') -> <<>>;
subtype(V, 'T') when is_atom(V) -> atom;
subtype(V, 'T') when is_number(V) -> number;
subtype(V, 'T') when is_bitstring(V) -> bitstring;
subtype(V, 'T') when is_pid(V) -> pid;
subtype(V, 'T') when is_port(V) -> port;
subtype(V, 'T') when is_reference(V) -> reference;
subtype(V, 'T') when is_function(V) -> 'fun';
subtype(V, 'T') when is_list(V) -> list;
subtype(V, 'T') when is_tuple(V) -> tuple;
subtype(V, 'T') when ?is_map(V) -> map;

subtype(true, atom) -> boolean;
subtype(false, atom) -> boolean;

subtype(N, number) when is_integer(N) -> integer;
subtype(N, number) when is_float(N) -> float;

subtype(I, integer) when I >= 0 -> non_neg_integer;
subtype(I, integer) when I < 0 -> neg_integer;

subtype(0, non_neg_integer) -> 0;
subtype(_I, non_neg_integer) -> pos_integer;
subtype(I, pos_integer) when I =< 16#10ffff -> char;
subtype(I, char) when I =< 255 -> byte;

subtype(B, bitstring) when is_binary(B) -> binary;

subtype(F, 'fun') ->
    {arity, Arity} = erlang:fun_info(F, arity),
    {'fun', Arity};

subtype(L, list) ->
    case is_improper(L) of
        false -> nonempty_list;
        true -> improper_list
    end;

subtype(L, nonempty_list) -> {'$elements', L};
subtype(L, improper_list) -> improper_list(L);

subtype(T, tuple) ->
    case dyn_record(T) of
        {RecName, Size} -> {record, {RecName, Size}};
        false -> {tuple, size(T)}
    end;

%% compound types where we want to recurse on their elements:
subtype(T, {tuple,_N}) -> {'$fields', tuple_to_list(T)};
subtype(R, {record, {_RecName,_Size}}) -> {'$fields', tl(tuple_to_list(R))};
subtype(M, map) -> subtype_map(M); % separate function for the sake of -ifdef.

subtype(_V, _Class) -> '$null'.

-ifdef(CONFIG_MAPS).
subtype_map(Map) -> {'$attrs', lists:sort(maps:to_list(Map))}.
-else.
subtype_map(_Map) -> '$null'.
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
%% group different types separately.
dyn_record(V) when is_tuple(V) andalso size(V) > 1 ->
    RecName = element(1, V),
    case is_atom(RecName) of
        true  -> {RecName, size(V)};
        false -> false
    end;
dyn_record(_V) -> false.


%% class-specific extra data

%% class-specific initializers
ext_new({record, RecId}) ->
    case ds_records:lookup(RecId) of
        false -> [];
        RAs -> RAs
    end;
ext_new(_Class) -> [].

%% class-specific per-term updaters
ext_add(VA, atom, Ext) -> ext_add_atom(VA, Ext);
ext_add(VA, nonempty_list, Ext) -> ext_add_nonempty_list(VA, Ext);
ext_add(_V,_Class, Ext) -> Ext.

%% For atoms, maintain a dictionary of per-value stats for each value
ext_add_atom({V, Attrs}, Ext) ->
    PVS = case orddict:find(V, Ext) of
              error -> ds_pvattrs:new(Attrs);
              {ok, PVS0} -> ds_pvattrs:add(Attrs, PVS0)
          end,
    orddict:store(V, PVS, Ext).

%% For lists, maintain a histogram of lengths
ext_add_nonempty_list({V,_Attrs}, Ext) ->
    orddict:update_counter(length(V), 1, Ext).

%% class-specific joins
ext_join(atom, Ext1, Ext2) -> ext_join_atom(Ext1, Ext2);
ext_join(nonempty_list, Ext1, Ext2) -> ext_join_nonempty_list(Ext1, Ext2);
ext_join(_Class, Ext1,_Ext2) -> Ext1.

ext_join_atom(Ext1, Ext2) ->
    orddict:merge(fun(_K, V1, V2) -> ds_pvattrs:join(V1, V2) end, Ext1, Ext2).

ext_join_nonempty_list(Ext1, Ext2) ->
    orddict:merge(fun(_K, N1, N2) -> N1 + N2 end, Ext1, Ext2).

%% Tests
-ifdef(TEST).

hierarchy(V) -> hierarchy(V, 'T', []).

hierarchy(V, Class, TypeList) ->
    case subtype(V, Class) of
        '$null' -> lists:reverse(TypeList);
        SubType -> hierarchy(V, SubType, [SubType|TypeList])
    end.

subtype_test() ->
    %% atomic types
    ?assertEqual([[]], hierarchy([])),
    ?assertEqual([{}], hierarchy({})),
    ?assertEqual([<<>>], hierarchy(<<>>)),
    ?assertEqual([atom], hierarchy(a)),
    ?assertEqual([atom, boolean], hierarchy(true)),
    ?assertEqual([atom, boolean], hierarchy(false)),
    ?assertEqual([number, integer, non_neg_integer, 0],
                 hierarchy(0)),
    ?assertEqual([number, integer, non_neg_integer, pos_integer],
                 hierarchy(12345678)),
    ?assertEqual([number, integer, neg_integer], hierarchy(-5)),
    ?assertEqual([number, integer, neg_integer], hierarchy(-99999999)),
    ?assertEqual([number, integer, non_neg_integer, pos_integer, char],
                 hierarchy(16#10ffff)),
    ?assertEqual([number, integer, non_neg_integer, pos_integer, char, byte],
                 hierarchy(255)),
    ?assertEqual([number, float], hierarchy(123.45678)),
    ?assertEqual([bitstring], hierarchy(<<1,17,42:12>>)),
    ?assertEqual([bitstring, binary], hierarchy(<<1,17,42>>)),
    ?assertEqual([bitstring, binary], hierarchy(<<"this is a binary">>)),
    ?assertEqual([pid], hierarchy(self())),
    ?assertEqual([port], hierarchy(hd(erlang:ports()))),
    ?assertEqual([reference], hierarchy(erlang:make_ref())),
    ?assertEqual(['fun', {'fun',0}], hierarchy(fun() -> ok end)),
    ?assertEqual(['fun', {'fun',1}], hierarchy(fun(_) -> ok end)),
    ?assertEqual(['fun', {'fun',2}], hierarchy(fun(_,_) -> ok end)),
    ?assertEqual(['fun', {'fun',3}], hierarchy(fun(_,_,_) -> ok end)),
    ?assertEqual(['fun', {'fun',4}], hierarchy(fun(_,_,_,_) -> ok end)),

    %% generic types
    ?assertEqual([list, nonempty_list, {'$elements', [a,b,c]}],
                 hierarchy([a,b,c])),
    ?assertEqual([list, improper_list, {'$improper_list', [b,a], c}],
                 hierarchy([a,b|c])),
    ?assertEqual([tuple, {tuple,3}, {'$fields', [123,456,789]}],
                 hierarchy({123,456,789})),
    ?assertEqual([tuple, {record, {abc,3}}, {'$fields', [123,"sss"]}],
                 hierarchy({abc,123,"sss"})).

-ifdef(CONFIG_MAPS).

subtype_maps_test() ->
    ?assertEqual([map, {'$attrs', [{key1, value1}, {key2, value2}]}],
                 hierarchy(#{key1 => value1, key2 => value2})).

-endif.

ext_atom_test() ->
    C = atom,
    E0 = ext_new(C),
    E1 = ext_add({one, [{ts, 0}, {key, x}]}, C, E0),
    E2 = ext_add({two, [{ts, 0}, {key, w}]}, C, E1),
    E3 = ext_add({one, [{ts, 3}, {key, a}]}, C, E2),

    [{one, PV1}, {two, PV2}] = E3,
    ?assertEqual(2, ds_pvattrs:get_count(PV1)),
    ?assertEqual({{0,x}, {3,a}}, ds_pvattrs:get_timespan(PV1)),
    ?assertEqual(1, ds_pvattrs:get_count(PV2)),
    ?assertEqual({{0,w}, {0,w}}, ds_pvattrs:get_timespan(PV2)),

    E4 = ext_add({one, [{ts, 5}, {key, p}]}, C, E0),
    E5 = ext_add({zzz, [{ts, 8}, {key, q}]}, C, E4),
    E6 = ext_add({zzz, [{ts, 8}, {key, r}]}, C, E5),
    [{one, PV3}, {zzz, PV4}] = E6,
    ?assertEqual(1, ds_pvattrs:get_count(PV3)),
    ?assertEqual({{5,p}, {5,p}}, ds_pvattrs:get_timespan(PV3)),
    ?assertEqual(2, ds_pvattrs:get_count(PV4)),
    ?assertEqual({{8,q}, {8,r}}, ds_pvattrs:get_timespan(PV4)),

    E7 = ext_join(C, E3, E6),
    [{one, PV5}, {two, PV6}, {zzz, PV7}] = E7,
    ?assertEqual(3, ds_pvattrs:get_count(PV5)),
    ?assertEqual({{0,x}, {5,p}}, ds_pvattrs:get_timespan(PV5)),
    ?assertEqual(1, ds_pvattrs:get_count(PV6)),
    ?assertEqual({{0,w}, {0,w}}, ds_pvattrs:get_timespan(PV6)),
    ?assertEqual(2, ds_pvattrs:get_count(PV7)),
    ?assertEqual({{8,q}, {8,r}}, ds_pvattrs:get_timespan(PV7)).

ext_nonempty_list_test() ->
    C = nonempty_list,
    E0 = ext_new(C),
    E1 = ext_add({[a,b,c], []}, C, E0),
    E2 = ext_add({[6,7,8], []}, C, E1),
    E3 = ext_add({[9,10], []}, C, E2),
    E4 = ext_add({[list_item], []}, C, E3),
    ?assertEqual([{1,1}, {2,1}, {3,2}], E4),

    E5 = ext_add({[some, items], []}, C, E0),
    E6 = ext_add({[x], []}, C, E5),
    E7 = ext_add({[y], []}, C, E6),
    E8 = ext_add({[z], []}, C, E7),
    ?assertEqual([{1,3}, {2,1}], E8),

    E9 = ext_join(C, E4, E8),
    ?assertEqual([{1,4}, {2,2}, {3,2}], E9).

-endif.
