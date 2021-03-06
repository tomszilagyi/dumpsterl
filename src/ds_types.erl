%% -*- coding: utf-8 -*-

%% @private
-module(ds_types).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ kind/1
        , type_to_string/1
        , spec_to_form/1
        , pp_spec/1
        , attributes/2
        , attribute_to_string/2
        , subtype/2
        , ext_new/1
        , ext_add/3
        , ext_join/3
        ]).

-export_type([ ext_data/0 ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("config.hrl").

-ifdef(CONFIG_MAPS).
-define(is_map(X), is_map(X)).
-else.
-define(is_map(X), false).
-endif.

-opaque ext_data() :: list().

%% Category, or "kind", of type.
%% Each type belongs to one of three kinds:
%% - union:    A type which is, by its nature, a union of subtypes.
%%             Such a type cannot be described with an atomic name,
%%             only as a union of other types.
%%             Does not apply to all types that can be broken down,
%%             e.g. integer() can be broken into neg_integer() etc, but
%%             it is not categorized as a union type.
%% - generic:  A type which is parameterized by further types.
%%             E.g. a list is really a list of T where T is the type
%%             of its elements.
%%             Generic types include lists, tuples, records and maps.
%% - ordinary: A type which may or may not be broken into further sub-types.
%%             Any actual data term is either passed on to one of those
%%             sub-types, or, if neither of their predicates is true, is
%%             absorbed by this type.
%%
%% For union and ordinary types, the node's SubSpec is a list of subtypes;
%% for generic types, SubSpec is a list of type specs that map to
%% the individual type elements. In other words, generic types are leaf nodes
%% from a strict type hierarchy point of view, since their children
%% belong to another type domain.
kind(term)          -> union;
kind(list)          -> union;
kind(tuple)         -> union;
kind(nonempty_list) -> generic;
kind(improper_list) -> generic;
kind({tuple, _})    -> generic;
kind({record, _})   -> generic;
kind(map)           -> generic;
kind(_T)            -> ordinary.

%% Convert type to a string representing it in a GUI.
type_to_string({record, {RecName, RecSize}}) ->
    io_lib:format("#~s/~B", [RecName, RecSize]);
type_to_string({tuple, Size}) ->
    io_lib:format("tuple/~B", [Size]);
type_to_string(Type) ->
    io_lib:format("~p", [Type]).

%% Pretty-print a spec (multi-line, full expansion)
%% by converting it to an Erlang abstract type definition form
%% and using the Erlang pretty-printer on the result.
pp_spec(Spec) ->
    Str0 = lists:flatten(erl_pp:attribute(spec_to_form(Spec),
                                          [{encoding, utf8}])),
    "-type t() ::" ++ Str1 = Str0,
    Str2 = string:strip(Str1, right, $\n),
    Str3 = "            " ++ string:strip(Str2, right, $.),
    [First|Rest] = string:tokens(Str3, "\n"),
    Lines = case string:strip(First, both, $ ) of
                "" -> Rest;
                _  -> [First|Rest]
            end,
    StripCount = lists:min([string:span(L, " ") || L <- Lines]),
    string:join([string:substr(L, 1 + StripCount) || L <- Lines],
                "\n") ++ "\n".

%% Convert a spec to an Erlang abstract type definition form
spec_to_form(Spec) ->
    {attribute, anno(1), type, {t, spec_to_form_1(Spec), []}}.

spec_to_form_1({term,_Data, []}) ->
    {type, 1, term, []};
spec_to_form_1({Type,_Data, Children} = Spec) ->
    case kind(Type) of
        union ->
            ChForms = flatten_union([spec_to_form_1(Ch) || Ch <- Children]),
            {type, 1, union, ChForms};
        _ ->
            spec_to_form_2(Spec)
    end.

spec_to_form_2({nonempty_list,_Data, Children}) ->
    {type, 1, list, [spec_to_form_1(Ch) || Ch <- Children]};
spec_to_form_2({improper_list,_Data, Children}) ->
    {type, 1, improper_list, [spec_to_form_1(Ch) || Ch <- Children]};
spec_to_form_2({{tuple,_Size},_Data, Children}) ->
    {type, 1, tuple, [spec_to_form_1(Ch) || Ch <- Children]};
spec_to_form_2({map,_Data,_Children}) ->
    {type, 1, map, any}; %% TODO
spec_to_form_2({<<>>,_Data,_Children}) ->
    {type, 1, binary, [{integer, 1, 0}, {integer, 1, 0}]};
spec_to_form_2({0,_Data,_Children}) ->
    {integer, 1, 0};
spec_to_form_2({{},_Data,_Children}) ->
    {type, 1, tuple, []};
spec_to_form_2({[],_Data,_Children}) ->
    {type, 1, nil, []};
spec_to_form_2({{'fun',_Arity},_Data,_Children}) ->
    {type, 1, 'fun', []};
spec_to_form_2({{record,{RecName, RecSize}}, {_Stats,Ext}, Children}) ->
    Ns = lists:seq(2, RecSize),
    Attributes =
        case Ext of
            [{Attrs,_Locations}|_] -> [A || A <- Attrs];
            [] -> [list_to_atom("field" ++ integer_to_list(N)) || N <- Ns]
        end,
    AttrChL = lists:zip(Attributes, Children),
    FieldSpecL = [{type,1,field_type,
                   [{atom,1,Attr},
                    spec_to_form_1(Ch)]} || {Attr, Ch} <- AttrChL],
    {type,1,record,[{atom,1,RecName} | FieldSpecL]};
spec_to_form_2({Class,_Data,_Children}) ->
    {type, 1, Class, []}.

%% get rid of nested unions
flatten_union(SpecL) ->
    lists:flatmap(fun({type, 1, union, SL}) -> SL;
                     (T) -> [T]
                  end, SpecL).

%% This is needed for the sake of dialyzer correctness.
-ifdef(CONFIG_ERL_ANNO).
anno(LN) -> erl_anno:from_term(LN).
-else.
anno(LN) -> LN.
-endif.


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
attribute_to_string({record, {_RecName,_RecSize}}, {No, ""}) ->
    io_lib:format("E~B", [No]);
attribute_to_string({record, {RecName,_RecSize}}, {No, Attribute}) ->
    io_lib:format("E~B #~s.~s", [No, RecName, Attribute]);
attribute_to_string({tuple,_Size}, {No,_Attribute}) ->
    io_lib:format("E~B", [No]);
attribute_to_string(_Class, {_No, Attribute}) ->
    Attribute.

%% This should be auto-generated by a 'decision tree compiler'
%% that creates compiled Erlang code from a declarative type
%% hierarchy specification.
subtype(V, term) when is_atom(V) -> atom;
subtype(V, term) when is_number(V) -> number;
subtype(V, term) when is_bitstring(V) -> bitstring;
subtype(V, term) when is_pid(V) -> pid;
subtype(V, term) when is_port(V) -> port;
subtype(V, term) when is_reference(V) -> reference;
subtype(V, term) when is_function(V) -> 'fun';
subtype(V, term) when is_list(V) -> list;
subtype(V, term) when is_tuple(V) -> tuple;
subtype(V, term) when ?is_map(V) -> map;

subtype(true, atom) -> boolean;
subtype(false, atom) -> boolean;

subtype(N, number) when is_integer(N) -> integer;
subtype(N, number) when is_float(N) -> float;

subtype(I, integer) when I >= 0 -> non_neg_integer;
subtype(I, integer) when I < 0 -> neg_integer;

subtype(I, non_neg_integer) when I =< 16#10ffff -> char;
subtype(I, char) when I =< 255 -> byte;
subtype(0, byte) -> 0;

subtype(B, bitstring) when is_binary(B) -> binary;

subtype(<<>>, binary) -> <<>>;

subtype(F, 'fun') ->
    {arity, Arity} = erlang:fun_info(F, arity),
    {'fun', Arity};

subtype([], list) -> [];
subtype(L, list) ->
    case is_improper(L) of
        false -> nonempty_list;
        true -> improper_list
    end;

subtype({}, tuple) -> {};
subtype(T, tuple) ->
    case dyn_record(T) of
        {RecName, Size} -> {record, {RecName, Size}};
        false -> {tuple, size(T)}
    end;

%% generic types where we want to further process their elements:
subtype(T, {tuple,_N}) -> {'$fields', tuple_to_list(T)};
subtype(R, {record, {_RecName,_Size}}) -> {'$fields', tl(tuple_to_list(R))};
subtype(L, nonempty_list) -> {'$elements', L};
subtype(L, improper_list) -> improper_list(L);
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
        true  ->
            RecId = {RecName, size(V)},
            case ds_records:lookup(RecId) of
                false -> false;
                _RAs  -> RecId
            end;
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
ext_add(VA, boolean, Ext) -> ext_add_atom(VA, Ext);
ext_add(VA, bitstring, Ext) -> ext_add_bitstring(VA, Ext);
ext_add(VA, binary, Ext) -> ext_add_bitstring(VA, Ext);
ext_add(VA, <<>>, Ext) -> ext_add_bitstring(VA, Ext);
ext_add(VA, nonempty_list, Ext) -> ext_add_nonempty_list(VA, Ext);
ext_add(VA, improper_list, Ext) -> ext_add_improper_list(VA, Ext);

ext_add(_V,_Class, Ext) -> Ext.

%% For atoms, maintain a dictionary of per-value stats for each value
ext_add_atom({V, Attrs}, Ext) ->
    PVS = case orddict:find(V, Ext) of
              error -> ds_pvattrs:new(Attrs);
              {ok, PVS0} -> ds_pvattrs:add(Attrs, PVS0)
          end,
    orddict:store(V, PVS, Ext).

%% For bit strings (including binaries), maintain a histogram of bit size
ext_add_bitstring({V,_Attrs}, Ext) ->
    histogram_add(bit_size(V), Ext).

%% For nonempty lists, maintain a histogram of lengths
ext_add_nonempty_list({V,_Attrs}, Ext) ->
    histogram_add(length(V), Ext).

%% For improper lists, maintain a histogram of the proper parts' lengths
ext_add_improper_list({V,_Attrs}, Ext) ->
    {'$improper_list', ProperL,_Tail} = improper_list(V),
    histogram_add(length(ProperL), Ext).

%% class-specific joins
%% NB. these may be called even with Ext's of different classes,
%% but only if one is a super-class of another. The clause called
%% and Ext1 will correspond to the super-class, Ext2 to the sub-class.
ext_join(atom, Ext1, Ext2) -> ext_join_atom(Ext1, Ext2);
ext_join(boolean, Ext1, Ext2) -> ext_join_atom(Ext1, Ext2);
ext_join(<<>>, Ext1, Ext2) -> histogram_join(Ext1, Ext2);
ext_join(binary, Ext1, Ext2) -> histogram_join(Ext1, Ext2);
ext_join(bitstring, Ext1, Ext2) -> histogram_join(Ext1, Ext2);
ext_join(nonempty_list, Ext1, Ext2) -> histogram_join(Ext1, Ext2);
ext_join(improper_list, Ext1, Ext2) -> histogram_join(Ext1, Ext2);
%% In the general case, different classes' Ext cannot be joined:
ext_join(_Class, Ext1,_Ext2) -> Ext1.


ext_join_atom(Ext1, Ext2) ->
    orddict:merge(fun(_K, V1, V2) -> ds_pvattrs:join(V1, V2) end, Ext1, Ext2).

%% A histogram measuring the occurrence number of factor levels is
%% represented as an orddict of {Count, Factor}
histogram_add(Value, Hist) ->
    orddict:update_counter(Value, 1, Hist).

histogram_join(Hist1, Hist2) ->
    orddict:merge(fun(_K, N1, N2) -> N1 + N2 end, Hist1, Hist2).

%% Tests
-ifdef(TEST).

hierarchy(V) -> hierarchy(V, term, []).

hierarchy(V, Class, TypeList) ->
    case subtype(V, Class) of
        '$null' -> lists:reverse(TypeList);
        SubType -> hierarchy(V, SubType, [SubType|TypeList])
    end.

subtype_test() ->
    %% atomic types
    ?assertEqual([atom], hierarchy(a)),
    ?assertEqual([atom, boolean], hierarchy(true)),
    ?assertEqual([atom, boolean], hierarchy(false)),
    ?assertEqual([number, integer, non_neg_integer, char, byte, 0],
                 hierarchy(0)),
    ?assertEqual([number, integer, non_neg_integer],
                 hierarchy(12345678)),
    ?assertEqual([number, integer, neg_integer], hierarchy(-5)),
    ?assertEqual([number, integer, neg_integer], hierarchy(-99999999)),
    ?assertEqual([number, integer, non_neg_integer, char],
                 hierarchy(16#10ffff)),
    ?assertEqual([number, integer, non_neg_integer, char, byte],
                 hierarchy(255)),
    ?assertEqual([number, float], hierarchy(123.45678)),
    ?assertEqual([bitstring], hierarchy(<<1,17,42:12>>)),
    ?assertEqual([bitstring, binary], hierarchy(<<1,17,42>>)),
    ?assertEqual([bitstring, binary], hierarchy(<<"this is a binary">>)),
    ?assertEqual([bitstring, binary, <<>>], hierarchy(<<>>)),
    ?assertEqual([pid], hierarchy(self())),
    ?assertEqual([port], hierarchy(hd(erlang:ports()))),
    ?assertEqual([reference], hierarchy(erlang:make_ref())),
    ?assertEqual(['fun', {'fun',0}], hierarchy(fun() -> ok end)),
    ?assertEqual(['fun', {'fun',1}], hierarchy(fun(_) -> ok end)),
    ?assertEqual(['fun', {'fun',2}], hierarchy(fun(_,_) -> ok end)),
    ?assertEqual(['fun', {'fun',3}], hierarchy(fun(_,_,_) -> ok end)),
    ?assertEqual(['fun', {'fun',4}], hierarchy(fun(_,_,_,_) -> ok end)),
    ?assertEqual([list, []], hierarchy([])),
    ?assertEqual([tuple, {}], hierarchy({})),

    %% generic types
    ?assertEqual([list, nonempty_list, {'$elements', [a,b,c]}],
                 hierarchy([a,b,c])),
    ?assertEqual([list, improper_list, {'$improper_list', [b,a], c}],
                 hierarchy([a,b|c])),
    ?assertEqual([tuple, {tuple,3}, {'$fields', [123,456,789]}],
                 hierarchy({123,456,789})),

    %% records are classified as such only if their attributes are known
    ?assertEqual([tuple, {tuple,3}, {'$fields', [abc,123,"sss"]}],
                 hierarchy({abc,123,"sss"})),
    ds_records:put_attrs([{{abc,3}, [{[f1, f2], dummy_source_list}]}]),
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

    E5 = ext_add({[some,items], []}, C, E0),
    E6 = ext_add({[x], []}, C, E5),
    E7 = ext_add({[y], []}, C, E6),
    E8 = ext_add({[z], []}, C, E7),
    ?assertEqual([{1,3}, {2,1}], E8),

    E9 = ext_join(C, E4, E8),
    ?assertEqual([{1,4}, {2,2}, {3,2}], E9).

ext_improper_list_test() ->
    C = improper_list,
    E0 = ext_new(C),
    E1 = ext_add({[a,b,c|tail1], []}, C, E0),
    E2 = ext_add({[6,7,8|tail2], []}, C, E1),
    E3 = ext_add({[9,10|tail3], []}, C, E2),
    E4 = ext_add({[list_item|tail4], []}, C, E3),
    ?assertEqual([{1,1}, {2,1}, {3,2}], E4),

    E5 = ext_add({[some,items|tail5], []}, C, E0),
    E6 = ext_add({[x|tail6], []}, C, E5),
    E7 = ext_add({[y|tail7], []}, C, E6),
    E8 = ext_add({[z|tail8], []}, C, E7),
    ?assertEqual([{1,3}, {2,1}], E8),

    E9 = ext_join(C, E4, E8),
    ?assertEqual([{1,4}, {2,2}, {3,2}], E9).

form_test_spec_children() ->
    [{atom, data, []},
     {pos_integer, data, []},
     {[], data, []},
     {<<>>, data, []},
     {{}, data, []},
     {nonempty_list, data,
      [{{tuple,2}, data,
        [{atom, data, []},
         {0, data, []}]}]},
     {improper_list, data,
      [{term, data,
        [{atom, data, []},
         {float, data, []}]},
       {atom, data, []}]},
     {{record, {widget1,4}}, {stats, []},
      [{number, data, []},
       {atom, data, []},
       {byte, data, []}]},
     {{record, {widget2,4}},
      {stats, [{[count, type, status],
                attr_source_locations}]},
      [{number, data, []},
       {atom, data, []},
       {byte, data, []}]},
     {tuple, data,
      [{{tuple,2}, data,
        [{atom, data, []},
         {0, data, []}]},
       {{tuple,3}, data,
        [{atom, data, []},
         {0, data, []},
         {float, data, []}]}]}].

union_form_no_maps() ->
    [{type,1,atom,[]},
     {type,1,pos_integer,[]},
     {type,1,nil,[]},
     {type,1,binary,[{integer,1,0},{integer,1,0}]},
     {type,1,tuple,[]},
     {type,1,list,
      [{type,1,tuple,
        [{type,1,atom,[]},
         {integer,1,0}]}]},
     {type,1,improper_list,
      [{type,1,union,
        [{type,1,atom,[]},
         {type,1,float,[]}]},
       {type,1,atom,[]}]},
     {type,1,record,
      [{atom,1,widget1},
       {type,1,field_type,[{atom,1,field2},{type,1,number,[]}]},
       {type,1,field_type,[{atom,1,field3},{type,1,atom,[]}]},
       {type,1,field_type,[{atom,1,field4},{type,1,byte,[]}]}]},
     {type,1,record,
      [{atom,1,widget2},
       {type,1,field_type,[{atom,1,count},{type,1,number,[]}]},
       {type,1,field_type,[{atom,1,type},{type,1,atom,[]}]},
       {type,1,field_type,[{atom,1,status},{type,1,byte,[]}]}]},
     {type,1,tuple,[{type,1,atom,[]},{integer,1,0}]},
     {type,1,tuple,
      [{type,1,atom,[]},{integer,1,0},{type,1,float,[]}]}].

-ifdef(CONFIG_MAPS).

form_test_spec() ->
    {term, data,
     [ {map, data, []}
     | form_test_spec_children()
     ]}.

union_form() ->
    [ {type,1,map,any}
    | union_form_no_maps()
    ].

-else.

form_test_spec() ->
    {term, data, form_test_spec_children()}.

union_form() ->
    union_form_no_maps().

-endif.

spec_to_form_test() ->
    ?assertEqual(
       {attribute,1,type,
        {t,
         {type,1,union,union_form()},
         []}},
       spec_to_form(form_test_spec())).

-ifdef(CONFIG_PP_NEW).
pp_spec_test() ->
    ?assertEqual("term()\n", pp_spec(ds_spec:new())),
    ?assertEqual(
       "map() |\n"
       "atom() |\n"
       "pos_integer() |\n"
       "[] |\n"
       "<<>> |\n"
       "{} |\n"
       "[{atom(), 0}] |\n"
       "improper_list(atom() | float(), atom()) |\n"
       "#widget1{field2 :: number(),\n"
       "         field3 :: atom(),\n"
       "         field4 :: byte()} |\n"
       "#widget2{count :: number(), type :: atom(), status :: byte()} |\n"
       "{atom(), 0} |\n"
       "{atom(), 0, float()}\n",
       pp_spec(form_test_spec())).
-else.
-ifdef(CONFIG_MAPS).
pp_spec_test() ->
    ?assertEqual("term()\n", pp_spec(ds_spec:new())),
    ?assertEqual(
       "  map()\n"
       "| atom()\n"
       "| pos_integer()\n"
       "| []\n"
       "| <<>>\n"
       "| {}\n"
       "| [{atom(), 0}]\n"
       "| improper_list(atom() | float(), atom())\n"
       "| #widget1{field2 :: number(),\n"
       "           field3 :: atom(),\n"
       "           field4 :: byte()}\n"
       "| #widget2{count :: number(),\n"
       "           type :: atom(),\n"
       "           status :: byte()}\n"
       "| {atom(), 0}\n"
       "| {atom(), 0, float()}\n",
       pp_spec(form_test_spec())).
-else.
pp_spec_test() ->
    ?assertEqual("term()\n", pp_spec(ds_spec:new())),
    ?assertEqual(
       "  atom()\n"
       "| pos_integer()\n"
       "| []\n"
       "| <<>>\n"
       "| {}\n"
       "| [{atom(), 0}]\n"
       "| improper_list(atom() | float(), atom())\n"
       "| #widget1{field2 :: number(),\n"
       "           field3 :: atom(),\n"
       "           field4 :: byte()}\n"
       "| #widget2{count :: number(),\n"
       "           type :: atom(),\n"
       "           status :: byte()}\n"
       "| {atom(), 0}\n"
       "| {atom(), 0, float()}\n",
       pp_spec(form_test_spec())).
-endif.
-endif.

-endif.
