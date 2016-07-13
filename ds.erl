-module(ds).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% `dataspec' derives a spec of data based on a stream of values.
%% It can be used to eg. `discover' the data type stored in a table
%% (column, etc.)

-export([ add/2
        , new/0
        , new/1
        , setopts/1
        ]).

% DEBUG:
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").

%% Option handling: interface to ds_opts module
getopt(Opt) -> ds_opts:getopt(Opt).
setopts(Opts) -> ds_opts:setopts(Opts).

%% Initialize a new data spec
%% A data spec is a union of sub-specs, represented as a list of terms.
%% Terms are tuples describing various types of data.
%% - {integer, Min, Max, Count}
%% - {float, Min, Max, Count}
%% - {Value, Count}
new() -> [].

%% Initialize, immediately adding one data term
new(Data) -> add(Data, new()).


%% Add an instance of Data to Spec.
%% If Spec already contains Data, do nothing.
%% Else, extend Spec in the smallest possible way to include Data.
%% add/2 is written so it can be used as a function to lists:foldl/3.
add(0, Spec) -> add_value(0, Spec);
add(0.0, Spec) -> add_value(0.0, Spec);
add(I, Spec) when is_integer(I) -> add_numeric(integer, I, Spec);
add(F, Spec) when is_float(F) -> add_numeric(float, F, Spec);
add(A, Spec) when is_atom(A) -> add_atom(A, Spec);
add([], Spec) -> add_value([], Spec);
add({}, Spec) -> add_value({}, Spec);
add(L, Spec) when is_list(L) -> add_list(classify_list(L), L, Spec);
add(T, Spec) when is_tuple(T) -> add_tuple(T, Spec);
add(Data, Spec) -> add_value(Data, Spec).

%% add a numeric under key 'integer' or 'float'
add_numeric(Key, Value, Spec) ->
    %% interpret the 'mag' option as:
    %% - if N = 0: no subgrouping based on magnitude
    %% - if N > 0: group N orders of magnitude into one subspec
    Path = case getopt(mag) of
               0 -> [Key];
               N -> [Key, {mag, mag(Value, N)}]
           end,
    merge_spec(Spec, Path, fun f_merge_num/2, Value).

%% add an atom
add_atom(Atom, Spec) ->
    merge_spec(Spec, [atom, Atom], fun f_merge_count/2, dummy).

%% add a value not in any broader set
add_value(Value, Spec) ->
    merge_spec(Spec, [{value, Value}], fun f_merge_count/2, Value).

add_list(list, L, Spec) -> % generic list is type-tagged with the length of the list
    %% build a spec recursively for all members of the list
    merge_spec(Spec, [{list, length(L)}], fun f_merge_recur/2, L);
add_list(Type, L, Spec) -> % specially classified list, eg. str_alpha
    %% the 'strlen' option enables subgrouping strings by length
    Path = case getopt(strlen) of
               false -> [Type];
               true  -> [Type, {len, length(L)}]
           end,
    merge_spec(Spec, Path, fun f_merge_sample/2, L).

add_tuple(T, Spec) ->
    merge_spec(Spec, [{tuple, tuple_size(T)}], fun f_merge_recur/2, tuple_to_list(T)).

%% spec merge function for counting values
%% spec data: Count
f_merge_count(error, _Data)       -> 1;
f_merge_count({ok, Count}, _Data) -> Count+1.

%% spec merge function for numerals
%% spec data: {Count, Min, Max}
f_merge_num(error, Data)                                   -> {1, Data, Data};
f_merge_num({ok, {Count, Min, Max}}, Data) when Data < Min -> {Count+1, Data, Max};
f_merge_num({ok, {Count, Min, Max}}, Data) when Data > Max -> {Count+1, Min, Data};
f_merge_num({ok, {Count, Min, Max}}, _Data)                -> {Count+1, Min, Max}.

%% spec merge function for sampling: keep count and store a sample of data.
%% spec data: {Count, SampleData}
f_merge_sample(error, Data)             -> {1, sample_data(Data)};
f_merge_sample({ok, {Count, SD}}, Data) -> {Count+1, sample_data(Data, SD)}.

%% spec merge function for recursively spec'ing compound structures
%% spec data: list of subspecs, one per item
f_merge_recur(error, L)       -> lists:map(fun new/1, L);
f_merge_recur({ok, SpecL}, L) -> lists:zipwith(fun add/2, L, SpecL).

%% general hierarchical storage for specs
%%   Spec: spec to modify: [{Key, Value|Subspec}]
%%   Path: list of key terms specifying spec location
%%   Fun: arity 2 merge function to add data to spec:
%%        (error, Data)       -> init_value(Data);
%%        ({ok, Value}, Data) -> merge_value(Value, Data).
%%   Data: term to merge into spec
merge_spec(Spec, [Key], Fun, Data) ->
    %io:format("merge_spec: Spec: ~p  Key: ~p~n", [Spec, Key]),
    orddict:store(Key, Fun(orddict:find(Key, Spec), Data), Spec);
merge_spec(Spec, [Key|Rest], Fun, Data) ->
    %io:format("merge_spec: Spec: ~p  Key: ~p  Rest: ~p~n", [Spec, Key, Rest]),
    Subspec = case orddict:find(Key, Spec) of
                  error          -> merge_spec([], Rest, Fun, Data);
                  {ok, Subspec0} -> merge_spec(Subspec0, Rest, Fun, Data)
              end,
    orddict:store(Key, Subspec, Spec).

%% List classification
%% we try to fit the list into a category as narrow as possible;
%% spec functions to be listed in decreasing order of specificity!
classify_list(L) ->
    SpecList = [ {str_integer, fun is_str_integer/1}
               , {str_alpha, fun is_str_alpha/1}
               , {str_alnum, fun is_str_alnum/1}
               , {str_printable, fun is_str_printable/1}
               ],
    classify_list(L, SpecList).

classify_list(_L, []) -> list; % fallback to generic case
classify_list(L, [{Spec, SpecFun} | Rest]) ->
    case SpecFun(L) of
        false -> classify_list(L, Rest);
        true  -> Spec
    end.

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

%% Algorithm: we have a certain capacity of samples in sample_data,
%% configurable with the option 'samples'. When this buffer is filled,
%% we throw every other element away.  From then on, we put only every
%% other incoming sample in the list.  When sample_data is filled
%% again, we again throw every other away.  From then on, only every
%% fourth incoming sample is put in the list, etc.

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


%% Tests

merge_spec_flat_test() ->
    Spec0 = new(),
    Spec1 = merge_spec(Spec0, [key1], fun f_merge_count/2, data),
    ?assertEqual([{key1, 1}], Spec1),
    Spec2 = merge_spec(Spec1, [key2], fun f_merge_count/2, data),
    ?assertEqual([{key1, 1}, {key2, 1}], Spec2),
    Spec3 = merge_spec(Spec2, [key1], fun f_merge_count/2, data),
    ?assertEqual([{key1, 2}, {key2, 1}], Spec3).

merge_spec_hier_test() ->
    Spec0 = new(),
    Spec1 = merge_spec(Spec0, [key1, key2], fun f_merge_count/2, data),
    ?assertEqual([{key1, [{key2, 1}]}], Spec1),
    Spec2 = merge_spec(Spec1, [key1, key3], fun f_merge_count/2, data),
    ?assertEqual([{key1, [{key2, 1}, {key3, 1}]}], Spec2),
    Spec3 = merge_spec(Spec2, [key1, key2], fun f_merge_count/2, data),
    ?assertEqual([{key1, [{key2, 2}, {key3, 1}]}], Spec3).

add_integer_test() ->
    setopts([{mag, 3}]),
    L = [1, 4, 9, 0, -5],
    Spec = lists:foldl(fun add/2, new(), L),
    ?assertEqual([{integer, [{{mag, 0}, {length(L)-1, lists:min(L), lists:max(L)}}]},
                  {{value, 0}, 1}], Spec).

add_float_test() ->
    setopts([{mag, 3}]),
    L = [1.3, 4.12, 9.99, 0.0, -5.31],
    Spec = lists:foldl(fun add/2, new(), L),
    ?assertEqual([{float, [{{mag, 0}, {length(L)-1, lists:min(L), lists:max(L)}}]},
                  {{value, 0.0}, 1}], Spec).

add_atom_test() ->
    L = [this, that, this],
    Spec = lists:foldl(fun add/2, new(), L),
    ?assertEqual([{atom, [ {that, 1}
                         , {this, 2}
                         ]}], Spec).

add_value_test() ->
    L = [[], 0, []],
    Spec = lists:foldl(fun add/2, new(), L),
    ?assertEqual([ {{value, 0}, 1}
                 , {{value, []}, 2}
                 ], Spec).

add_list_test() ->
    setopts([{mag, 3}]),
    Spec0 = new([1, 2]),
    ?assertEqual([{{list, 2},
                   [[{integer, [{{mag, 0}, {1, 1, 1}}]}],
                    [{integer, [{{mag, 0}, {1, 2, 2}}]}]]}], Spec0),
    Spec1 = add([1, 3], Spec0),
    ?assertEqual([{{list, 2},
                   [[{integer, [{{mag, 0}, {2, 1, 1}}]}],
                    [{integer, [{{mag, 0}, {2, 2, 3}}]}]]}], Spec1).

%% rec_test() ->
%%     R = alma,
%%     record_info(fields, R).

char_in_ranges_test() ->
    ?assertNot(char_in_ranges($A, [])),
    ?assertNot(char_in_ranges($A, [{$a, $z}])),
    ?assert(char_in_ranges($A, [{$A, $Z}])),
    ?assert(char_in_ranges($A, [{$A, $Z}, {$a, $z}])).

classify_list_test() ->
    ?assertEqual(str_integer, classify_list("0123456789")),
    ?assertEqual(str_alpha, classify_list("aAbBcCxXyYzZ")),
    ?assertEqual(str_alnum, classify_list("abcDEF123")),
    ?assertEqual(str_printable, classify_list("This is a (printable) string!")).

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
    ?assertEqual({7,128,16,16,[128,120,112,104,96,88,80,72,64,56,48,40,32,24,16,8]}, SD24).

alma_test() ->
    L = [ 515093038992664081
        , 515073483181164481
        , 40710133123535078
        , 515123497979824288
        , 4893771330
        , 516043709666134184
        , 514102101126374480
        , 514012531138744982
        , 41202195133174076
        , 1295605314
        , 11511467663415974
        , "112565013005069"
        , "11256501300506912"
        , 41205179623328878
        , 514051140156809388
        , 41512717723271171
        , 5084876331
        , 516011927036469480
        , 41103539808650672
        , 514122371907089187
        , 515102101149918989
        , 515083100612458682
        , 1767454231
        , 0
        , 0
        , 513101513560049589
        , 10803491380215771
        , 5351657231
        , 516044483611233488
        , 516023359455077782
        , 82130510420040119108
        , 41204179636652372
        , 11203189241035875
        , 516042101148626683
        , 516012375408070685
        , 11306427851622277
        , 513111012335095181
        , 9370430014003480501610064591118
        , 514051196430019087
        , 2856722207
        , 3532068241
        , 515112375426043087
        , 515122633671538880
        , 516052874282392182
        , 516054195143471488
        , 516043616127968680
        , 516034699560030987
        , 515062926282180981
        , 514072531893693587
        , 514032101102091483
        , []
        , 515082814976436784
        , 515092598709688187
        , 516063937098249081
        , 516053655567274487
        , 1575
        , 41303941909500376
        , 1537406230
        , 4093528241
        , 515083670399122982
        , 514072375461551284
        , 516033359284707185
        , 514122106058832880
        , 0
        , 515073359769705780
        , 516044642381456385
        , 11209429575492670
        , 515032261111073786
        , 3199255210
        , 515083478804586480
        , 516043655518835488
        , 514032215940996685
        , 81160400000250981800
        , 41306702864767377
        , 515033609772960288
        , 1152921521346574788
        , 1247727251
        , 1136006222
        , 515123359417798583
        , 516052845642103980
        , 516043198172236289
        , 0
        , 10905432882512973
        , 41111890009064373
        , 0
        , 11208440689215478
        , 515053359753447989
        , 516013713077383386
        , 516053860313561282
        , 2766633340
        , 516013359261588883
        , 516043359721260986
        , 515073242481260286
        , 515052757828039387
        , 516063693195667788
        , 516024206715895280
        , 11503422450622472
        , 514012535510170583
        , 41108892220601871
        , 41002120712104577
        , 4371992221
        ],
    lists:foldl(fun add/2, new(), L).
