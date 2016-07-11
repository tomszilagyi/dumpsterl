-module(dataspec).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% This module derives a spec of data based on a stream of values.
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

%% Example: go through an ets table to spec a field based on a limited
%% number of rows processed:
%%   fold_tab_field(my_ets_table, #my_ets_record.my_field, 1000)

-define(ex, ex).
-ifdef(ex).
fold_tab_field(Tab, FieldNo, Limit) ->
    fold_tab_field(Tab, FieldNo, ets:first(Tab), new(), Limit).

fold_tab_field(Tab, FieldNo, Limit, Opts) ->
    setopts(Opts),
    fold_tab_field(Tab, FieldNo, ets:first(Tab), new(), Limit).

fold_tab_field(_Tab, _FieldNo, _Key, Spec, 0) -> Spec;
fold_tab_field(_Tab, _FieldNo, '$end_of_table', Spec, _Limit) -> Spec;
fold_tab_field(Tab, FieldNo, Key, Spec0, Limit) ->
    [Rec] = ets:lookup(Tab, Key),
    Data = element(FieldNo, Rec),
    %io:format("Data: ~p~n", [Data]),
    Spec = add(Data, Spec0),
    fold_tab_field(Tab, FieldNo, ets:next(Tab, Key), Spec, Limit-1).
-endif.


%% Supported options:
%%
%%   mag: integer()
%%     control subgrouping of numerals by magnitude
%%        0: turn off subgrouping;
%%        N: subgroup N orders of magnitude in one; defults to 3
%%
%%   strlen: true | false
%%     control subgrouping of strings by length
%%
default_opts() ->
    [ {mag, 3}
    , {strlen, false}
    ].

%% NB. using the process dict is ugly; passing Opts around is uglier.
setopts(Opts) -> put(dataspec_opts, Opts).

getopts() ->
    case get(dataspec_opts) of
        undefined -> default_opts();
        Opts      -> Opts
    end.

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
    %% interpret the {mag, N} option as:
    %% - if N = 0 or the option is missing: no subgrouping based on magnitude
    %% - if N > 0: group N orders of magnitude into one subspec
    %% - if mag is present as a boolean option, N defaults to 3.
    Path = case proplists:get_value(mag, getopts()) of
               undefined -> [Key];
               0         -> [Key];
               true      -> [Key, {mag, mag(Value, 3)}];
               N         -> [Key, {mag, mag(Value, N)}]
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
    %% the strlen option enables subgrouping strings by length
    Path = case proplists:get_value(strlen, getopts(), false) of
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
%% spec data: {Min, Max, Count}
f_merge_num(error, Data)                                   -> {Data, Data, 1};
f_merge_num({ok, {Min, Max, Count}}, Data) when Data < Min -> {Data, Max, Count+1};
f_merge_num({ok, {Min, Max, Count}}, Data) when Data > Max -> {Min, Data, Count+1};
f_merge_num({ok, {Min, Max, Count}}, _Data)                -> {Min, Max, Count+1}.

%% spec merge function for sampling: keep count and store an example
%% spec data: {Count, Sample}
f_merge_sample(error, Data)                  -> {1, Data};
f_merge_sample({ok, {Count, Sample}}, _Data) -> {Count+1, Sample}.

%% spec merge function for recursively spec'ing compound structures
%% spec data: list of subspecs, one per item
f_merge_recur(error, L)       -> lists:map(fun new/1, L);
f_merge_recur({ok, SpecL}, L) -> lists:zipwith(fun add/2, L, SpecL).

%% general hierarchical storage for specs
%%   Spec: spec to modify: [{Key, Value|Subspec}]
%%   Path: list of key terms specifying spec location
%%   Fun: arinty 2 merge function to add data to spec
%%        (false, Data)        -> init_value(Data);
%%        ({Key, Value}, Data) -> merge_value(Value, Data).
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
    L = [1, 4, 9, 0, -5],
    Spec = lists:foldl(fun add/2, new(), L),
    ?assertEqual([{integer, [{{mag, 0}, {lists:min(L), lists:max(L), length(L)-1}}]},
                  {{value, 0}, 1}], Spec).

add_float_test() ->
    L = [1.3, 4.12, 9.99, 0.0, -5.31],
    Spec = lists:foldl(fun add/2, new(), L),
    ?assertEqual([{float, [{{mag, 0}, {lists:min(L), lists:max(L), length(L)-1}}]},
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
    Spec0 = new([1, 2]),
    ?assertEqual([{{list, 2},
                   [[{integer, [{{mag, 0}, {1, 1, 1}}]}],
                    [{integer, [{{mag, 0}, {2, 2, 1}}]}]]}], Spec0),
    Spec1 = add([1, 3], Spec0),
    ?assertEqual([{{list, 2},
                   [[{integer, [{{mag, 0}, {1, 1, 2}}]}],
                    [{integer, [{{mag, 0}, {2, 3, 2}}]}]]}], Spec1).

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
