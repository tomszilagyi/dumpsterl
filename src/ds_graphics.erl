%% -*- coding: utf-8 -*-
-module(ds_graphics).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% Graph generation facility

%% Client API
-export([ histogram_graph/2
        , timestamp_range_graph/2
        , graph/3
        , merge_attrs/2
        , gc_image_file/2
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Gnuplot takes timestamps in Unix format, starting from the Epoch.
%% Value: calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}})
-define(EPOCH, 62167219200).

-define(DEFAULT_XSIZE, 750).

%% Attrs is a list of attribute tuples of {Key, Value}.
%% Data is a list of tuples {Value, Count}.
histogram_graph(Attrs, Data) ->
    Xvalues = [V || {V,_Cnt} <- Data],
    Xmin = lists:min(Xvalues),
    Xmax = lists:max(Xvalues) + 1,
    MergedAttrs =
        merge_attrs([{xsize, ?DEFAULT_XSIZE}, % override this from Attrs
                     {ysize, 350},
                     {xmin, Xmin},
                     {xmax, Xmax}],
                    Attrs),
    graph(histogram, MergedAttrs, Data).

%% Attrs0 is a list of attribute tuples of {Key, Value}.
%% Data is a list of tuples {Value, Count, FirstTs, FirstKey, LastTs, LastKey}
%% where *Ts are timestamps in gregorian seconds.
timestamp_range_graph(Attrs0, Data) ->
    DataSeq = lists:zip(Data, lists:seq(1, length(Data))),
    Rows = [{conv_ts(T1), Seq, conv_ts(T1), conv_ts(T2), value_label(V)} ||
               {{V,_C, T1,_K1, T2,_K2}, Seq} <- DataSeq],
    TsMin = lists:min([element(3, R) || R <- Rows]),
    TsMax = lists:max([element(4, R) || R <- Rows]),
    NRows = length(Rows),
    YSize = 60 + 16*NRows, % manually tuned based on gnuplot output
    YMax = NRows + 1,
    Attrs1 = merge_attrs([{xsize, ?DEFAULT_XSIZE}, % override this from Attrs0
                          {ysize, YSize},
                          {ymin, 0},
                          {ymax, YMax}],
                         Attrs0),
    Attrs = auto_xtics(TsMin, TsMax, Attrs1),
    graph(ranges, Attrs, Rows).

%% Calculate appropriate xtics attributes for x-axis showing time.
%% Returns Attrs with added attributes 'xtics_format' and 'xtics_interval'.
auto_xtics(TsMin, TsMax, Attrs) ->
    XSize = proplists:get_value(xsize, Attrs, ?DEFAULT_XSIZE),
    TimeSpan = TsMax - TsMin,
    MinTicWidth = 150, % manually tuned based on gnuplot output
    MinSecondsPerTic = max(1, TimeSpan * MinTicWidth div XSize),
    {XticsInterval, XticsFormat} = xtics_choose_range(MinSecondsPerTic),
    merge_attrs([{xtics_format, XticsFormat},
                 {xtics_interval, XticsInterval}], Attrs).

%% Choose the smallest tic interval and associated label format
%% from xtics_range_table that accomodates our actual x-axis density
xtics_choose_range(SecondsPerTic) ->
    xtics_choose_range(SecondsPerTic, xtics_range_table()).

xtics_choose_range(SecondsPerTic, [{Limit,_Format}=RangeSpec|_Rest])
  when SecondsPerTic >= Limit -> RangeSpec;
xtics_choose_range(SecondsPerTic, [_RangeSpec|Rest]) ->
    xtics_choose_range(SecondsPerTic, Rest).

xtics_range_table() ->
    %% xtics interval    format         one tic label per every...
    [ {86400 * 365 * 50, "%Y"}          % 50 years
    , {86400 * 365 * 30, "%Y"}          % 30 years
    , {86400 * 365 * 20, "%Y"}          % 20 years
    , {86400 * 365 * 10, "%Y"}          % 10 years
    , {86400 * 365 * 5,  "%Y"}          %  5 years
    , {86400 * 365 * 3,  "%Y"}          %  3 years
    , {86400 * 365 * 2,  "%Y"}          %  2 years
    , {86400 * 365,      "%Y"}          %  1 year
    , {86400 * 182,      "%Y/%m"}       %  6 months
    , {86400 * 30 * 3,   "%Y/%m"}       % 90 days
    , {86400 * 30 * 2,   "%Y/%m"}       % 60 days
    , {86400 * 30,       "%Y/%m"}       % 30 days
    , {86400 * 15,       "%Y/%m/%d"}    % 15 days
    , {86400 * 10,       "%Y/%m/%d"}    % 10 days
    , {86400 * 5,        "%Y/%m/%d"}    %  5 days
    , {86400 * 3,        "%Y/%m/%d"}    %  3 days
    , {86400 * 2,        "%Y/%m/%d"}    %  2 days
    , {86400,            "%Y/%m/%d"}    %  1 day
    , {43200,            "%m/%d %H'"}   % 12 hours
    , {21600,            "%m/%d %H'"}   %  6 hours
    , {10800,            "%m/%d %H'"}   %  3 hours
    , { 7200,            "%d. %H:%M"}   %  2 hours
    , { 3600,            "%d. %H:%M"}   %  1 hour
    , { 1800,            "%d. %H:%M"}   % 30 minutes
    , { 1200,            "%d. %H:%M"}   % 20 minutes
    , {  600,            "%d. %H:%M"}   % 10 minutes
    , {  300,            "%d. %H:%M"}   %  5 minutes
    , {  180,            "%d. %H:%M"}   %  3 minutes
    , {  120,            "%d. %H:%M"}   %  2 minutes
    , {   60,            "%d. %H:%M"}   %  1 minute
    , {    0,            "%M:%S"}       %  catch-all
    ].

%% Convert standard timestamp (gregorian seconds) to the format
%% expected by Gnuplot (seconds since Unix epoch).
conv_ts(Ts) -> max(0, Ts - ?EPOCH).

%% Graphs are generated via Gnuplot.
%%
%% Type is an atom that maps to a .plt.in template file under priv/.
%%
%% The actual Gnuplot .plt file is generated from this template,
%% substituting the attribute variables supplied in Attributes plus
%% standard attributes $datafile and $pngfile for input and output.
%%
%% Finally, the tabular data (list of tuples) in Data is plot via
%% Gnuplot using the .plt file, to a file with an auto-generated
%% unique filename under priv/. The full path to this file is
%% returned.
%%
%% The generated image files need to be garbage-collected.
%% The caller must either diplose of them manually via file:delete/1,
%% or call gc_image_file/2 with a suitable delay.
graph(Type, Attributes0, Data) ->
    graph(Type, Attributes0, Data, os:find_executable("gnuplot")).

graph(_Type,_Attributes,_Data, false) ->
    throw(gnuplot_not_found);
graph(Type, Attributes0, Data, GnuplotExecutable) ->
    Template = filename:join(dir(), lists:concat([Type, ".plt.in"])),
    RandomKey = random_key(),
    DatFile = filename:join(dir(), RandomKey ++ ".dat"),
    PltFile = filename:join(dir(), RandomKey ++ ".plt"),
    PngFile = filename:join(dir(), RandomKey ++ ".png"),
    {ok, TemplateBin} = file:read_file(Template),
    Attributes = [ {datafile, DatFile}
                 , {pngfile, PngFile}
                 | Attributes0],
    Plt = fill_attrs(binary_to_list(TemplateBin), Attributes),
    ok = file:write_file(PltFile, Plt),
    ok = file:write_file(DatFile, format_csv(Data)),
    os:cmd(GnuplotExecutable ++ " " ++ PltFile),
    ok = file:delete(DatFile),
    ok = file:delete(PltFile),
    PngFile.

fill_attrs(String, Attributes) ->
    lists:foldl(fun fill_attr/2, String, Attributes).

fill_attr({Attr, Value}, String) ->
    AttrStr = lists:concat(["$", Attr]),
    lists:flatten(replace_all(AttrStr, format_value(Value), String)).

replace_all(FromStr, ToStr, String) ->
    case string:str(String, FromStr) of
        0  -> String;
        Ix -> {S1, S2} = lists:split(Ix-1, String),
              RestStr = string:substr(S2, length(FromStr)+1),
              [S1, ToStr, replace_all(FromStr, ToStr, RestStr)]
    end.

%% Convert a list of n-tuples to a string containing lines of csv
format_csv(Data) ->
    Lines = [format_line(D) || D <- Data],
    string:join(Lines, "\n").

format_line(Values) ->
    string:join([format_value(V) || V <- tuple_to_list(Values)], ", ").

format_value(Value) ->
    lists:flatten(io_lib:format("~p", [Value])).

%% Convert Value into a length-limited, ellipsized string
%% suitable for use as a tic label.
value_label(Value) when is_binary(Value) ->
    value_label_filter(io_lib:format("~p", [Value]));
value_label(Value) ->
    Str = try io_lib:format("~s", [Value])
          catch error:badarg -> io_lib:format("~p", [Value])
          end,
    value_label_filter(Str).

%% Remove characters that cause problems within a Gnuplot data string
value_label_filter(Str) ->
    ellipsize(lists:filter(fun($") -> false;
                              ($\n) -> false;
                              (_)  -> true
                           end, lists:flatten(Str))).

ellipsize(Str) ->
    if length(Str) > 23 -> string:substr(Str, 1, 20) ++ "...";
       true -> Str
    end.

random_key() ->
    RandomKey = integer_to_list(erlang:phash2(make_ref(), 1 bsl 32)),
    Exists = filelib:is_file(filename:join(dir(), RandomKey ++ ".png")),
    if Exists -> random_key();
       true -> RandomKey
    end.

dir() -> code:priv_dir(dumpsterl).

%% Merge ExtraAttrs into Attrs, by entering all entries in ExtraAttrs
%% into Attrs. This means that an entry in ExtraAttrs takes precedence.
merge_attrs(Attrs, ExtraAttrs) ->
    lists:foldl(fun({Key,_V} = ExtraAttr, Acc) ->
                        lists:keystore(Key, 1, Acc, ExtraAttr)
                end, Attrs, ExtraAttrs).

%% Schedule the file Filename to be deleted after Delay milliseconds.
%% The purpose of this is to allow the HTML widget to read and display
%% the image, without having to manually revisit the file later.
%% Call with Delay == 0 to immediately delete the file.
gc_image_file(Filename, Delay) ->
    spawn(fun() ->
              receive after Delay ->
                  file:delete(Filename)
              end
          end).

%% Tests
-ifdef(TEST).

histogram_graph_test() ->
         %% Value Count
    Data = [{1,  168046}, {2,15308145}, {3, 7832820}, {4, 4108368},
            {5, 2148978}, {6, 1898901}, {7, 1557822}, {8,  938278},
            {9,  424736}, {10, 486730}, {11,  38853}, {12,  12340},
            {13,   5446}, {14,   8357}, {15,   2578}, {16,   2098},
            {17,   1030}, {18,   1312}, {19,    762}, {20,    878}],
    try
        PngFile = histogram_graph([], Data),
        ok = file:delete(PngFile)
    catch gnuplot_not_found -> ok
    end.

timestamp_range_graph_test() ->
         %% Value Count FirstTs      FirstKey   LastTs       LastKey
    Data = [{5,      3, 63604195833, 151047710, 63648171560, 393359950},
            {6,   2941, 63332678758,      7147, 63657273600, 480595070},
            {8,   1280, 63341232447,     12977, 63647078400, 379945090},
            {19,  2150, 63297552721,        67, 63657273600, 480218350}],
    try
        PngFile = timestamp_range_graph([], Data),
        ok = file:delete(PngFile)
    catch gnuplot_not_found -> ok
    end.

fill_attr_test() ->
    ?assertEqual("pos1=123, pos2=123,\npos3=$ps",
                 fill_attr({pos, 123}, "pos1=$pos, pos2=$pos,\npos3=$ps")),
    ?assertEqual("title=\"MyPlot\"\n"
                 "set width = 100, height = 200\n"
                 "plot with lines",
                 fill_attrs("title=$title\n"
                            "set width = $width, height = $height\n"
                            "plot with lines",
                            [{title, "MyPlot"}, {width, 100}, {height, 200}])).

format_csv_test() ->
    ?assertEqual("", format_csv([])),
    Data = [{"Value", "Count", first_ts, last_ts},
            {1,  1127, 63341232447, 63657376337},
            {3,     1, 63649324800, 63649324800},
            {4, 35354, 63297552721, 63657359744},
            {5,     7, 63642709095, 63650707200},
            {6,     2, 63645125869, 63656841600}],
    ?assertEqual("\"Value\", \"Count\", first_ts, last_ts\n"
                 "1, 1127, 63341232447, 63657376337\n"
                 "3, 1, 63649324800, 63649324800\n"
                 "4, 35354, 63297552721, 63657359744\n"
                 "5, 7, 63642709095, 63650707200\n"
                 "6, 2, 63645125869, 63656841600",
                 format_csv(Data)).

value_label_test() ->
    ?assertEqual("5", value_label(5)),
    ?assertEqual("{data,123}", value_label({data, 123})),
    ?assertEqual("[19,a,{b,555}]", value_label([19, a, {b, 555}])),
    ?assertEqual("stringent", value_label("string\n\"ent\"")),
    ?assertEqual("<<151,208,232,173>>", value_label(<<151,208,232,173>>)),
    ?assertEqual("<<string-as-a-binary>>",
                 value_label(<<"string-as-a-binary">>)),
    ?assertEqual("{long,[1234567,abcde...",
                 value_label({long, [1234567,"abcdefgh"]})),
    ok.

merge_attrs_test() ->
    ?assertEqual([{key, 123}], merge_attrs([], [{key, 123}])),
    ?assertEqual([{key, 123}], merge_attrs([{key, 123}], [])),
    ?assertEqual([{key1, 123}, {key3, 789}, {key2, 456}],
                 merge_attrs([{key1, 123}, {key3, 789}], [{key2, 456}])),
    ?assertEqual([{key1, 123}, {key2, 456}],
                 merge_attrs([{key1, 123}, {key2, 789}], [{key2, 456}])).

-endif.
