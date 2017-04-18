%% -*- coding: utf-8 -*-
-module(ds_graphics).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% Graph generation facility

%% Client API
-export([ timestamp_range_graph/2
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

%% Attrs is a list of attribute tuples of {Key, Value}.
%% Data is a list of tuples {Value, Count, FirstTs, FirstKey, LastTs, LastKey}
%% where Ts are timestamps in gregorian seconds.
timestamp_range_graph(Attrs, Data) ->
    DataSeq = lists:zip(Data, lists:seq(1, length(Data))),
    Rows = [{T1-?EPOCH, Seq, T1-?EPOCH, T2-?EPOCH, value_label(V)} ||
               {{V,_C, T1,_K1, T2,_K2}, Seq} <- DataSeq],
    NRows = length(Rows),
    YSize = 60 + 16*NRows,
    YMax = NRows + 1,
    %% TODO set Xtics format and interval based on data
    XticsFormat = "%Y",
    XticsInterval = calendar:datetime_to_gregorian_seconds({{2,1,1},{0,0,0}}),
    MergedAttrs =
        merge_attrs([{xsize, 750}, % override with real width in Attrs
                     {ysize, YSize},
                     {xtics_format, XticsFormat},
                     {xtics_interval, XticsInterval},
                     {ymin, 0},
                     {ymax, YMax}],
                    Attrs),
    graph(ranges, MergedAttrs, Rows).

%% Graphs are generated via Gnuplot.
%%
%% Type is an atom that maps to a .plt.in template file under priv/.
%%
%% The actual Gnuplot .plt file is generated from this template,
%% substituting the attribute variables supplied in Attributes plus
%% standard attributes $datafile, $pngfile for input and output.
%%
%% Finally, the tabular data (list of tuples) in Data is plot via
%% Gnuplot using the .plt file, to a file with an auto-generated
%% unique filename under priv/. The full path to this file is
%% returned.
%%
%% The generated image files need to be garbage-collected.
%% The caller must either diplose of them manually via file:delete/1,
%% or call gc_image_file/2 below with a suitable delay.
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
