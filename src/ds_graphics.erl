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
    Rows = [{T1-?EPOCH, Seq, T1-?EPOCH, T2-?EPOCH, V} ||
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
%% or call gc_image_file below with a suitable interval argument.
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

format_value(Value) -> lists:flatten(io_lib:format("~tp", [Value])).

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
    Data = [{"one value", 63341232447, 63657376337},
            {"another value", 63649324800, 63649324800},
            {3, 63649324800, 63649324800},
            {"apple", 63297552721, 63657359744},
            {"orange", 63297552721, 63657359744},
            {"cat", 63297552721, 63657359744},
            {5, 63642709095, 63650707200},
            {6, 63642709095, 63650707200},
            {7, 63645125869, 63656841600}],
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

merge_attrs_test() ->
    ?assertEqual([{key, 123}], merge_attrs([], [{key, 123}])),
    ?assertEqual([{key, 123}], merge_attrs([{key, 123}], [])),
    ?assertEqual([{key1, 123}, {key3, 789}, {key2, 456}],
                 merge_attrs([{key1, 123}, {key3, 789}], [{key2, 456}])),
    ?assertEqual([{key1, 123}, {key2, 456}],
                 merge_attrs([{key1, 123}, {key2, 789}], [{key2, 456}])).

-endif.
