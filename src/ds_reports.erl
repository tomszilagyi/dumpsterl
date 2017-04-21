%% -*- coding: utf-8 -*-
-module(ds_reports).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% HTML-based report generator

%% Client API
-export([ report_page/2
        , config_update/2
        , config_store/3
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(COLOR_PTHEAD,  "#a0c0ff").
-define(COLOR_TABHEAD, "#cceeff").
-define(COLOR_TABRULE, "#ddffee").
-define(COLOR_LINK,    "#000000").
-define(COLOR_LINEBRK, "#ff0000").

-define(HTML_LINEBRK, "<font color=\"" ?COLOR_LINEBRK "\">\\</font>\n").

-define(IMAGE_GC_TIMEOUT, 5000).

report_page({Class, {Stats, Ext}}, ReportCfg) ->
    MainTable = main_table(Stats, ReportCfg),
    Samples = case Class of
                  atom -> Ext; % use exhaustive dict of values
                  _    -> ds_stats:get_samples(Stats)
              end,
    PtsTable = pts_samples_table(ds_stats:get_pts(Stats), Samples, ReportCfg),
    Page = {html, [],
            [{head, [],
              [{meta, [{'http-equiv', "Content-Type"},
                       {content, "text/html; charset=UTF-8"}], []}]},
             {body, [{link, ?COLOR_LINK},
                     {alink, ?COLOR_LINK},
                     {vlink, ?COLOR_LINK}],
              [{h3, [], [{str, ds_types:type_to_string(Class)}]},
               {font, [{size, "-1"}],
                [MainTable, PtsTable,
                 report_ext(Class, Ext, ReportCfg)]}]}]},
    html(Page).

main_table(Stats,_ReportCfg) ->
    CountStr = ds_utils:integer_to_sigfig(ds_stats:get_count(Stats)),
    {CardEstimate, CardEstError} = ds_stats:get_cardinality(Stats),
    CardStr = io_lib:format("~s ± ~s",
                            [ds_utils:integer_to_sigfig(CardEstimate),
                             ds_utils:integer_to_sigfig(CardEstError)]),
    [{font, [{size, "+2"}], [{str, "Properties"}]},
     {table, [{cellspacing, 0}],
      [{tr, [], [{td, [{align, right}], [{str, "Count: "}]},
                 {td, [], [{str, CountStr}]}]},
       {tr, [], [{td, [{align, right}], [{str, "Cardinality: "}]},
                 {td, [], [{str, CardStr}]}]}]}].

pts_samples_table(Pts, Samples, ReportCfg) ->
    AttrCols = tk_attrs_present(Pts) orelse tk_attrs_present(Samples),
    Cols = case AttrCols of
               true  -> 6;
               false -> 2
           end,
    [{table, [{width, "100%"}, {cellspacing, 0}],
      [table_section("Extremes"),
       [pt_table(Pt, AttrCols, Cols, ReportCfg) || Pt <- Pts],
       table_section("Samples"),
       samples_table(Samples, AttrCols, ReportCfg)]},
     {table, [{width, "100%"}, {cellspacing, 0}],
      range_graph(AttrCols, Pts, Samples, ReportCfg)}].

pt_table({Pt, Value, PVAttrs}, AttrCols, Cols, ReportCfg) ->
    Data = [value_row({Value, PVAttrs}, AttrCols)],
    [{tr, [], [{td, [], []}]},
     {tr, [{bgcolor, ?COLOR_PTHEAD}],
      [{th, [{align, left}, {colspan, Cols}],
        [{term, Pt}]}]},
     frame(Pt, stats_headers(AttrCols), Data, fun stats_display_f/1, ReportCfg)].

samples_table(Values, AttrCols, ReportCfg) ->
    Data = [value_row(V, AttrCols) || V <- Values],
    frame(samples, stats_headers(AttrCols), Data, fun stats_display_f/1, ReportCfg).

range_graph(false,_Pts,_Samples,_ReportCfg) -> [];
range_graph(true, Pts, Samples, ReportCfg) ->
    AllSamples0 = Samples ++ [{Value, PVAttrs} || {_Pt, Value, PVAttrs} <- Pts],
    AllSamples1 = [value_row(V, true) || V <- AllSamples0],
    AllSamples = [Row || {_V,_C, MinTS,_MinK, MaxTS,_MaxK} = Row <- AllSamples1,
                         MinTS /= undefined, MaxTS /= undefined],
    Data0 = lists:usort(AllSamples),
    %% Use the same sorting as selected for samples table:
    Sort = config_lookup(samples, sort, ReportCfg),
    Data = frame_data(Data0, Sort, fun(T) -> T end),

    {width, Width} = config_lookup(report, width, ReportCfg),
    PngFile = ds_graphics:timestamp_range_graph([{xsize, Width-40}], %% FIXME
                                                lists:reverse(Data)),
    ds_graphics:gc_image_file(PngFile, ?IMAGE_GC_TIMEOUT),
    [table_section("Timeline of sampled values"),
     {tr, [],
      [{td, [{align, left}],
        [{img, [{src, PngFile}], []}]}]}].

value_row({Value, PVAttrs}, false) ->
    Count = ds_pvattrs:get_count(PVAttrs),
    {Value, Count};
value_row({Value, PVAttrs}, true) ->
    Count = ds_pvattrs:get_count(PVAttrs),
    {{MinTS, MinKey}, {MaxTS, MaxKey}} = ds_pvattrs:get_timespan(PVAttrs),
    {Value, Count, MinTS, MinKey, MaxTS, MaxKey}.

stats_display_f({Value, Count}) ->
    {{term, Value},
     {td, [{align, right}], [{str, ds_utils:integer_to_sigfig(Count)}]}};
stats_display_f({Value, Count, MinTS, MinKey, MaxTS, MaxKey}) ->
    {{term, Value},
     {td, [{align, right}], [{str, ds_utils:integer_to_sigfig(Count)}]},
     {td, [{align, right}], [blank_if_missing(MinTS)]},
     {td, [{align, right}], [blank_if_missing(MinKey)]},
     {td, [{align, right}], [blank_if_equal(MaxTS, MinTS)]},
     {td, [{align, right}], [blank_if_equal(MaxKey, MinKey)]}}.

blank_if_missing(undefined) -> {raw, ""};
blank_if_missing(V)         -> {term, V}.

blank_if_equal(V, V)    -> {raw, ""};
blank_if_equal(V,_Vref) -> blank_if_missing(V).

stats_headers(false) ->
    [[{"Value", [{align, left}]},
      {"Count", [{align, right}]}]];
stats_headers(true) ->
    [["", "",
      {"First", [{align, right}]}, {"First", [{align, right}]},
      {"Last", [{align, right}]}, {"Last", [{align, right}]}],
     [{"Value", [{align, left}]},
      {"Count", [{align, right}]},
      {"Timestamp", [{align, right}]},
      {"Key", [{align, right}]},
      {"Timestamp", [{align, right}]},
      {"Key", [{align, right}]}]].

%% See if there are any timestamp/key annotations in the dataset.
%% If there are none, the relevant columns will be omitted.
tk_attrs_present([]) -> false;
tk_attrs_present([{_Pt,_Value, PVAttrs}|Rest]) ->
    tk_attrs_present(PVAttrs, Rest);
tk_attrs_present([{_Value, PVAttrs}|Rest]) ->
    tk_attrs_present(PVAttrs, Rest).

tk_attrs_present(PVAttrs, Rest) ->
    case ds_pvattrs:get_timespan(PVAttrs) of
        {{undefined, undefined}, {undefined, undefined}} ->
            tk_attrs_present(Rest);
        _ -> true
    end.

report_ext(_Class, [],_ReportCfg) -> [];
report_ext(atom,_Ext,_ReportCfg) -> []; % used as Samples in the common report
report_ext(binary, Ext, ReportCfg) -> report_ext(bitstring, Ext, ReportCfg);
report_ext(bitstring, Ext, ReportCfg) ->
    histogram(size_dist, "Bit size histogram", Ext, ReportCfg);
report_ext(nonempty_list, Ext, ReportCfg) ->
    histogram(length_dist, "List length histogram", Ext, ReportCfg);
report_ext(_Class, Ext,_ReportCfg) ->
    [{font, [{size, "+2"}], [br, {str, "Extended data (raw)"}, br, br]},
     {term, Ext}].

histogram(Id, Title, Data, ReportCfg) ->
    {show_table, ShowTable} = config_lookup(Id, show_table, ReportCfg),
    ShowTableLink = config_link(Id, show_table, not ShowTable),
    ShowTableLinkText = case ShowTable of
                            true  -> "[Hide table]";
                            false -> "[Show table]"
                        end,
    {logscale_y, LogScaleY} = config_lookup(Id, logscale_y, ReportCfg),
    LogScaleYLink = config_link(Id, logscale_y, not LogScaleY),
    LogScaleYLinkText = case LogScaleY of
                            true  -> "[Lin Y]";
                            false -> "[Log Y]"
                        end,
    LogScaleYAttr = case LogScaleY of
                        true  -> set;
                        false -> unset
                    end,
    {hist_autobins, AutoBins} = config_lookup(Id, hist_autobins, ReportCfg),
    AutoBinsLink = config_link(Id, hist_autobins, not AutoBins),
    AutoBinsLinkText = case AutoBins of
                            true  -> "[Raw x-data]";
                            false -> "[Auto-binned]"
                        end,
    {width, Width} = config_lookup(report, width, ReportCfg),
    BinnedData = case AutoBins of
                     true  ->
                         %% manually tuned based on gnuplot output:
                         NBins = max(1, (Width - 100) div 30),
                         bin_histdata(Data, NBins);
                     false ->
                         Data
                 end,
    PngFile = ds_graphics:histogram_graph([{set_logscale_y, LogScaleYAttr},
                                           {xsize, Width-40}], %% FIXME
                                          BinnedData),
    ds_graphics:gc_image_file(PngFile, ?IMAGE_GC_TIMEOUT),
    [{table, [{width, "100%"}, {cellspacing, 0}],
      [table_section(Title),
       {tr, [{bgcolor, ?COLOR_PTHEAD}],
        [{th, [{align, left}],
          [{a, [{href, ShowTableLink}], [{str, ShowTableLinkText}]},
           {str, " "},
           {a, [{href, LogScaleYLink}], [{str, LogScaleYLinkText}]},
           {str, " "},
           {a, [{href, AutoBinsLink}], [{str, AutoBinsLinkText}]}]}]},
       {tr, [],
        [{td, [],
          [{img, [{src, PngFile}], []}]}]},
       {tr, [],
        [{td, [],
          [{table, [{cellspacing, 0}],
            histogram_data_table(ShowTable, Id, Data, ReportCfg)}]}]}]}].

histogram_data_table(false,_Id,_Data,_ReportCfg) -> [];
histogram_data_table(true, Id, Data, ReportCfg) ->
    frame(Id, stats_headers(false), Data, fun stats_display_f/1, ReportCfg).

bin_histdata(Data, Bins) when length(Data) =< Bins -> Data;
bin_histdata([{FirstValue,_}|_] = Data, Bins) ->
    {LastValue,_} = lists:last(Data),
    Span = LastValue - FirstValue,
    BinWidth = Span / Bins,
    BinRanges =
        [{FirstValue + BinWidth * Seq,
          FirstValue + BinWidth * (Seq + 1)} || Seq <- lists:seq(0, Bins-2)]
        ++ [{FirstValue + BinWidth * (Bins-1), LastValue}],
    [select_bin({From, To}, LastValue, Data) || {From, To} <- BinRanges].

select_bin({From, To}, To, Data) ->
    {(From + To) / 2, lists:sum([C || {V,C} <- Data, V >= From, V =< To])};
select_bin({From, To},_LastValue, Data) ->
    {(From + To) / 2, lists:sum([C || {V,C} <- Data, V >= From, V < To])}.

table_section(Title) ->
    [{tr, [], [{td, [], [br]}]}, % vskip
     {tr, [],
      [{th, [{align, left}],
        [{font, [{size, "+1"}], [{str, Title}]}]}]}].

%% A frame shows tabular data and allows the user to click on headers
%% to sort the data according to a certain column.
%% - Id: atom to identify this frame in ReportConfig
%% - Headers: table header rows
%%     (the last one will receive sort links, any preceding ones are for
%%      grouping only):
%%     [ ..., ["Header" | {"Header", [{Attr, Value]}}] ]
%% - Data: list of tuples containing raw values
%% - DisplayFun: fun({...}) -> {...} end
%%     transform rows for display
%%     - useful to hide 'undefined', etc.
%%     - convert each field to {str, Str} or {term, Term}
%%     from tuple of raw values to tuple of html structures
%%     (enclosing table cell is optional, useful to add cell attributes)
%% - ReportCfg: list of tuples, we look for
%%     {Id, [{sort, ColNo, ascending|descending}]}
frame(Id, Headers, Data0, DisplayFun, ReportCfg) ->
    Sort = config_lookup(Id, sort, ReportCfg),
    Data = frame_data(Data0, Sort, DisplayFun),
    NDataRows = length(Data),
    HeaderRows = frame_headers(Id, Headers, Sort, NDataRows),
    [HeaderRows | stripe_rows([frame_data_row(D) || D <- Data])].

frame_sort(Data, false) -> Data;
frame_sort(Data, {sort, N, Dir}) -> lists:sort(mk_sort_fun(N, Dir), Data).

mk_sort_fun(N, ascending)  -> fun(T1, T2) -> element(N, T1) =< element(N, T2) end;
mk_sort_fun(N, descending) -> fun(T1, T2) -> element(N, T1) >= element(N, T2) end.

%% Prepare the raw data by sorting and converting via DisplayFun.
frame_data(Data0, Sort, DisplayFun) ->
    Data = frame_sort(Data0, Sort),
    [DisplayFun(D) || D <- Data].

frame_data_row(DataRow) ->
    {tr, [], [frame_data_cell(Field) || Field <- tuple_to_list(DataRow)]}.

frame_data_cell({td,_Attrs,_Content}=Cell) -> Cell;
frame_data_cell(Content) when is_list(Content) -> {td, [], Content};
frame_data_cell(Content) -> {td, [], [Content]}.


frame_headers(_Id, [],_Sort,_NDataRows) -> [];
frame_headers(Id, HeaderSpecL, Sort, NDataRows) ->
    [SortHdr|GroupHdrL] = lists:reverse(HeaderSpecL),
    lists:reverse([frame_header(Id, SortHdr, Sort, NDataRows > 1)
                   | [frame_header(Id, GroupHdr, false, false)
                      || GroupHdr <- GroupHdrL]]).

frame_header(Id, HeaderSpec0, Sort, EnableSortLinks) ->
    HeaderSpec = lists:zip(HeaderSpec0, lists:seq(1, length(HeaderSpec0))),
    {tr, [{bgcolor, ?COLOR_TABHEAD}],
     [frame_header_cell(Id, HeaderCell, N, Sort, EnableSortLinks)
      || {HeaderCell, N} <- HeaderSpec]}.

frame_header_cell(Id, {Caption, AttrList}, N, Sort, true) ->
    Link = config_link(Id, sort, N),
    SortCaption = frame_header_caption(Caption, N, Sort),
    {th, AttrList, [{a, [{href, Link}], [{str, SortCaption}]}]};
frame_header_cell(_Id, {Caption, AttrList},_N,_Sort, false) ->
    {th, AttrList, [{str, Caption}]};
frame_header_cell(Id, Caption, N, Sort, EnableSortLinks) ->
    frame_header_cell(Id, {Caption, []}, N, Sort, EnableSortLinks).

frame_header_caption(Caption, N, {sort, N, ascending}) -> Caption ++ " ↓";
frame_header_caption(Caption, N, {sort, N, descending}) -> Caption ++ " ↑";
frame_header_caption(Caption,_N, {sort,_SortColN,_SortDir}) -> Caption;
frame_header_caption(Caption,_N, false) -> Caption.

%% Take a list of table rows and set bgcolor attribute on every other
%% to achieve the striped rows effect.
stripe_rows(Rows) -> stripe_rows(Rows, [], false).

stripe_rows([], Acc,_Stripe) -> lists:reverse(Acc);
stripe_rows([{tr, Attrs0, Content}|Rest], Acc, true) ->
    Attrs = [{bgcolor, ?COLOR_TABRULE}|Attrs0],
    stripe_rows(Rest, [{tr, Attrs, Content}|Acc], false);
stripe_rows([Row|Rest], Acc, false) ->
    stripe_rows(Rest, [Row|Acc], true).

%% Report config
%%
%% A key-value list [{Id, Cfg}] where Id is a frame identifier
%% and Cfg is a further tuple list where tuple size may vary.
%% Currently the following tuple types are used for Cfg:
%% - {sort, ColN, Dir} to specify the currently selected sort on
%%      the frame;
%% - {show_table, boolean()} to specify whether a table with the
%%      raw data underlying the graph should be shown;
%% - {logscale_y, boolean()} to specify whether the y axis should
%%      be scaled logarithmically;
%% - {hist_autobins, boolean()} to specify whether x values of a
%%      histogram should be auto-binned.
config_update(ReportCfg, Link) ->
    [IdStr, KeyStr | ArgsStrL] = string:tokens(Link, "."),
    Id = list_to_atom(IdStr),
    Key = list_to_atom(KeyStr),
    OldSetting = config_lookup(Id, Key, ReportCfg),
    NewSetting = config_update(Key, OldSetting, ArgsStrL),
    config_store(Id, NewSetting, ReportCfg).

config_update(hist_autobins, _OldSetting, [BooleanStr]) ->
    {hist_autobins, list_to_atom(BooleanStr)};
config_update(logscale_y, _OldSetting, [BooleanStr]) ->
    {logscale_y, list_to_atom(BooleanStr)};
config_update(show_table, _OldSetting, [BooleanStr]) ->
    {show_table, list_to_atom(BooleanStr)};
config_update(sort, OldSetting, [ColStr]) ->
    ColN = list_to_integer(ColStr),
    case OldSetting of
        {sort, ColN, Dir} -> {sort, ColN, invert_dir(Dir)};
        _                 -> {sort, ColN, ascending}
    end.

config_store(Id, CfgTuple, ReportCfg) ->
    Cfg = case lists:keyfind(Id, 1, ReportCfg) of
              false -> [CfgTuple];
              {Id, Cfg0} ->
                  Key = element(1, CfgTuple),
                  lists:keystore(Key, 1, Cfg0, CfgTuple)
          end,
    lists:keystore(Id, 1, ReportCfg, {Id, Cfg}).

config_lookup(Id, Key, ReportCfg) ->
    case lists:keyfind(Id, 1, ReportCfg) of
        false     -> lists:keyfind(Key, 1, config_defaults());
        {Id, Cfg} ->
            case lists:keyfind(Key, 1, Cfg) of
                false    -> lists:keyfind(Key, 1, config_defaults());
                CfgTuple -> CfgTuple
            end
    end.

%% Generate a link that will trigger appropriate config update actions
%% in config_update above.
config_link(Id, Key, Data) ->
    lists:concat([Id, ".", Key, ".", Data]).

%% Default settings for ReportCfg keys to use in case they are missing.
config_defaults() ->
    [ {sort, 1, ascending}
    , {show_table, false}
    , {logscale_y, false}
    , {hist_autobins, false}
    ].

invert_dir(ascending) -> descending;
invert_dir(descending) -> ascending.

%% Generate a HTML-encoded string from a data structure encoding HTML.
%%
%% HTML is represented as a recursive data structure:
%%   {Tag, Attributes, Contents}
%% where Tag is an atom;
%%       Attributes is a list of {Attr, Value}, and
%%       Contents is a list of children tags.
%%
%% Special 'terminal' tags {raw, Str}, {str, Str} and {term, Term}
%% produce direct output with different degrees of processing.
html({raw, Str}) -> Str;
html({str, Str}) -> html_encode(Str);
html({term, Term}) ->
    RawStr = io_lib:format("~80tp", [Term]),
    Str = nl_encode(linewrap(html_encode(RawStr), 82)),
    html({code, [], [{raw, Str}]});
html(Tags) when is_list(Tags) -> lists:concat([html(T) || T <- Tags]);
html(Tag) when is_atom(Tag) -> lists:concat(["<", Tag, ">"]);
html({Tag, Attributes, Contents}) ->
    ValueStrs = [attr_to_str(A) || A <- [Tag|Attributes]],
    TagStr = string:join(ValueStrs, " "),
    ContentStr = lists:concat([html(C) || C <- Contents]),
    lists:concat(["<", TagStr, ">", ContentStr, "</", Tag, ">"]).

attr_to_str({Attr, Value}) -> lists:concat([Attr, "=\"", Value, "\""]);
attr_to_str(Attr) -> lists:concat([Attr]).

%% Insert line breaks and preceding break marks (backslash in
%% ?COLOR_LINEBRK) into lines longer than Cols.
%% We receive a html-escaped string, so take care not to break it
%% in the middle of an HTML entity.
linewrap(String, Cols) ->
    Lines = string:tokens(String, "\r\n"),
    string:join([wrap(Line, Cols) || Line <- Lines], "\n").

wrap(Line, Cols) ->
    wrap(Line, [], 0, Cols).

wrap([], Acc,_N,_Cols) -> lists:flatten(lists:reverse(Acc));
wrap(Line, Acc, Cols, Cols) -> % Time for a smooth line break:
    wrap(Line, [?HTML_LINEBRK|Acc], 0, Cols);
wrap([$&|Rest], Acc, N, Cols) ->
    IxCloseEntity = string:chr(Rest, $;),
    {R0, R1} = lists:split(IxCloseEntity, Rest),
    wrap(R1, [[$&|R0]|Acc], N+1, Cols);
wrap([C|Rest], Acc, N, Cols) ->
    wrap(Rest, [C|Acc], N+1, Cols).

%% Encode HTML special characters into their respective entities
html_encode(C) when is_integer(C) -> C;
html_encode(S) -> html_encode(lists:flatten(S), []).

html_encode([], Acc) -> lists:flatten(lists:reverse(Acc));
html_encode([$< | T], Acc) -> html_encode(T, ["&lt;" | Acc]);
html_encode([$> | T], Acc) -> html_encode(T, ["&gt;" | Acc]);
html_encode([$& | T], Acc) -> html_encode(T, ["&amp;" | Acc]);
html_encode([$" | T], Acc) -> html_encode(T, ["&quot;" | Acc]);
html_encode([$' | T], Acc) -> html_encode(T, ["&#39;" | Acc]);
html_encode([$  | T], Acc) -> html_encode(T, ["&nbsp;" | Acc]);
html_encode([H|T], Acc) -> html_encode(T, [H|Acc]).

%% Encode newlines so they remain visible in HTML
nl_encode(S) -> nl_encode(S, []).

nl_encode([], Acc) -> lists:flatten(lists:reverse(Acc));
nl_encode([$\n | T], Acc) -> nl_encode(T, ["<br>\n" | Acc]);
nl_encode([H|T], Acc) -> nl_encode(T, [H|Acc]).

%% Tests
-ifdef(TEST).

bin_histdata_test() ->
    Data = [{1,   6481}, {3, 862931}, {4,   5109}, {5,  59340},
            {6,  32026}, {7,  54689}, {8, 137813}, {11, 53553}],
    BinnedData1 = bin_histdata(Data, 1),
    ?assertEqual([{6.0, 1211942}], BinnedData1),
    BinnedData2 = bin_histdata(Data, 2),
    ?assertEqual([{3.5, 933861}, {8.5, 278081}], BinnedData2),
    BinnedData4 = bin_histdata(Data, 4),
    ?assertEqual([{2.25, 869412},
                  {4.75, 64449},
                  {7.25, 224528},
                  {9.75, 53553}], BinnedData4).

config_lookup_test() ->
    ?assertEqual({sort, 1, ascending}, % default picked due to missing id
                 config_lookup(myframeid, sort, [])),
    ?assertEqual({sort, 1, ascending}, % default picked due to missing key for id
                 config_lookup(myframeid, sort, [{myframeid, []}])),
    ?assertEqual({sort, 3, ascending},
                 config_lookup(myframeid, sort,
                               [{myframeid, [{sort, 3, ascending}]}])).

config_store_test() ->
    ?assertEqual([{myframeid, [{sort, 3, descending}]}],
                 config_store(myframeid, {sort, 3, descending}, [])),
    ?assertEqual([{myframeid, [{sort, 3, descending}]}],
                 config_store(myframeid, {sort, 3, descending},
                              [{myframeid, [{sort, 3, ascending}]}])),
    ?assertEqual([{otherframeid, [{dummy, 123}]},
                  {myframeid, [{sort, 3, descending}]}],
                 config_store(myframeid, {sort, 3, descending},
                              [{otherframeid, [{dummy, 123}]},
                               {myframeid, [{sort, 1, ascending}]}])).

config_update_test() ->
    ?assertEqual([{myframeid, [{sort, 1, descending}]}],
                 config_update([], config_link(myframeid, sort, 1))),
    ?assertEqual([{myframeid, [{sort, 1, ascending}]}],
                 config_update([{myframeid, [{sort, 1, descending}]}],
                               config_link(myframeid, sort, 1))),
    ?assertEqual([{otherframeid, [{dummy, 123}]},
                  {myframeid, [{sort, 3, ascending}]}],
                 config_update([{otherframeid, [{dummy, 123}]},
                                {myframeid, [{sort, 1, ascending}]}],
                               config_link(myframeid, sort, 3))),

    ?assertEqual([{myframeid, [{show_table, true}]}],
                 config_update([], config_link(myframeid, show_table, true))),
    ?assertEqual([{myframeid, [{show_table, false}]}],
                 config_update([{myframeid, [{show_table, true}]}],
                               config_link(myframeid, show_table, false))).

linewrap_test() ->
    ?assertEqual("abcdefghij" ++ ?HTML_LINEBRK ++
                 "klmnopqrst" ++ ?HTML_LINEBRK ++ "uvwxyz",
                 linewrap("abcdefghijklmnopqrstuvwxyz", 10)),
    ?assertEqual("abcd&quot;fghij" ++ ?HTML_LINEBRK ++
                 "klmnopqrs&#1234;" ++ ?HTML_LINEBRK ++ "uvwxyz",
                 linewrap("abcd&quot;fghijklmnopqrs&#1234;uvwxyz", 10)).

html_test() ->
    ?assertEqual("<br>", html(br)),
    ?assertEqual("<html><head><meta http-equiv=\"Content-Type\" "
                 "content=\"text/html; charset=UTF-8\"></meta></head>"
                 "<body></body></html>",
                 html({html, [],
                       [{head, [],
                         [{meta, [{'http-equiv', "Content-Type"},
                                  {content, "text/html; charset=UTF-8"}],
                           []}]},
                        {body, [], []}]})),
    ?assertEqual("<table width=\"100%\" cellspacing=\"0\">"
                 "<tr bgcolor=\"#123456\">"
                 "<td align=\"left\">Cell</td>"
                 "<td>I&#39;m&nbsp;valid&nbsp;&amp;&nbsp;&nbsp;encoded!</td>"
                 "</tr></table>",
                 html({table, [{width, "100%"}, {cellspacing, 0}],
                       [{tr, [{bgcolor, "#123456"}],
                         [{td, [{align, left}], [{str, "Cell"}]},
                          {td, [], [{str, "I'm valid &  encoded!"}]}]}]})),
    ?assertEqual("<html><body bgcolor=\"#123456\" align=\"left\">"
                 "<table cellpadding=\"0\"><hr><br></table></body></html>",
                 html({html, [],
                       [{body, [{bgcolor, "#123456"}, {align, left}],
                         [{table, [{cellpadding, 0}], [hr, br]}]}]})).

-endif.
