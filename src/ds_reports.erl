-module(ds_reports).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% HTML-based report generator

%% Client API
-export([ stats_page/2
        , config_update/2
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(COLOR_PTHEAD,  "#d0d0d0").
-define(COLOR_TABHEAD, "#e0e0ff").
-define(COLOR_LINK,    "#000000").

stats_page(Stats, ReportCfg) ->
    MainTable = main_table(Stats, ReportCfg),
    PtsTable = pts_samples_table(ds_stats:get_pts(Stats),
                                 ds_stats:get_samples(Stats),
                                 ReportCfg),
    Page = {html, [],
            [{body, [{link, ?COLOR_LINK},
                     {alink, ?COLOR_LINK},
                     {vlink, ?COLOR_LINK}],
              [{font, [{size, "-1"}],
                [MainTable, PtsTable]}]}]},
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
    {table, [{width, "100%"}, {cellspacing, 0}],
     [{tr, [], [{td, [], [br]}]}, % vskip
      {tr, [],
       [{th, [{align, left}, {colspan, 4}],
         [{font, [{size, "+1"}],
           [{str, "Points of interest"}]}]}]},
      [pt_table(Pt, ReportCfg) || Pt <- Pts],
      {tr, [], [{td, [], [br]}]}, % vskip
      {tr, [],
       [{th, [{align, left}, {colspan, 4}],
         [{font, [{size, "+1"}],
           [{str, "Samples"}]}]}]},
      samples_table(Samples, ReportCfg)]}.

pt_table({Pt, Value, PVAttrs}, ReportCfg) ->
    Count = ds_pvattrs:get_count(PVAttrs),
    {MinTK, MaxTK} = ds_pvattrs:get_timespan(PVAttrs),
    Data = [{Value, Count, MinTK, MaxTK}],
    [{tr, [], [{td, [], []}]},
     {tr, [{bgcolor, ?COLOR_PTHEAD}],
      [{th, [{align, left}, {colspan, 4}],
        [{term, Pt}]}]}
     | frame(Pt, stats_headers(), Data, fun stats_display_f/1, ReportCfg)].

samples_table(Values, ReportCfg) ->
    Data = [value_row(V) || V <- Values],
    frame(samples, stats_headers(), Data, fun stats_display_f/1, ReportCfg).

value_row({Value, PVAttrs}) ->
    Count = ds_pvattrs:get_count(PVAttrs),
    {MinTK, MaxTK} = ds_pvattrs:get_timespan(PVAttrs),
    {Value, Count, MinTK, MaxTK}.

stats_display_f({Value, Count, MinTK, MaxTK}) ->
    {{term, Value},
     {td, [{align, right}], [{str, ds_utils:integer_to_sigfig(Count)}]},
     {term, MinTK},
     {term, MaxTK}}.

stats_headers() ->
    [["", "", {"Range", [{colspan, 2}]}],
     [{"Value", [{align, left}]},
      {"Count", [{align, right}]},
      "First",
      "Last"]].

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
    Data1 = frame_sort(Data0, Sort),
    Data = [DisplayFun(D) || D <- Data1],
    NDataRows = length(Data),
    HeaderRows = frame_headers(Id, Headers, Sort, NDataRows),
    [HeaderRows | [frame_data_row(D) || D <- Data]].

frame_sort(Data, false) -> Data;
frame_sort(Data, {sort, N, Dir}) -> lists:sort(mk_sort_fun(N, Dir), Data).

mk_sort_fun(N, ascending)  -> fun(T1, T2) -> element(N, T1) =< element(N, T2) end;
mk_sort_fun(N, descending) -> fun(T1, T2) -> element(N, T1) >= element(N, T2) end.

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
    Link = lists:concat([Id, ".", N]),
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

%% Report config
%%
%% A key-value list [{Id, Cfg}] where Id is a frame identifier
%% and Cfg is a further tuple list. Currently this is only used
%% to store a single type of tuple, {sort, ColN, Dir} to specify
%% the currently selected sort on the frame.
config_update(ReportCfg, Link) ->
    [IdStr, ColStr] = string:tokens(Link, "."),
    Id = list_to_atom(IdStr),
    ColN = list_to_integer(ColStr),
    Sort = case config_lookup(Id, sort, ReportCfg) of
               {sort, ColN, Dir} -> {sort, ColN, invert_dir(Dir)};
               _                 -> {sort, ColN, ascending}
           end,
    config_store(Id, Sort, ReportCfg).

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
        false     -> false;
        {Id, Cfg} -> lists:keyfind(Key, 1, Cfg)
    end.

invert_dir(ascending) -> descending;
invert_dir(descending) -> ascending.

%% Generate a HTML-encoded string from a data structure encoding HTML.
html({str, Str}) -> html_esc(Str);
html({term, Term}) -> html({code, [], [{str, io_lib:format("~40p", [Term])}]});
html(Tags) when is_list(Tags) -> lists:concat([html(T) || T <- Tags]);
html(Tag) when is_atom(Tag) -> lists:concat(["<", Tag, ">"]);
html({Tag, Attributes, Contents}) ->
    ValueStrs = [attr_to_str(A) || A <- [Tag|Attributes]],
    TagStr = string:join(ValueStrs, " "),
    ContentStr = lists:concat([html(C) || C <- Contents]),
    lists:concat(["<", TagStr, ">", ContentStr, "</", Tag, ">"]).

attr_to_str({Attr, Value}) -> lists:concat([Attr, "=\"", Value, "\""]);
attr_to_str(Attr) -> lists:concat([Attr]).

html_esc(C) when is_integer(C) -> C;
html_esc(S) -> html_esc(lists:flatten(S), []).

html_esc([], Acc) -> lists:flatten(lists:reverse(Acc));
html_esc([$< | T], Acc) -> html_esc(T, ["&lt;" | Acc]);
html_esc([$> | T], Acc) -> html_esc(T, ["&gt;" | Acc]);
html_esc([$& | T], Acc) -> html_esc(T, ["&amp;" | Acc]);
html_esc([$" | T], Acc) -> html_esc(T, ["&quot;" | Acc]);
html_esc([$' | T], Acc) -> html_esc(T, ["&#39;" | Acc]);
html_esc([$  | T], Acc) -> html_esc(T, ["&nbsp;" | Acc]);
html_esc([$\n | T], Acc) -> html_esc(T, ["<br>" | Acc]);
html_esc([H|T], Acc) -> html_esc(T, [H|Acc]).

%% Tests
-ifdef(TEST).

config_lookup_test() ->
    ?assertEqual(false, config_lookup(myframeid, sort, [])),
    ?assertEqual(false, config_lookup(myframeid, sort, [{myframeid, []}])),
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
    ?assertEqual([{myframeid, [{sort, 1, ascending}]}],
                 config_update([], "myframeid.1")),
    ?assertEqual([{myframeid, [{sort, 1, descending}]}],
                 config_update([{myframeid, [{sort, 1, ascending}]}],
                               "myframeid.1")),
    ?assertEqual([{otherframeid, [{dummy, 123}]},
                  {myframeid, [{sort, 3, ascending}]}],
                 config_update([{otherframeid, [{dummy, 123}]},
                                {myframeid, [{sort, 1, ascending}]}],
                               "myframeid.3")).

html_test() ->
    ?assertEqual("<br>", html(br)),
    ?assertEqual("<body></body>", html({body, [], []})),
    ?assertEqual("<table width=\"100%\" cellspacing=\"0\"></table>",
                 html({table, [{width, "100%"}, {cellspacing, 0}], []})),
    ?assertEqual("<html><body bgcolor=\"#123456\" align=\"left\">"
                 "<table cellpadding=\"0\"><hr><br></table></body></html>",
                 html({html, [],
                       [{body, [{bgcolor, "#123456"}, {align, left}],
                         [{table, [{cellpadding, 0}], [hr, br]}]}]})).

-endif.
