%% -*- coding: utf-8 -*-

%% @private
-module(ds_gui).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% Client API
-export([ start/0
        , start/1
        ]).

-include("config.hrl").
-ifdef(CONFIG_WXGUI).

-behaviour(wx_object).

%% wx_object callbacks
-export([init/1, terminate/2, code_change/3,
         handle_info/2, handle_call/3, handle_cast/2, handle_event/2]).

-include_lib("wx/include/wx.hrl").

-define(LIST_CTRL_STACK,    10).
-define(LIST_CTRL_CHILDREN, 20).

-define(SCROLLBAR_WIDTH,    15).

-define(ZOOM_LEVEL_MIN,      0).
-define(ZOOM_LEVEL_DEFAULT,  2).
-define(ZOOM_LEVEL_MAX,      7).
-define(ZOOM_POINT_SIZES,    [ {0, 9}
                             , {1, 10}
                             , {2, 11}
                             , {3, 12}
                             , {4, 14}
                             , {5, 16}
                             , {6, 18}
                             , {7, 20}
                             ]).

-record(state,
        { config
        , frame
        , panel_main
        , panel_left
        , panel_right
        , lc_stack
        , lc_stack_size
        , lc_children
        , text_children
        , report_html_win
        , report_cfg
        , report_scroll_pos
        , zipper
        , is_generic_type
        , stack_col_widths
        , children_col_widths
        , zoom_level
        }).

start() -> start("ds.bin").

start(Spec) ->
    Server = wx:new(),
    wx_object:start(?MODULE, [Server, Spec], []).

init(Config) ->
    wx:batch(fun() -> do_init(Config) end).

do_init([Server, Spec] = Config) ->
    {Zipper, Meta} = load_spec(Spec),
    Options = proplists:get_value(options, Meta, []),
    WinTitle = case proplists:get_value(dump, Options, undefined) of
                   undefined -> "Dumpsterl";
                   Filename  -> "Dumpsterl ["++Filename++"]"
               end,
    Frame = wxFrame:new(Server, ?wxID_ANY, WinTitle, []),
    Panel = wxPanel:new(Frame, []),
    wxPanel:connect(Panel, char_hook, [{skip, true}]),
    Sizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(Panel, Sizer),
    Splitter = wxSplitterWindow:new(Panel, []),
    SizerOpts = [{flag, ?wxEXPAND}, {proportion, 1}],

    %% Left panel
    LeftPanel = wxPanel:new(Splitter, []),
    LeftSplitter = wxSplitterWindow:new(LeftPanel, []),
    LeftSizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(LeftPanel, LeftSizer),

    TopLeftPanel = wxPanel:new(LeftSplitter, []),
    TopLeftSizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(TopLeftPanel, TopLeftSizer),

    Text1 = wxStaticText:new(TopLeftPanel, ?wxID_ANY, " Stack", []),
    set_window_font_bold(Text1),
    _ = wxSizer:addSpacer(TopLeftSizer, 3),
    _ = wxSizer:add(TopLeftSizer, Text1, []),
    _ = wxSizer:addSpacer(TopLeftSizer, 3),

    LC_Stack = wxListCtrl:new(TopLeftPanel,
                              [{winid, ?LIST_CTRL_STACK},
                               {style, ?wxLC_REPORT bor ?wxLC_SINGLE_SEL}]),
    _ = wxSizer:add(TopLeftSizer, LC_Stack, SizerOpts),
    setup_child_list_cols(LC_Stack, false),
    wxListCtrl:connect(LC_Stack, command_list_item_selected, []),
    wxListCtrl:connect(LC_Stack, size, [{skip, true}]),

    BottomLeftPanel = wxPanel:new(LeftSplitter, []),
    BottomLeftSizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(BottomLeftPanel, BottomLeftSizer),

    TextChildren = wxStaticText:new(BottomLeftPanel, ?wxID_ANY, "", []),
    set_window_font_bold(TextChildren),
    _ = wxSizer:addSpacer(BottomLeftSizer, 3),
    _ = wxSizer:add(BottomLeftSizer, TextChildren, []),
    _ = wxSizer:addSpacer(BottomLeftSizer, 3),

    LC_Children = wxListCtrl:new(BottomLeftPanel,
                                 [{winid, ?LIST_CTRL_CHILDREN},
                                  {style, ?wxLC_REPORT bor ?wxLC_SINGLE_SEL}]),
    _ = wxSizer:add(BottomLeftSizer, LC_Children, SizerOpts),
    setup_child_list_cols(LC_Children, false),
    wxListCtrl:connect(LC_Children, command_list_item_selected, []),

    wxSplitterWindow:splitHorizontally(LeftSplitter, TopLeftPanel, BottomLeftPanel),
    wxSplitterWindow:setSashGravity(LeftSplitter, 0.5),
    wxSplitterWindow:setMinimumPaneSize(LeftSplitter, 1),
    _ = wxSizer:add(LeftSizer, LeftSplitter, SizerOpts),

    RightPanel = wxPanel:new(Splitter, []),
    RightSizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(RightPanel, RightSizer),
    ReportHtmlWin = wxHtmlWindow:new(RightPanel, []),
    wxHtmlWindow:connect(ReportHtmlWin, command_html_link_clicked, []),
    [wxHtmlWindow:connect(ReportHtmlWin, Event, [{skip, true}]) ||
        Event <- [scrollwin_top, scrollwin_bottom,
                  scrollwin_lineup, scrollwin_linedown,
                  scrollwin_pageup, scrollwin_pagedown,
                  scrollwin_thumbtrack, scrollwin_thumbrelease]],
    _ = wxSizer:add(RightSizer, ReportHtmlWin, SizerOpts),

    %% Main vertical splitter
    wxSplitterWindow:splitVertically(Splitter, LeftPanel, RightPanel),
    wxSplitterWindow:setSashGravity(Splitter, 0.5),
    wxSplitterWindow:setMinimumPaneSize(Splitter, 1),
    _ = wxSizer:add(Sizer, Splitter, SizerOpts),

    wxFrame:show(Frame),
    State = #state{config=Config, frame=Frame, panel_main=Panel,
                   panel_left=LeftPanel, panel_right=RightPanel,
                   lc_stack=LC_Stack, lc_children=LC_Children,
                   text_children=TextChildren,
                   report_html_win=ReportHtmlWin,
                   report_scroll_pos={0,0}, report_cfg=[],
                   zipper=Zipper, zoom_level=?ZOOM_LEVEL_DEFAULT},
    {Frame, update_zoom_level(State)}. % NB. includes update_gui/1.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Async Events are handled in handle_event as in handle_info
handle_event(#wx{id = ?LIST_CTRL_STACK, obj = LC,
                 event = #wxList{itemIndex = Item}},
             State0 = #state{zipper=Zipper0}) ->
    FramesUp = wxListCtrl:getItemCount(LC) - Item - 1,
    Zipper = ds_zipper:nth_parent(Zipper0, FramesUp),
    State = State0#state{zipper=Zipper, report_scroll_pos={0,0}},
    {noreply, update_gui(State)};
handle_event(#wx{id = ?LIST_CTRL_CHILDREN,
                 event = #wxList{itemIndex = Item}},
             State0 = #state{zipper=Zipper0}) ->
    Zipper = ds_zipper:nth_child(Zipper0, Item),
    State = State0#state{zipper=Zipper, report_scroll_pos={0,0}},
    {noreply, update_gui(State)};
handle_event(#wx{event = #wxSize{size=Size}}, #state{lc_stack_size=Size}=State) ->
    {noreply, State};
handle_event(#wx{event = #wxSize{size=Size}}, State) ->
    {noreply, update_report(adjust_size(State#state{lc_stack_size=Size}))};
handle_event(#wx{event = #wxHtmlLink{linkInfo = #wxHtmlLinkInfo{href=Link}}},
	     #state{report_cfg=ReportCfg0} = State) ->
    ReportCfg = ds_reports:config_update(ReportCfg0, Link),
    {noreply, update_report(State#state{report_cfg=ReportCfg})};
handle_event(#wx{event = #wxScrollWin{}},
             #state{report_html_win=ReportHtmlWin} = State) ->
    ScrollPos = wxScrolledWindow:getViewStart(ReportHtmlWin),
    {noreply, State#state{report_scroll_pos=ScrollPos}};
handle_event(#wx{event = #wxKey{controlDown=true, keyCode=$+}},
             #state{zoom_level = ZoomLevel0} = State) ->
    ZoomLevel = min(ZoomLevel0 + 1, ?ZOOM_LEVEL_MAX),
    {noreply, update_zoom_level(State#state{zoom_level = ZoomLevel})};
handle_event(#wx{event = #wxKey{controlDown=true, keyCode=$-}},
             #state{zoom_level = ZoomLevel0} = State) ->
    ZoomLevel = max(?ZOOM_LEVEL_MIN, ZoomLevel0 - 1),
    {noreply, update_zoom_level(State#state{zoom_level = ZoomLevel})};
handle_event(#wx{event = #wxKey{}}, State) ->
    {noreply, State};
handle_event(#wx{} = Event, State = #state{}) ->
    io:format(user, "Event ~p~n", [Event]),
    {noreply, State}.

%% Callbacks handled as normal gen_server callbacks
handle_info(_Msg, State) ->
    {noreply, State}.

handle_call(_Msg, _From, State) ->
    {reply, {error, nyi}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

code_change(_, _, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

update_gui(#state{lc_stack=LC_Stack, lc_children=LC_Children,
                  text_children=TextChildren, zipper=Zipper} = State0) ->
    wxListCtrl:deleteAllItems(LC_Stack),
    StackTransFun = fun({Class, Data,_ChildL}, NthChild) ->
                            TypeStr = ds_types:type_to_string(Class),
                            {Class, TypeStr, NthChild, Data}
                    end,
    Stack0 = ds_zipper:stack(Zipper, StackTransFun),
    Stack = stack_with_parent_refs(Stack0),
    LastIdx = length(Stack)-1,
    StWidths = add_stack(LC_Stack, Stack),
    wxListCtrl:setItemBackgroundColour(LC_Stack, LastIdx, {64, 128, 192}),
    wxListCtrl:setItemTextColour(LC_Stack, LastIdx, {255, 255, 255}),

    IsGenericType = ds_types:kind(ds_zipper:class(Zipper)) =:= generic,
    set_child_text(TextChildren, IsGenericType),
    setup_child_list_cols(LC_Children, IsGenericType),
    ChWidths = add_stack(LC_Children, child_stack(Zipper, IsGenericType)),
    State = State0#state{is_generic_type=IsGenericType,
                         stack_col_widths = StWidths,
                         children_col_widths = ChWidths},
    update_report(adjust_size(State)).

update_report(#state{report_cfg=ReportCfg,
                     report_html_win=ReportHtmlWin,
                     report_scroll_pos=ScrollPos,
                     zipper=Zipper} = State) ->
    Tree = ds_zipper:to_tree(Zipper),
    Page = ds_reports:report_page(Tree, ReportCfg),
    wxHtmlWindow:setPage(ReportHtmlWin, Page),
    scrolled_win_set_pos(ReportHtmlWin, ScrollPos),
    State.

scrolled_win_set_pos(Win, {Px, Py}) ->
    wxScrolledWindow:scroll(Win, Px, Py).

update_zoom_level(#state{panel_main = Panel,
                         report_cfg = ReportCfg0,
                         zoom_level = ZoomLevel} = State) ->
    set_window_fontsize(Panel, ZoomLevel),
    ReportCfg = ds_reports:config_store(report, {zoom_level, ZoomLevel}, ReportCfg0),
    update_gui(State#state{report_cfg = ReportCfg}).

set_window_fontsize(Win, ZoomLevel) ->
    Font = wxWindow:getFont(Win),
    {_, PointSize} = lists:keyfind(ZoomLevel, 1, ?ZOOM_POINT_SIZES),
    wxFont:setPointSize(Font, PointSize),
    wxWindow:setFont(Win, Font),
    [set_window_fontsize(Ch, ZoomLevel) || Ch <- wxWindow:getChildren(Win)],
    wxWindow:layout(Win),
    ok.

set_window_font_bold(Win) ->
    Font = wxWindow:getFont(Win),
    wxFont:setWeight(Font, ?wxFONTWEIGHT_BOLD),
    wxWindow:setFont(Win, Font),
    ok.

%% Produce a stack of {TypeStr, Count} with the type strings
%% enriched with parent refs
stack_with_parent_refs(Stack) ->
    Shifted = [{undefined, undefined, undefined, undefined} |
               lists:sublist(Stack, length(Stack)-1)],
    [{parent_ref(ParentClass, ParentData, Nth) ++ TypeStr,
      ds_stats:get_count(Stats)}
     || {{_Class, TypeStr, Nth, {Stats,_Ext}},
         {ParentClass,_ParentTypeStr,_ParentNth, ParentData}}
            <- lists:zip(Stack, Shifted)].

%% Look up the attributes for the Nth child
parent_ref(undefined, undefined,_Nth) -> "";
parent_ref(Class, Data, Nth) ->
    case ds_types:kind(Class) of
        generic ->
            Attrs = ds_types:attributes(Class, Data),
            "[" ++ ds_types:attribute_to_string(Class, lists:nth(Nth, Attrs)) ++ "]: ";
        _ -> ""
    end.

set_child_text(Text, false = _IsGenericType) ->
    wxStaticText:setLabel(Text, " Alternatives");
set_child_text(Text, true = _IsGenericType) ->
    wxStaticText:setLabel(Text, " Attributes").

child_stack(Zipper, false = _IsGenericType) ->
    child_list_with_stats(Zipper);
child_stack(Zipper, true = _IsGenericType) ->
    Class = ds_zipper:class(Zipper),
    Data = ds_zipper:data(Zipper),
    Attributes = ds_types:attributes(Class, Data),
    ChildList = child_list_with_stats(Zipper),
    true = length(Attributes) == length(ChildList),
    AttChList = lists:zip(Attributes, ChildList),
    [{Field, Attr, TypeStr, Count}
     || {{Field, Attr}, {TypeStr, Count}} <- AttChList].

child_list_with_stats(Zipper) ->
    TransFun = fun({Class, {Stats,_Ext},_ChildL}) ->
                       TypeStr = ds_types:type_to_string(Class),
                       {TypeStr, ds_stats:get_count(Stats)}
               end,
    ds_zipper:child_list(Zipper, TransFun).

setup_child_list_cols(LC, false = _IsGenericType) ->
    delete_items_and_cols(LC),
    wxListCtrl:insertColumn(LC, 0, "Type", []),
    wxListCtrl:insertColumn(LC, 1, "Count", [{format, ?wxLIST_FORMAT_RIGHT}]);
setup_child_list_cols(LC, true = _IsGenericType) ->
    delete_items_and_cols(LC),
    AlignRight = [{format, ?wxLIST_FORMAT_RIGHT}],
    wxListCtrl:insertColumn(LC, 0, "E", AlignRight),
    wxListCtrl:insertColumn(LC, 1, "Attribute", []),
    wxListCtrl:insertColumn(LC, 2, "Type", []),
    wxListCtrl:insertColumn(LC, 3, "Count", AlignRight).

delete_items_and_cols(LC) ->
    wxListCtrl:deleteAllItems(LC),
    NCols = wxListCtrl:getColumnCount(LC),
    [wxListCtrl:deleteColumn(LC, Col) || Col <- lists:seq(NCols-1, 0, -1)],
    ok.

adjust_size(#state{panel_left=LeftPanel, panel_right=RightPanel,
                   lc_stack=LC_Stack, lc_children=LC_Children,
                   is_generic_type=IsGenericType,
                   stack_col_widths = StWidths0,
                   children_col_widths = ChWidths0,
                   report_html_win = ReportHtmlWin,
                   report_scroll_pos = ReportScrollPos,
                   report_cfg = ReportCfg0} = State) ->
    %% use the already measured "desired" widths to compute the actual
    %% widths and set them
    LastColW = get_last_col_width(StWidths0, ChWidths0),
    {W,_H} = wxWindow:getSize(LeftPanel),
    StWidths = set_stack_cols_width(LC_Stack, StWidths0, W, LastColW),
    ChWidths = set_children_cols_width(LC_Children, ChWidths0, W, LastColW,
                                       IsGenericType),

    %% Save the report area width in the report config
    {ReportW,_ReportH} = wxWindow:getSize(RightPanel),
    ReportCfg = ds_reports:config_store(report, {width, ReportW}, ReportCfg0),
    scrolled_win_set_pos(ReportHtmlWin, ReportScrollPos),

    State#state{stack_col_widths = StWidths,
                children_col_widths = ChWidths,
                report_cfg = ReportCfg}.

%% keep the last column width the same for both list widgets
get_last_col_width(StWidths, []) -> lists:last(StWidths);
get_last_col_width(StWidths, ChWidths) ->
    max(lists:last(StWidths), lists:last(ChWidths)).

set_stack_cols_width(LC, [C0req,_C1req], TotalW, LastColW) ->
    C0 = TotalW - LastColW - ?SCROLLBAR_WIDTH,
    if C0 > 0 ->
            wxListCtrl:setColumnWidth(LC, 0, C0),
            wxListCtrl:setColumnWidth(LC, 1, LastColW),
            [C0, LastColW];
       true ->
            [C0req, LastColW]
    end.

set_children_cols_width(LC, [], TotalW, LastColW, _IsGenericType) ->
    C0 = TotalW - LastColW - ?SCROLLBAR_WIDTH,
    set_stack_cols_width(LC, [C0, LastColW], TotalW, LastColW);
set_children_cols_width(LC, Ws, TotalW, LastColW, false = _IsGenericType) ->
    set_stack_cols_width(LC, Ws, TotalW, LastColW);
set_children_cols_width(LC, [C0req, C1req, C2req,_C3req], TotalW, LastColW,
                        true = _IsGenericType) ->
    RemW = TotalW - C0req - LastColW - ?SCROLLBAR_WIDTH,
    if RemW > 0 ->
            %% Divide RemW between col 1 and 2 in proportion to their need
            C1 = RemW * C1req div (C1req + C2req),
            C2 = RemW - C1,
            wxListCtrl:setColumnWidth(LC, 0, C0req),
            wxListCtrl:setColumnWidth(LC, 1, C1),
            wxListCtrl:setColumnWidth(LC, 2, C2),
            wxListCtrl:setColumnWidth(LC, 3, LastColW),
            [C0req, C1, C2, LastColW];
       true ->
            [C0req, C1req, C2req, LastColW]
    end.

%% populate ListCtrl with stack, return a list of "natural" column widths
add_stack(LC, Data) ->
    DC = wxWindowDC:new(LC),
    add_stack(LC, Data, DC, [], 0).

add_stack(_LC, [], DC, Acc,_N) ->
    wxWindowDC:destroy(DC),
    Acc;
add_stack(LC, [{TypeStr, Count}|Rest], DC, Acc0, N) ->
    CountStr = ds_utils:integer_to_sigfig(Count),
    Acc = fold_widths(text_widths(DC, [TypeStr, CountStr]), Acc0),
    lc_add_row(LC, N, [TypeStr, CountStr]),
    add_stack(LC, Rest, DC, Acc, N+1);
add_stack(LC, [{No, Attribute, TypeStr, Count}|Rest], DC, Acc0, N) ->
    NoStr = integer_to_list(No),
    CountStr = ds_utils:integer_to_sigfig(Count),
    TextWidths = text_widths(DC, [NoStr, Attribute, TypeStr, CountStr]),
    Acc = fold_widths(TextWidths, Acc0),
    lc_add_row(LC, N, [NoStr, Attribute, TypeStr, CountStr]),
    add_stack(LC, Rest, DC, Acc, N+1).

text_widths(DC, Cols) ->
    [text_width(DC, Str) || Str <- Cols].

text_width(DC, Str) ->
    {W,_H} = wxDC:getTextExtent(DC, Str),
    if W  > 0 -> W + 8; %% FIXME
       W == 0 -> 0
    end.

fold_widths(Ws, []) -> Ws;
fold_widths(Ws, Acc) ->
    lists:map(fun({W, Aw}) -> max(W, Aw) end, lists:zip(Ws, Acc)).

lc_add_row(LC, N, Items) ->
    wxListCtrl:insertItem(LC, N, ""),
    ItemsCols = lists:zip(Items, lists:seq(0, length(Items)-1)),
    [wxListCtrl:setItem(LC, N, Col, Item) || {Item, Col} <- ItemsCols],
    ok.

load_spec({_Class,_Data,_Children}=Tree0) ->
    {Tree, Meta} = decode_spec(Tree0),
    io:format(user, "Spec metadata: ~p~n", [Meta]),
    {ds_zipper:from_tree(ds_spec:postproc(Tree)), Meta};
load_spec(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    load_spec(erlang:binary_to_term(Bin)).

decode_spec({Class, {Stats, MetaExt}, Children} = Tree0) ->
    case MetaExt of
        [{meta, Meta} | Ext] ->
            Tree = {Class, {Stats, Ext}, Children},
            {Tree, Meta};
        _ ->
            {Tree0, []}
    end.

-else.
%% In case the GUI is disabled:
start() -> throw(wx_gui_disabled).
start(_) -> throw(wx_gui_disabled).
-endif.
