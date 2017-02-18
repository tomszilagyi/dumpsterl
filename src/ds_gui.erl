-module(ds_gui).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-behaviour(wx_object).

%% Client API
-export([start_link/0, stop/1]).

%% wx_object callbacks
-export([init/1, terminate/2,  code_change/3,
         handle_info/2, handle_call/3, handle_cast/2, handle_event/2]).

-include_lib("wx/include/wx.hrl").

-define(LIST_CTRL_STACK,    10).
-define(LIST_CTRL_CHILDREN, 20).

-define(SCROLLBAR_WIDTH,    15).

-record(state,
        {
          config,
          frame,
          panel_main,
          panel_left,
          lc_stack,
          lc_children,
          text_children,
          text_stats,
          text_ext,
          zipper,
          is_complex_type,
          stack_col_widths,
          children_col_widths
        }).

start_link() ->
    Server = wx:new(),
    {_, _, _, Pid} = wx_object:start_link(?MODULE, [Server], []),
    {ok, Pid}.

stop(Pid) ->
    gen_server:call(Pid, shutdown).

init(Config) ->
    wx:batch(fun() -> do_init(Config) end).

do_init([Server] = Config) ->
    Zipper = load_zipper("dataspec.bin"),

    Frame = wxFrame:new(Server, ?wxID_ANY, "Dataspec", []),
    Panel = wxPanel:new(Frame, []),
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

    Text1 = wxStaticText:new(TopLeftPanel, ?wxID_ANY, "Type hierarchy stack", []),
    wxSizer:add(TopLeftSizer, Text1, []),

    LC_Stack = wxListCtrl:new(TopLeftPanel,
                              [{winid, ?LIST_CTRL_STACK},
                               {style, ?wxLC_REPORT bor ?wxLC_SINGLE_SEL}]),
    wxSizer:add(TopLeftSizer, LC_Stack, SizerOpts),
    setup_child_list_cols(LC_Stack, false),
    wxListCtrl:connect(LC_Stack, command_list_item_selected, []),
    wxListCtrl:connect(LC_Stack, size, [{skip, true}]),

    BottomLeftPanel = wxPanel:new(LeftSplitter, []),
    BottomLeftSizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(BottomLeftPanel, BottomLeftSizer),

    TextChildren = wxStaticText:new(BottomLeftPanel, ?wxID_ANY, "Subtype or element list", []),
    wxSizer:add(BottomLeftSizer, TextChildren, []),

    LC_Children = wxListCtrl:new(BottomLeftPanel,
                                 [{winid, ?LIST_CTRL_CHILDREN},
                                  {style, ?wxLC_REPORT bor ?wxLC_SINGLE_SEL}]),
    wxSizer:add(BottomLeftSizer, LC_Children, SizerOpts),
    setup_child_list_cols(LC_Children, false),
    wxListCtrl:connect(LC_Children, command_list_item_selected, []),
    wxListCtrl:connect(LC_Children, size, [{skip, true}]),

    wxSplitterWindow:splitHorizontally(LeftSplitter, TopLeftPanel, BottomLeftPanel),
    wxSplitterWindow:setSashGravity(LeftSplitter, 0.5),
    wxSplitterWindow:setMinimumPaneSize(LeftSplitter, 1),
    wxSizer:add(LeftSizer, LeftSplitter, SizerOpts),

    %% Right panel
    RightPanel = wxPanel:new(Splitter, []),
    RightSplitter = wxSplitterWindow:new(RightPanel, []),
    RightSizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(RightPanel, RightSizer),

    TopRightPanel = wxPanel:new(RightSplitter, []),
    TopRightSizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(TopRightPanel, TopRightSizer),
    TextStats = wxTextCtrl:new(TopRightPanel, ?wxID_ANY,
                               [{value, "Statistics"},
                                {style, ?wxDEFAULT bor ?wxTE_MULTILINE}]),
    wxSizer:add(TopRightSizer, TextStats, SizerOpts),

    BottomRightPanel = wxPanel:new(RightSplitter, []),
    BottomRightSizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(BottomRightPanel, BottomRightSizer),
    TextExt = wxTextCtrl:new(BottomRightPanel, ?wxID_ANY,
                             [{value, "Extended data"},
                              {style, ?wxDEFAULT bor ?wxTE_MULTILINE}]),
    wxSizer:add(BottomRightSizer, TextExt, SizerOpts),

    wxSplitterWindow:splitHorizontally(RightSplitter, TopRightPanel, BottomRightPanel),
    wxSplitterWindow:setSashGravity(RightSplitter, 0.5),
    wxSplitterWindow:setMinimumPaneSize(RightSplitter, 1),
    wxSizer:add(RightSizer, RightSplitter, SizerOpts),

    %% Main vertical splitter
    wxSplitterWindow:splitVertically(Splitter, LeftPanel, RightPanel),
    wxSplitterWindow:setSashGravity(Splitter, 0.5),
    wxSplitterWindow:setMinimumPaneSize(Splitter, 1),
    wxSizer:add(Sizer, Splitter, SizerOpts),

    wxFrame:show(Frame),
    State = #state{config=Config, frame=Frame, panel_main=Panel,
                   panel_left=LeftPanel,
                   lc_stack=LC_Stack, lc_children=LC_Children,
                   text_children=TextChildren,
                   text_stats=TextStats, text_ext=TextExt,
                   zipper=Zipper},
    {Frame, update_gui(State)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Async Events are handled in handle_event as in handle_info
handle_event(#wx{id = ?LIST_CTRL_STACK, obj = LC,
                 event = #wxList{itemIndex = Item}},
             State0 = #state{zipper=Zipper0}) ->
    FramesUp = wxListCtrl:getItemCount(LC) - Item - 1,
    Zipper = ds_zipper:nth_parent(Zipper0, FramesUp),
    State = State0#state{zipper=Zipper},
    {noreply, update_gui(State)};
handle_event(#wx{id = ?LIST_CTRL_CHILDREN,
                 event = #wxList{itemIndex = Item}},
             State0 = #state{zipper=Zipper0}) ->
    Zipper = ds_zipper:nth_child(Zipper0, Item),
    State = State0#state{zipper=Zipper},
    {noreply, update_gui(State)};
handle_event(#wx{event = #wxSize{}}, State) ->
    {noreply, adjust_list_cols(State)};
handle_event(#wx{} = Event, State = #state{}) ->
    io:format(user, "Event ~p~n", [Event]),
    {noreply, State}.

%% Callbacks handled as normal gen_server callbacks
handle_info(_Msg, State) ->
    {noreply, State}.

handle_call(shutdown, _From, State=#state{frame=Frame}) ->
    wxFrame:destroy(Frame),
    {stop, normal, ok, State};

handle_call(_Msg, _From, State) ->
    {reply, {error, nyi}, State}.

handle_cast(Msg, State) ->
    io:format(user, "Got cast ~p~n",[Msg]),
    {noreply, State}.

code_change(_, _, State) ->
    {stop, ignore, State}.

terminate(_Reason, _State) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

update_gui(#state{lc_stack=LC_Stack, lc_children=LC_Children,
                  text_children=TextChildren,
                  text_stats=TextStats, text_ext=TextExt,
                  zipper=Zipper} = State0) ->
    wxListCtrl:deleteAllItems(LC_Stack),
    Stack = ds_zipper:stack(Zipper),
    LastIdx = length(Stack)-1,
    StWidths = add_stack(LC_Stack, Stack),
    wxListCtrl:setItemBackgroundColour(LC_Stack, LastIdx, {64, 128, 192}),
    wxListCtrl:setItemTextColour(LC_Stack, LastIdx, {255, 255, 255}),

    IsComplexType = ds_types:kind(ds_zipper:class(Zipper)) =:= complex,
    set_child_text(TextChildren, IsComplexType),
    setup_child_list_cols(LC_Children, IsComplexType),
    ChWidths = add_stack(LC_Children, child_stack(Zipper, IsComplexType)),
    State1 = State0#state{is_complex_type=IsComplexType,
                          stack_col_widths = StWidths,
                          children_col_widths = ChWidths},
    State = adjust_list_cols(State1),

    {Stats, Ext} = ds_zipper:data(Zipper),
    StatsStr = io_lib:format("~p", [Stats]),
    ExtStr = io_lib:format("~p", [Ext]),
    wxTextCtrl:setValue(TextStats, StatsStr),
    wxTextCtrl:setValue(TextExt, ExtStr),

    State.

set_child_text(Text, false = _IsComplexType) ->
    wxStaticText:setLabel(Text, "List of subtypes");
set_child_text(Text, true = _IsComplexType) ->
    wxStaticText:setLabel(Text, "List of type parameters").

child_stack(Zipper, false = _IsComplexType) ->
    ds_zipper:child_list(Zipper);
child_stack(Zipper, true = _IsComplexType) ->
    Class = ds_zipper:class(Zipper),
    Data = ds_zipper:data(Zipper),
    Attributes = ds_types:attributes(Class, Data),
    ChildList = ds_zipper:child_list(Zipper),
    true = length(Attributes) == length(ChildList),
    AttChList = lists:zip(Attributes, ChildList),
    [{Field, Attr, Type, Count} || {{Field, Attr}, {Type, Count}} <- AttChList].

setup_child_list_cols(LC, false = _IsComplexType) ->
    delete_items_and_cols(LC),
    wxListCtrl:insertColumn(LC, 0, "Type", []),
    wxListCtrl:insertColumn(LC, 1, "Count", [{format, ?wxLIST_FORMAT_RIGHT}]);
setup_child_list_cols(LC, true = _IsComplexType) ->
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

adjust_list_cols(#state{panel_left=LeftPanel,
                        lc_stack=LC_Stack, lc_children=LC_Children,
                        is_complex_type=IsComplexType,
                        stack_col_widths = StWidths0,
                        children_col_widths = ChWidths0} = State) ->
    %% use the already measured "desired" widths to compute the actual
    %% widths and set them
    LastColW = get_last_col_width(StWidths0, ChWidths0),
    {W,_H} = wxWindow:getSize(LeftPanel),
    StWidths = set_stack_cols_width(LC_Stack, StWidths0, W, LastColW),
    ChWidths = set_children_cols_width(LC_Children, ChWidths0, W, LastColW, IsComplexType),
    State#state{stack_col_widths = StWidths,
                children_col_widths = ChWidths}.

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

set_children_cols_width(LC, [], TotalW, LastColW, _IsComplexType) ->
    C0 = TotalW - LastColW - ?SCROLLBAR_WIDTH,
    set_stack_cols_width(LC, [C0, LastColW], TotalW, LastColW);
set_children_cols_width(LC, Ws, TotalW, LastColW, false = _IsComplexType) ->
    set_stack_cols_width(LC, Ws, TotalW, LastColW);
set_children_cols_width(LC, [C0req, C1req, C2req,_C3req], TotalW, LastColW, true = _IsComplexType) ->
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
add_stack(LC, [{Type, Count}|Rest], DC, Acc0, N) ->
    TypeStr = ds_types:to_string(Type),
    CountStr = integer_to_list(Count),
    Acc = fold_widths(text_widths(DC, [TypeStr, CountStr]), Acc0),
    wxListCtrl:insertItem(LC, N, ""),
    wxListCtrl:setItem(LC, N, 0, TypeStr),
    wxListCtrl:setItem(LC, N, 1, CountStr),
    add_stack(LC, Rest, DC, Acc, N+1);
add_stack(LC, [{No, Attribute, Type, Count}|Rest], DC, Acc0, N) ->
    NoStr = integer_to_list(No),
    TypeStr = ds_types:to_string(Type),
    CountStr = integer_to_list(Count),
    TextWidths = text_widths(DC, [NoStr, Attribute, TypeStr, CountStr]),
    Acc = fold_widths(TextWidths, Acc0),
    wxListCtrl:insertItem(LC, N, ""),
    wxListCtrl:setItem(LC, N, 0, NoStr),
    wxListCtrl:setItem(LC, N, 1, Attribute),
    wxListCtrl:setItem(LC, N, 2, TypeStr),
    wxListCtrl:setItem(LC, N, 3, CountStr),
    add_stack(LC, Rest, DC, Acc, N+1).

text_widths(DC, Cols) ->
    [text_width(DC, Str) || Str <- Cols].

text_width(DC, Str) ->
    {W,_H} = wxDC:getTextExtent(DC, Str),
    W+8. %% FIXME

fold_widths(Ws, []) -> Ws;
fold_widths(Ws, Acc) ->
    lists:map(fun({W, Aw}) -> max(W, Aw) end, lists:zip(Ws, Acc)).

load_zipper(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    Tree = binary_to_term(Bin),
    ds_zipper:from_tree(ds:join_up(ds:simplify(Tree))).
