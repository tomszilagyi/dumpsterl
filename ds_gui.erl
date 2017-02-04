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

-record(state,
        {
          config,
          frame,
          panel_main,
          panel_left,
          lc_stack,
          lc_children,
          text_stats,
          text_ext,
          zipper
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
    wxListCtrl:insertColumn(LC_Stack, 0, "Type", []),
    wxListCtrl:insertColumn(LC_Stack, 1, "Count", [{format, ?wxLIST_FORMAT_RIGHT}]),
    wxListCtrl:setColumnWidth(LC_Stack, 1, ?wxLIST_AUTOSIZE),
    wxListCtrl:connect(LC_Stack, command_list_item_selected, []),
    wxListCtrl:connect(LC_Stack, size, [{skip, true}]),

    BottomLeftPanel = wxPanel:new(LeftSplitter, []),
    BottomLeftSizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(BottomLeftPanel, BottomLeftSizer),

    Text2 = wxStaticText:new(BottomLeftPanel, ?wxID_ANY, "Subtype or element list", []),
    wxSizer:add(BottomLeftSizer, Text2, []),

    LC_Children = wxListCtrl:new(BottomLeftPanel,
                                 [{winid, ?LIST_CTRL_CHILDREN},
                                  {style, ?wxLC_REPORT bor ?wxLC_SINGLE_SEL}]),
    wxSizer:add(BottomLeftSizer, LC_Children, SizerOpts),
    wxListCtrl:insertColumn(LC_Children, 0, "Type", []),
    wxListCtrl:insertColumn(LC_Children, 1, "Count", [{format, ?wxLIST_FORMAT_RIGHT}]),
    wxListCtrl:setColumnWidth(LC_Children, 1, ?wxLIST_AUTOSIZE),
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
    TextStats = wxStaticText:new(TopRightPanel, ?wxID_ANY, "Statistics", []),
    wxSizer:add(TopRightSizer, TextStats, []),

    BottomRightPanel = wxPanel:new(RightSplitter, []),
    BottomRightSizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(BottomRightPanel, BottomRightSizer),
    TextExt = wxStaticText:new(BottomRightPanel, ?wxID_ANY, "Extended data", []),
    wxSizer:add(BottomRightSizer, TextExt, []),

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
                  text_stats=TextStats, text_ext=TextExt,
                  zipper=Zipper} = State) ->
    wxListCtrl:deleteAllItems(LC_Stack),
    Stack = ds_zipper:stack(Zipper),
    LastIdx = length(Stack)-1,
    add_stack(LC_Stack, Stack, 0),
    wxListCtrl:setItemBackgroundColour(LC_Stack, LastIdx, {64, 128, 192}),
    wxListCtrl:setItemTextColour(LC_Stack, LastIdx, {255, 255, 255}),

    wxListCtrl:deleteAllItems(LC_Children),
    add_stack(LC_Children, ds_zipper:child_list(Zipper), 0),

    {Stats, Ext} = ds_zipper:data(Zipper),
    StatsStr = io_lib:format("~p", [Stats]),
    ExtStr = io_lib:format("~p", [Ext]),
    wxStaticText:setLabel(TextStats, StatsStr),
    wxStaticText:setLabel(TextExt, ExtStr),
    adjust_list_cols(State).

adjust_list_cols(#state{panel_left=LeftPanel,
                        lc_stack=LC_Stack, lc_children=LC_Children} = State) ->
    wxListCtrl:setColumnWidth(LC_Stack, 1, ?wxLIST_AUTOSIZE),
    wxListCtrl:setColumnWidth(LC_Children, 1, ?wxLIST_AUTOSIZE),
    Col1W = max(wxListCtrl:getColumnWidth(LC_Stack, 1),
                wxListCtrl:getColumnWidth(LC_Children, 1)),
    {W,_H} = wxWindow:getSize(LeftPanel),
    Col0W = W - Col1W - 15,
    if Col0W > 0 ->
            wxListCtrl:setColumnWidth(LC_Stack, 0, Col0W),
            wxListCtrl:setColumnWidth(LC_Stack, 1, Col1W),
            wxListCtrl:setColumnWidth(LC_Children, 0, Col0W),
            wxListCtrl:setColumnWidth(LC_Children, 1, Col1W);
       true -> ok
    end,
    State.

%% populate ListCtrl with stack
add_stack(_LC, [], N) -> N;
add_stack(LC, [{Type, Count}|Rest], N) ->
    TypeStr = io_lib:format("~p", [Type]),
    wxListCtrl:insertItem(LC, N, ""),
    wxListCtrl:setItem(LC, N, 0, TypeStr),
    wxListCtrl:setItem(LC, N, 1, integer_to_list(Count)),
    add_stack(LC, Rest, N+1).

load_zipper(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    Tree = binary_to_term(Bin),
    ds_zipper:from_tree(ds:join_up(ds:simplify(Tree))).
