-module(ds_gui).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-behaviour(wx_object).

%% Client API
-export([start_link/0, stop/1]).

%% wx_object callbacks
-export([init/1, terminate/2,  code_change/3,
         handle_info/2, handle_call/3, handle_cast/2, handle_event/2]).

-include_lib("wx/include/wx.hrl").

-record(state,
        {
          config,
          frame,
          panel,
          panel_left,
          panel_left_div,
          panel_right,
          panel_right_div,
          sash_top_left,
          sash_bottom_left,
          sash_top_right,
          sash_bottom_right
        }).

-define(PANEL_LEFT,        10).
-define(PANEL_RIGHT,       20).
-define(SASH_TOP_LEFT,     30).
-define(SASH_BOTTOM_LEFT,  40).
-define(SASH_TOP_RIGHT,    50).
-define(SASH_BOTTOM_RIGHT, 60).

start_link() ->
    Server = wx:new(),
    {_, _, _, Pid} = wx_object:start_link(?MODULE, [Server], []),
    {ok, Pid}.

stop(Pid) ->
    gen_server:call(Pid, shutdown).

init(Config) ->
    wx:batch(fun() -> do_init(Config) end).

do_init([Server] = Config) ->
    Frame = wxFrame:new(Server, ?wxID_ANY, "Dataspec", []),
    Panel = wxPanel:new(Frame, []),
    Sizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(Panel, Sizer),
    Splitter = wxSplitterWindow:new(Panel, []),
    SizerOpts = [{flag, ?wxEXPAND}, {proportion, 1}],

    %% Left panel
    LeftPanel = wxPanel:new(Splitter, [{winid, ?PANEL_LEFT}]),
    LeftSizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(LeftPanel, LeftSizer),

    TopLeftSash = wxSashWindow:new(LeftPanel, [{id, ?SASH_TOP_LEFT}, {style, ?wxSW_3D}]),
    TopLeftPanel = wxPanel:new(TopLeftSash, []),
    TopLeftSizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(TopLeftPanel, TopLeftSizer),

    Text1 = wxStaticText:new(TopLeftPanel, ?wxID_ANY, "Type hierarchy stack", []),
    wxSizer:add(TopLeftSizer, Text1, []),

    LC1 = wxListCtrl:new(TopLeftPanel, [{style, ?wxLC_REPORT bor ?wxLC_SINGLE_SEL}]),
    wxSizer:add(TopLeftSizer, LC1, SizerOpts),
    wxListCtrl:insertColumn(LC1, 0, "Type", []),
    wxListCtrl:insertColumn(LC1, 1, "Count", []),
    wxListCtrl:insertItem(LC1, 0, ""),
    wxListCtrl:setItem(LC1, 0, 0, "T"),
    wxListCtrl:setItem(LC1, 0, 1, "123,456"),
    wxListCtrl:insertItem(LC1, 1, ""),
    wxListCtrl:setItem(LC1, 1, 0, "tuple"),
    wxListCtrl:setItem(LC1, 1, 1, "123,456"),
    wxListCtrl:insertItem(LC1, 2, ""),
    wxListCtrl:setItem(LC1, 2, 0, "{record, {kcase, 21}}"),
    wxListCtrl:setItem(LC1, 2, 1, "123,456"),
    wxListCtrl:connect(LC1, command_list_item_selected, []),

    BottomLeftSash = wxSashWindow:new(LeftPanel, [{id, ?SASH_BOTTOM_LEFT}, {style, ?wxSW_3D}]),
    BottomLeftPanel = wxPanel:new(BottomLeftSash, []),
    BottomLeftSizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(BottomLeftPanel, BottomLeftSizer),

    Text2 = wxStaticText:new(BottomLeftPanel, ?wxID_ANY, "Subtype or element list", []),
    wxSizer:add(BottomLeftSizer, Text2, []),

    LC2 = wxListCtrl:new(BottomLeftPanel, [{style, ?wxLC_REPORT bor ?wxLC_SINGLE_SEL}]),
    wxSizer:add(BottomLeftSizer, LC2, SizerOpts),
    wxListCtrl:insertColumn(LC2, 0, "Type", []),
    wxListCtrl:insertColumn(LC2, 1, "Count", []),
    wxListCtrl:insertItem(LC2, 0, ""),
    wxListCtrl:setItem(LC2, 0, 0, "T"),
    wxListCtrl:setItem(LC2, 0, 1, "123,456"),
    wxListCtrl:connect(LC2, command_list_item_selected, []),

    wxSashWindow:setSashVisible(TopLeftSash, ?wxSASH_BOTTOM, true),
    wxPanel:connect(LeftPanel, sash_dragged),
    wxPanel:connect(LeftPanel, size),

    wxSizer:add(LeftSizer, TopLeftSash, SizerOpts),
    wxSizer:add(LeftSizer, BottomLeftSash, SizerOpts),

    %% Right panel
    RightPanel = wxPanel:new(Splitter, [{winid, ?PANEL_RIGHT}]),
    RightSizer = wxBoxSizer:new(?wxVERTICAL),
    wxPanel:setSizer(RightPanel, RightSizer),

    TopRightSash = wxSashWindow:new(RightPanel, [{id, ?SASH_TOP_RIGHT}, {style, ?wxSW_3D}]),
    Win3 = wxPanel:new(TopRightSash, []),
    wxStaticText:new(Win3, ?wxID_ANY, "Statistics visualization", []),

    BottomRightSash = wxSashWindow:new(RightPanel, [{id, ?SASH_BOTTOM_RIGHT}, {style, ?wxSW_3D}]),
    Win4 = wxPanel:new(BottomRightSash, []),
    wxStaticText:new(Win4, ?wxID_ANY, "Private data visualization", []),

    wxSashWindow:setSashVisible(TopRightSash, ?wxSASH_BOTTOM, true),
    wxPanel:connect(RightPanel, sash_dragged),
    wxPanel:connect(RightPanel, size),

    wxSizer:add(RightSizer, TopRightSash, SizerOpts),
    wxSizer:add(RightSizer, BottomRightSash, SizerOpts),

    %% Main vertical splitter
    wxSplitterWindow:splitVertically(Splitter, LeftPanel, RightPanel),
    wxSplitterWindow:setSashGravity(Splitter, 0.5),
    wxSplitterWindow:setMinimumPaneSize(Splitter, 1),
    wxSizer:add(Sizer, Splitter, SizerOpts),

    wxFrame:show(Frame),
    {Frame, #state{config=Config, frame=Frame, panel=Panel,
                   panel_left = LeftPanel, panel_left_div = 0.5,
                   panel_right = RightPanel, panel_right_div = 0.5,
                   sash_top_left = TopLeftSash, sash_bottom_left = BottomLeftSash,
                   sash_top_right = TopRightSash, sash_bottom_right = BottomRightSash}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Async Events are handled in handle_event as in handle_info
handle_event(#wx{obj = _ListCtrl,
                 event = #wxList{itemIndex = Item}},
             State = #state{}) ->
    io:format("Item ~p selected.~n", [Item]),
    {noreply,State};

handle_event(#wx{id=?SASH_TOP_LEFT, event = #wxSash{dragRect = DragRect}},
             State = #state{panel_left = Panel,
                            sash_top_left = TopSash,
                            sash_bottom_left = BottomSash}) ->
    PanelDiv = handle_sash_event(Panel, TopSash, BottomSash, DragRect),
    {noreply, State#state{panel_left_div=PanelDiv}};
handle_event(#wx{id=?SASH_TOP_RIGHT, event = #wxSash{dragRect = DragRect}},
             State = #state{panel_right = Panel,
                            sash_top_right = TopSash,
                            sash_bottom_right = BottomSash}) ->
    PanelDiv = handle_sash_event(Panel, TopSash, BottomSash, DragRect),
    {noreply, State#state{panel_right_div=PanelDiv}};
handle_event(#wx{id=?PANEL_LEFT, event = #wxSize{size = Size}},
             State = #state{panel_left = Panel,
                            panel_left_div = PanelDiv,
                            sash_top_left = TopSash,
                            sash_bottom_left = BottomSash}) ->
    handle_size_event(Panel, TopSash, BottomSash, PanelDiv, Size),
    {noreply, State};
handle_event(#wx{id=?PANEL_RIGHT, event = #wxSize{size = Size}},
             State = #state{panel_right = Panel,
                            panel_right_div = PanelDiv,
                            sash_top_right = TopSash,
                            sash_bottom_right = BottomSash}) ->
    handle_size_event(Panel, TopSash, BottomSash, PanelDiv, Size),
    {noreply, State};

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

handle_sash_event(Panel, TopSash, BottomSash, {_X, Y, _W, H}) ->
    {PanelW, PanelH} = wxPanel:getSize(Panel),
    BottomH = PanelH - H,
    {PanelX, _} = wxSashWindow:getPosition(BottomSash),
    wxSashWindow:setMinSize(BottomSash, {PanelW, BottomH}),
    wxSashWindow:setMinSize(TopSash, {PanelW, H}),
    wxSashWindow:setSize(BottomSash, {PanelX, Y, PanelW, BottomH}),
    wxSashWindow:setSize(TopSash, {PanelW, H}),
    wxPanel:refresh(Panel),
    H/PanelH.

handle_size_event(Panel, TopSash, BottomSash, PanelDiv, {W, H}) ->
    TopH = round(H * PanelDiv),
    BottomH = H - TopH,
    wxSashWindow:setMinSize(TopSash, {W, TopH}),
    wxSashWindow:setMinSize(BottomSash, {W, BottomH}),
    wxSashWindow:setSize(TopSash, {W, TopH}),
    wxSashWindow:setSize(BottomSash, {0, TopH, W, BottomH}),
    wxPanel:refresh(Panel).
