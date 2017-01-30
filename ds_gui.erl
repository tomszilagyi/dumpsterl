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
          panel
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

    BottomLeftPanel = wxPanel:new(LeftSplitter, []),
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
    wxStaticText:new(TopRightPanel, ?wxID_ANY, "Statistics visualization", []),

    BottomRightPanel = wxPanel:new(RightSplitter, []),
    wxStaticText:new(BottomRightPanel, ?wxID_ANY, "Private data visualization", []),

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
    {Frame, #state{config=Config, frame=Frame, panel=Panel}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Async Events are handled in handle_event as in handle_info
handle_event(#wx{obj = _ListCtrl,
                 event = #wxList{itemIndex = Item}},
             State = #state{}) ->
    io:format("Item ~p selected.~n", [Item]),
    {noreply,State};

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

