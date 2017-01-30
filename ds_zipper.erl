%% Zipper tree for navigating a spec
%% inspired by http://ferd.ca/yet-another-article-on-zippers.html

-module(ds_zipper).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([from_tree/1, value/1, left/1, right/1, children/1, parent/1]).

-type zlist(A) :: {Left::list(A), Right::list(A)}.
-type znode()  :: zlist({term(), zlist(_)}). % znode is a zlist of nodes
-type thread() :: [znode()].
-type zntree() :: {thread(), znode()}.

%% Convert a spec tree to zipper format
from_tree({'T', Data, SubSpec}) ->
    {[], {[], ft([{'T', Data, SubSpec}])}}.

ft([]) -> [];
ft([{Class, Data, Children}|R]) ->
    [{Class, Data, {[], ft(Children)}}|ft(R)].

%% Extract the node's value from the current tree position
-spec value(zntree()) -> term().
value({_Thread, {_Left, [{Class, Data, _Children} | _Right]}}) ->
    {Class, Data}.

%% Moves to the left of the current level
-spec left(zntree()) -> zntree().
left({Thread, {[H|T], R}}) -> {Thread, {T, [H|R]}}.

%% Moves to the right of the current level
-spec right(zntree()) -> zntree().
right({Thread, {L, [H|T]}}) -> {Thread, {[H|L], T}}.

%% Goes down one level to the children of the current node.
-spec children(zntree()) -> zntree().
children({Thread, {L, [{Class, Data, Children}|R]}}) ->
    {[{L, [{Class, Data}|R]}|Thread], Children}.

%% Moves up to the direct parent level.
-spec parent(zntree()) -> zntree().
parent({[{L, [{Class, Data}|R]}|Thread], Children}) ->
    {Thread, {L, [{Class, Data, Children}|R]}}.
