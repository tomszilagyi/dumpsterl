%% -*- coding: utf-8 -*-
-module(ds_zipper).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% Zipper tree for navigating a spec
%% inspired by http://ferd.ca/yet-another-article-on-zippers.html

-export([ from_tree/1
        , to_tree/1, class/1, data/1
        , stack/1, stack/2, child_list/1, child_list/2
        , left/1, right/1, children/1, nth_child/2, parent/1, nth_parent/2]).

-type zlist(A) :: {Left::list(A), Right::list(A)}.
-type znode()  :: zlist({term(), zlist(_)}). % znode is a zlist of nodes
-type thread() :: [znode()].
-type zntree() :: {thread(), znode()}.

%% Constructor: convert a spec tree to zipper format
from_tree({Class, Data, SubSpec}) ->
    {[], {[], ft([{Class, Data, SubSpec}])}}.

ft([]) -> [];
ft([{Class, Data, Children}|R]) ->
    [{Class, Data, {[], ft(Children)}}|ft(R)].

%% Return the (sub)tree from the current position of the zipper
to_tree({_Thread, {_Left, [TreeNode|_Right]}}) -> tree(TreeNode).

tree({Class, Data, {ChL,ChR}}) ->
    {Class, Data, [tree(Ch) || Ch <- lists:reverse(ChL)++ChR]}.

%% Extract the node's values from the current tree position
class({_Thread, {_Left, [{Class,_Data, _ChZ} | _Right]}}) -> Class.
data({_Thread, {_Left, [{_Class, Data, _ChZ} | _Right]}}) -> Data.

%% Get the stack of classes up to and including the current one
stack(Zipper) ->
    stack(Zipper, fun(TreeNode, NthChild) -> {TreeNode, NthChild} end).

%% TransformFun: fun(TreeNode, NthChild) -> StackItem ok.
stack(Zipper, TransformFun) ->
    stack(Zipper, TransformFun, []).

stack({[], {Left, [TreeNode|_Right]}}, TransformFun, Acc) ->
    Nth = length(Left) + 1, %% We are the Nth child of our parent
    [TransformFun(tree(TreeNode), Nth)|Acc];
stack({_Thread, {Left, [TreeNode|_Right]}}=Zipper, TransformFun, Acc) ->
    Nth = length(Left) + 1, %% We are the Nth child of our parent
    stack(parent(Zipper), TransformFun, [TransformFun(tree(TreeNode), Nth)|Acc]).

%% Get the list of classes of children nodes below the current one
child_list(Zipper) ->
    child_list(Zipper, fun(TreeNode) -> TreeNode end).

%% TransformFun: fun(TreeNode) -> ListItem ok.
child_list({_Thread, {_L, [{_Class, _Data, {ChL,ChR}}|_R]}},
           TransformFun) ->
    [TransformFun(tree(TreeNode)) || TreeNode <- lists:reverse(ChL)++ChR].

%% Move to the left of the current level
-spec left(zntree()) -> zntree().
left({Thread, {[H|T], R}}) -> {Thread, {T, [H|R]}}.

nth_left(Tree, 0) -> Tree;
nth_left(Tree, N) -> nth_left(left(Tree), N-1).

%% Move to the right of the current level
-spec right(zntree()) -> zntree().
right({Thread, {L, [H|T]}}) -> {Thread, {[H|L], T}}.

nth_right(Tree, 0) -> Tree;
nth_right(Tree, N) -> nth_right(right(Tree), N-1).

%% Get current position index among current level nodes
pos({_Thread, {L, [_TreeNode|_R]}}) -> length(L).

%% Go down one level to the children of the current node
-spec children(zntree()) -> zntree().
children({Thread, {L, [{Class, Data, Children}|R]}}) ->
    {[{L, [{Class, Data}|R]}|Thread], Children}.

%% Go down one level and position on N-th node
nth_child(Tree0, N) ->
    Tree = children(Tree0),
    Pos = pos(Tree),
    if Pos < N -> nth_right(Tree, N-Pos);
       Pos > N -> nth_left(Tree, Pos-N);
       Pos =:= N -> Tree
    end.

%% Move up to the direct parent level.
-spec parent(zntree()) -> zntree().
parent({[{L, [{Class, Data}|R]}|Thread], Children}) ->
    {Thread, {L, [{Class, Data, Children}|R]}}.

%% Move up N levels.
nth_parent(Tree, 0) -> Tree;
nth_parent(Tree, N) -> nth_parent(parent(Tree), N-1).
