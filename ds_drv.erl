%% Driver functions for dataspec
-module(ds_drv).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ spec_tab_field/3
        , spec_tab_field/4
        ]).

%% Go through an ets table to spec a field based on a limited number
%% of rows processed:
%%
%%   spec_tab_field(my_ets_table, #my_ets_record.my_field, 1000)
%%
%% Sending eg. 'inf' as Limit disables the limit (traverse whole table).

spec_tab_field(Tab, FieldNo, Limit) ->
    fold_tab_field(fun ds:add/2, ds:new(), Tab, FieldNo, Limit).

spec_tab_field(Tab, FieldNo, Limit, Opts) ->
    ds:setopts(Opts),
    fold_tab_field(fun ds:add/2, ds:new(), Tab, FieldNo, Limit).


fold_tab_field(Fun, Acc0, Tab, FieldNo, Limit) ->
    Progress = ds:getopt(progress),
    progress_init_output(Progress),
    fold_tab_field(Fun, Acc0, Tab, FieldNo, Limit, Progress, 0, ets:first(Tab)).

fold_tab_field(_Fun, Acc, _Tab, _FieldNo, 0, Progress, Count, _Key) ->
    progress_final_output(Progress, Count),
    Acc;
fold_tab_field(_Fun, Acc, _Tab, _FieldNo, _Limit, Progress, Count, '$end_of_table') ->
    progress_final_output(Progress, Count),
    Acc;
fold_tab_field(Fun, Acc, Tab, FieldNo, Limit, Progress0, Count, Key) ->
    Progress = progress_output(Progress0, Count+1),
    [Rec] = ets:lookup(Tab, Key),
    Field = element(FieldNo, Rec),
    fold_tab_field(Fun, Fun(Field, Acc), Tab, FieldNo,
                   counter_dec(Limit), Progress, Count+1,
                   ets:next(Tab, Key)).


%% output progress information if configured to do so
progress_output(false, _Count) -> false;
progress_output(1, Count) ->
    Progress = ds:getopt(progress),
    case Count div Progress rem 50 of
        0 -> io:format("~B", [Count]);
        _ -> io:put_chars(".")
    end,
    Progress;
progress_output(Progress, _Count) -> counter_dec(Progress).

progress_init_output(false)    -> false;
progress_init_output(Progress) -> io:format("progress (every ~B): ", [Progress]).

progress_final_output(false, _Count) -> false;
progress_final_output(_Prog, Count)  -> io:format("~nprocessed: ~B~n~n", [Count]).

%% decrement counters that might be disabled by being set to an atom
counter_dec(N) when is_integer(N) -> N-1;
counter_dec(A)                    -> A.
