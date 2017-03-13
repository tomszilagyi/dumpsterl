-module(ds_pvattrs).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ new/1
        , add/2
        , join/2

        , get_count/1
        , get_timespan/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(pvattrs,    % Per-Value Attributes
        { count     % integer
        , timespan  % {{TsMin, Key1}, {TsMax, Key2}}
        }).

%% TODO maybe require a normalized form of Attrs here
%% (pre-process them in ds_drv) so we can do simple pattern matching
%% instead of proplists:get_value/2 each time we want ts and key
new(#pvattrs{} = PVS) -> PVS;
new(Attrs) ->
    Ts = proplists:get_value(ts, Attrs),
    Key = proplists:get_value(key, Attrs),
    #pvattrs{count = 1, timespan = {{Ts, Key}, {Ts, Key}}}.

add(#pvattrs{count=Count0, timespan=TSp0},
    #pvattrs{count=Count1, timespan=TSp1}) ->
    #pvattrs{count = Count0+Count1, timespan = join_timespan(TSp0, TSp1)};
add(Attrs, #pvattrs{count=Count, timespan=TSp}) ->
    Ts = proplists:get_value(ts, Attrs),
    Key = proplists:get_value(key, Attrs),
    #pvattrs{count = Count+1, timespan = join_timespan({Ts, Key}, TSp)}.

%% Since we can discriminate Attrs from #pvattrs{}, we use add/2 for joins too.
%% This is useful in ds_sampler:join/2. Provide join/2 here as a simple alias.
join(PVS0, PVS1) -> add(PVS0, PVS1).

join_timespan({TSpMin0, TSpMax0}, {TSpMin1, TSpMax1}) ->
    {min(TSpMin0, TSpMin1), max(TSpMax0, TSpMax1)}.


get_count(#pvattrs{count = Count}) -> Count.

get_timespan(#pvattrs{timespan = TSp}) -> TSp.


%% Tests
-ifdef(TEST).



-endif.
