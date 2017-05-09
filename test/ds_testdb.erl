%% -*- coding: utf-8 -*-
-module(ds_testdb).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ generate/1
        , generate/2
        , generate/3
        , generate_ct/1
        , spec/1
        , spec/2
        , spec/3
        , spec_ct/1
        , gui/1
        , gui/2
        ]).

%% This module supports manual and automated testing with a synthetically
%% generated database.
%%
%% Manual testing:
%% - generate a test database with generate/1-3;
%% - spec it via spec/1-3;
%% - launch the gui for browsing the spec via gui/1-2.
%%
%% Examples (one minimal, one parameterized):
%% generate(disk_log).          generate(disk_log, mytable, 10000).
%% spec(disk_log).              spec(disk_log, mytable, [key]).
%% gui(disk_log).               gui(disk_log, mytable).
%%
%% The functions *_ct are entry points for Common Test suites.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("config.hrl").
-include("random.hrl").

-define(DEBUG, true).
-include("debug.hrl").

-define(DATA_DIR, filename:join(test, data)).
-define(MNESIA_DIR, db).
-define(RECORD, widget).
-define(NUM_RECORDS, 675000).

-record(?RECORD,
        { id
        , create_date
        , type
        , status
        , flags
        , priority
        , responsible
        , ocr
        , pno
        , data
        , props
        , txns
        , history
        , ftp
        , model_params
        }).

-define(ATTRS, [{key, #?RECORD.id}, {ts, #?RECORD.create_date}]).


generate(Type) -> generate(Type, default_name(Type)).

generate(Type, Tab) -> generate(Type, Tab, ?NUM_RECORDS).

generate(Type, Tab, NRecs) ->
    generate_ct([{type, Type},
                 {tab, Tab},
                 {num_records, NRecs},
                 {priv_dir, ?DATA_DIR}]).

spec(Type) -> spec(Type, default_name(Type)).

spec(Type, Tab) -> spec(Type, Tab, [key, ts]).

spec(Type, Tab, Attrs) ->
    spec_ct([{type, Type},
             {tab, Tab},
             {attributes, Attrs},
             {priv_dir, ?DATA_DIR}]).

spec_ct(Config) ->
    Type = proplists:get_value(type, Config),
    Tab = proplists:get_value(tab, Config, default_name(Type)),
    Attrs = proplists:get_value(attributes, Config, [key, ts]),
    PrivDir = proplists:get_value(priv_dir, Config),
    AttrSpecs = lists:filter(fun({AttrName,_AttrSpec}) ->
                                     lists:member(AttrName, Attrs)
                             end, ?ATTRS),
    ds_probe:spec(Type, Tab,
                  {0, AttrSpecs},
                  [ {limit, infinity}
                  , {dump, dumpfile_name(PrivDir, Type, Tab)}
                  , {progress, 0.5}
                  , {rec_attrs, force}
                  , {mnesia_dir, filename:join(PrivDir, ?MNESIA_DIR)}
                  ]).

gui(Type) -> gui(Type, default_name(Type)).

gui(Type, Tab) -> ds_gui:start_link(dumpfile_name(?DATA_DIR, Type, Tab)).


default_name(mnesia) ->
    list_to_atom(lists:concat([?RECORD, "_mnesia"]));
default_name(ets) ->
    list_to_atom(lists:concat([?RECORD, "_ets"]));
default_name(dets) ->
    filename:join(?DATA_DIR, lists:concat([?RECORD, ".dets"]));
default_name(disk_log) ->
    filename:join(?DATA_DIR, lists:concat([?RECORD, ".disk_log"])).

dumpfile_name(PrivDir, mnesia, Tab) -> filename:join(PrivDir, dumpfile_name(Tab));
dumpfile_name(PrivDir, ets, Tab) -> filename:join(PrivDir, dumpfile_name(Tab));
dumpfile_name(_PrivDir,_Type, Tab) -> dumpfile_name(Tab).

dumpfile_name(Tab) -> lists:concat([Tab, ".spec.bin"]).

generate_ct(Config) ->
    Type = proplists:get_value(type, Config),
    do_generate_ct(Type, Config).

do_generate_ct(mnesia=Type, Config) ->
    Tab = proplists:get_value(tab, Config, default_name(Type)),
    NRecs = proplists:get_value(num_records, Config, ?NUM_RECORDS),
    setup_mnesia(Config),
    Exists = lists:member(Tab, mnesia:system_info(tables)),
    if Exists ->
            ?debug("table exists, deleting it"),
            {atomic, ok} = mnesia:delete_table(Tab);
       true ->
            ok
    end,
    {atomic, ok} =
        mnesia:create_table(Tab,
                            [ {record_name, ?RECORD}
                            , {attributes, record_info(fields, ?RECORD)}
                            , {disc_copies, [node()]}
                            ]),
    populate(Type, Tab, NRecs),
    Tab;
do_generate_ct(ets=Type, Config) ->
    Tab = proplists:get_value(tab, Config, default_name(Type)),
    NRecs = proplists:get_value(num_records, Config, ?NUM_RECORDS),
    Exists = lists:member(Tab, ets:all()),
    if Exists ->
            ?debug("table exists, deleting it"),
            ets:delete(Tab);
       true   -> ok
    end,
    Tid = ets:new(Tab, [public, named_table, {keypos, #?RECORD.id}]),
    populate(Type, Tid, NRecs),
    Tid;
do_generate_ct(dets=Type, Config) ->
    DetsFile = proplists:get_value(tab, Config, default_name(Type)),
    NRecs = proplists:get_value(num_records, Config, ?NUM_RECORDS),
    ok = filelib:ensure_dir(DetsFile),
    Exists = filelib:is_regular(DetsFile),
    if Exists ->
            ?debug("table exists, deleting it"),
            ok = file:delete(DetsFile);
       true -> ok
    end,
    {ok, Dets} = dets:open_file(?RECORD, [ {file, DetsFile}
                                         , {keypos, #?RECORD.id}
                                         , {type, set}
                                         ]),
    populate(Type, Dets, NRecs),
    ok = dets:close(Dets),
    DetsFile;
do_generate_ct(disk_log=Type, Config) ->
    LogFile = proplists:get_value(tab, Config, default_name(Type)),
    NRecs = proplists:get_value(num_records, Config, ?NUM_RECORDS),
    ok = filelib:ensure_dir(LogFile),
    Exists = filelib:is_regular(LogFile),
    if Exists ->
            ?debug("table exists, deleting it"),
            file:delete(LogFile);
       true -> ok
    end,
    {ok, Log} = disk_log:open([ {name, ?RECORD}
                              , {file, LogFile}
                              , {mode, read_write}
                              ]),
    populate(Type, Log, NRecs),
    ok = disk_log:close(Log),
    LogFile.

populate(Type, Tid, NRecs) ->
    ?debug("populating ~p: ~p with ~B records", [Type, Tid, NRecs]),
    ?RNDINIT_DET,
    populate(Type, Tid, NRecs, generator_init()).

populate(_Type, _Tid, 0,_State) -> ok;
populate(Type, Tid, Count, State0) ->
    State = add_rec(Type, Tid, State0),
    populate(Type, Tid, Count-1, State).

add_rec(mnesia, Tid, State0) ->
    {Rec, State} = generate_rec(State0),
    ok = mnesia:dirty_write(Tid, Rec),
    State;
add_rec(ets, Tid, State0) ->
    {Rec, State} = generate_rec(State0),
    true = ets:insert(Tid, Rec),
    State;
add_rec(dets, Dets, State0) ->
    {Rec, State} = generate_rec(State0),
    ok = dets:insert(Dets, Rec),
    State;
add_rec(disk_log, Log, State0) ->
    {Rec, State} = generate_rec(State0),
    ok = disk_log:log(Log, Rec),
    State.

%% This function will perform the minimal set of actions required
%% to get mnesia into the state we want: disk-based schema set up
%% in our designated database directory.
%%
%% Mnesia may be
%% - not running
%% - running with a database directory other than what we want
%% - running with our database directory, but without a disk-based schema
setup_mnesia(Config) ->
    setup_mnesia(Config, directory),
    setup_mnesia(Config, running),
    setup_mnesia(Config, schema),
    mnesia:wait_for_tables(mnesia:system_info(tables), infinity).

setup_mnesia(Config, directory) ->
    PrivDir = proplists:get_value(priv_dir, Config),
    AbsMnesiaDir = filename:absname(filename:join(PrivDir, ?MNESIA_DIR)),
    ok = filelib:ensure_dir(AbsMnesiaDir),
    case mnesia:system_info(directory) of
        AbsMnesiaDir ->
            ?debug("mnesia db dir is already ~s.", [mnesia:system_info(directory)]);
        _ ->
            setup_mnesia(Config, stopped),
            application:set_env(mnesia, dir, AbsMnesiaDir),
            ?debug("mnesia db dir set to ~s.", [mnesia:system_info(directory)])
    end;
setup_mnesia(_Config, stopped) ->
    case mnesia:system_info(is_running) of
        yes ->
            ?debug("mnesia is running, stopping it."),
            mnesia:stop();
        no ->
            ?debug("mnesia is already stopped.")
    end;
setup_mnesia(_Config, running) ->
    case mnesia:system_info(is_running) of
        no ->
            ?debug("mnesia is not running, starting it."),
            mnesia:start();
        yes ->
            ?debug("mnesia is already started.")
    end;
setup_mnesia(Config, schema) ->
    DiscNodes = [node()],
    case mnesia:table_info(schema, disc_copies) of
        [] ->
            ?debug("mnesia schema not on disc, reinitializing."),
            setup_mnesia(Config, stopped),
            mnesia:delete_schema(DiscNodes),
            ok = mnesia:create_schema(DiscNodes),
            setup_mnesia(Config, running);
        DiscNodes ->
            ?debug("mnesia schema is already on disc.")
    end.


%% Test database model.

%% calendar:datetime_to_gregorian_seconds({{2010, 1, 1}, { 0, 0, 0}}).
-define(START_DATE, 63429523200).
%% calendar:datetime_to_gregorian_seconds({{2011, 1, 1}, { 0, 0, 0}}).
-define(STEP1_DATE, 63461059200).
%% calendar:datetime_to_gregorian_seconds({{2011, 3, 9}, {10,52, 0}}).
-define(STEP2_DATE, 63466887120).
%% calendar:datetime_to_gregorian_seconds({{2014, 9,12}, {12, 0, 0}}).
-define(STEP3_DATE, 63577742400).
%% calendar:datetime_to_gregorian_seconds({{2014, 9,13}, { 0, 0, 0}}).
-define(STEP4_DATE, 63577785600).

-record(state,
        { count
        , date
        }).

generator_init() ->
    TestData = read_testdata(),
    [put(Key, Value) || {Key, Value} <- TestData],
    #state{count = 0, date = ?START_DATE}.

read_testdata() ->
    Dir = code:priv_dir(dumpsterl),
    {ok, TestData} = file:consult(filename:join(Dir, "testdata.eterm")),
    TestData.

generate_rec(#state{count=Count0, date=Date0} = State) ->
    Count = Count0 + 1,
    Date = Date0 + 100 + ?RNDMOD:uniform(500),
    Type = type(Count, Date),
    Rec = #?RECORD{ id = 10 * Count
                  , create_date = Date
                  , type = type(Count, Date)
                  , status = status(Date)
                  , flags = flags(Count, Date)
                  , priority = priority(Count)
                  , responsible = responsible()
                  , ocr = ocr()
                  , pno = pno(Date)
                  , data = data(Type, Count, Date)
                  , props = plist_with_names()
                  , txns = []
                  , history = history()
                  , ftp = ftp(Date)
                  , model_params = random_improper()
                  },
    {Rec, State#state{count = Count, date=Date}}.

type(_Count, Date) when Date > ?STEP4_DATE ->
    ?RNDMOD:uniform(18);
type(Count, Date) when Date > ?STEP3_DATE ->
    Count rem 16 + 1;
type(_Count, Date) when Date > ?STEP2_DATE ->
    T = ?RNDMOD:uniform(16),
    if T > 12 -> 12 + (T - 12) div 2;
       true -> T
    end;
type(Count, Date) when Date > ?STEP1_DATE ->
    Count rem 8 + Count rem 2;
type(Count, Date) ->
    Date rem 4 + Count rem 2.

status(Date) when Date < ?STEP4_DATE ->
    choose_cdf([ {0.95, closed}
               , {1.00, expired}
               ]);
status(_Date) ->
    choose_cdf([ {0.65, closed}
               , {0.83, open}
               , {0.90, submitted}
               , {0.95, needs_review}
               , {0.98, waiting_for_customer}
               , {1.00, waiting_for_supplier}
               ]).

flags(Count, Date) when Date < ?STEP1_DATE ->
    Count rem 16;
flags(_Count, Date) when Date < ?STEP2_DATE ->
    16#7ffff;
flags(_Count, Date) when Date < ?STEP3_DATE ->
    gen_flags([0.1, 0.2, 0.3, 0.05, 0.05, 0.02]);
flags(_Count, Date) when Date < ?STEP4_DATE ->
    gen_flags([0.01, 0.05, 0.05, 0.02, 0.3, 0.4, 0.5]);
flags(_Count,_Date) ->
    choose_cdf([ {0.9998, gen_flags([0.0, 0.0, 0.0, 0.0, 0.5, 0.2, 0.1, 0.5,
                                     0.2, 0.4, 0.8])}
               , {0.9999, undefined}
               , {1.0000, []}
               ]).

%% Generate a value as a sum of bit fields where each bit is set with the
%% corresponding probability in BitProbs. E.g. BitProbs = [0.5, 0.1, 0.9]
%% generates a 3-bit flag where bit 0 is set with P=0.5, bit 1 is set with
%% P=0.1, bit 2 is set with P=0.9.
gen_flags(BitProbs) ->
    Seq = lists:seq(0, length(BitProbs) - 1),
    lists:sum([(1 bsl N) * choose_cdf([{P1, 1}, {1.00, 0}])
               || {P1, N} <- lists:zip(BitProbs, Seq)]).

priority(Count) when Count > 50000 ->
    Count rem 3 + 1;
priority(Count) when Count > 35000 ->
    Count rem 2;
priority(_Count) ->
    0.

responsible() ->
    Name = choose_random(get(names)),
    Surname = choose_random(get(surnames)),
    [deaccentize(C) || C <- string:to_lower(Name ++ "." ++ Surname)].

deaccentize($å) -> $a;
deaccentize($ä) -> $a;
deaccentize($ö) -> $o;
deaccentize($Å) -> $A;
deaccentize($Ä) -> $A;
deaccentize($Ö) -> $O;
deaccentize(C) -> C.

ocr() ->
    choose_cdf([ {0.50, random_digits(10)}
               , {0.70, random_digits(12)}
               , {0.85, random_digits(15)}
               , {0.96, random_digits(18)}
               , {0.975, integer_to_list(random_digits(10))}
               , {0.985, integer_to_list(random_digits(12))}
               , {0.996, integer_to_list(random_digits(15))}
               , {0.9999, integer_to_list(random_digits(18))}
               , {1.0000, []}
               ]).

pno(Date) when Date > ?STEP4_DATE ->
    choose_cdf([ {0.99, {random_digits(2), random_pno()}}
               , {0.99995, random_digits(40)}
               , {1.00000, "REFXXX " ++ integer_to_list(random_digits(40))}
               ]);
pno(_Date) ->
    choose_cdf([ {0.99999, {random_digits(1), random_pno()}}
               , {1.00000, choose_random([name, address, email,
                                          19, 1900, "system.default"])}
               ]).

data(Type,_Count,_Date) when Type > 15 ->
    {random_digits(3 + ?RNDMOD:uniform(3)),
     ?RNDMOD:uniform(16#ffffff) - 16#800000};
data(Type,_Count,_Date) when Type =:= 2 ->
    gen_map();
data(Type,_Count,_Date) when Type >= 7, Type =< 9 ->
    random_list_of(fun random_data_tuple/1, 20);
data(_Type,_Count,_Date) ->
    choose_random([true, false, undefined
                   | [Error || {Error,_No,_Descr} <- get(errno)]]).

history() -> random_list_of(fun history_item/1, 10).

history_item(_Seq) ->
    {?STEP4_DATE - ?RNDMOD:uniform(86400*365*10),
     random_list_of(fun hist_op/1, 5)}.

hist_op(Seq) ->
    ErrorDesc =
        choose_random([{No, Descr} || {_Error, No, Descr} <- get(errno)]),
    {Seq, ?RNDMOD:uniform(32) - 1, [],
     choose_cdf([ {0.9999, ErrorDesc}
                , {1.0000, term_to_binary(ErrorDesc)}
                ])}.

-record(ftp,
        { hostname
        , port
        , username
        , email
        , password
        , secret
        , last_login
        , conn
        }).

-record(connection, { ref, id, pid }).

ftp(Date) ->
    RandomDomainF = fun(_Seq) -> atom_to_list(choose_random(get(domains))) end,
    Domain = lists:concat([ string:join(random_list_of(RandomDomainF, 3), "-")
                          , "."
                          , choose_random(get(tlds))
                          ]),
    Hostname = lists:concat([choose_random(get(subdomains)), ".", Domain]),
    Username = choose_cdf([ {0.99, responsible()}
                          , {0.995, "sys"}
                          , {0.9999, "admin"}
                          , {1.0000, "username"}
                          ]),
    #ftp{ hostname = Hostname
        , port = choose_cdf([ {0.99, 21}
                            , {0.999, 8021}
                            , {1.000, 1024 + ?RNDMOD:uniform(64541)}
                            ])
        , username = Username
        , email = lists:concat([Username, "@", Domain])
        , password =
              choose_cdf([ {0.99, random(alpha)}
                         , {0.999, "abc123"}
                         , {1.000, "passw0rd"}
                         ])
        , secret =
              choose_cdf([ {0.9999, random_binary(16)}
                         , {0.99995, <<>>}
                         , {1.00000, []}
                         ])
        , last_login =
              choose_cdf([ {0.9999, Date - ?RNDMOD:uniform(86400*365*10)}
                         , {1.0000, 0}
                         ])
        , conn = #connection{ ref = erlang:make_ref()
                            , id = choose_random(erlang:ports())
                            , pid = self()}
        }.

-ifdef(CONFIG_MAPS).
gen_map() -> maps:from_list(gen_proplist()).
-else.
gen_map() -> gen_proplist().
-endif.

gen_proplist() ->
    choose_cdf([ {0.99, plist_with_atoms()}
               , {0.995, plist_with_numbers()}
               , {1.000, plist_with_names()}]).

plist_with_atoms() -> random_list_of(fun kv_atom/1, 10).

plist_with_numbers() -> random_list_of(fun kv_int/1, 5).

plist_with_names() -> [{surname, choose_random(get(surnames))},
                       {given_name, choose_random(get(names))}].

kv_atom(_Seq) ->
    {choose_random([Error || {Error,_No,_Descr} <- get(errno)]),
     choose_random([random(alpha), random(int), undefined])}.

kv_int(_Seq) ->
    {31 + ?RNDMOD:uniform(32), choose_random([random_id(), random(alpha)])}.

random_pno() ->
    Y = 1917 + ?RNDMOD:uniform(100),
    M = ?RNDMOD:uniform(12),
    D = ?RNDMOD:uniform(31),
    S = random_digits(4),
    {Y*10000 + M*100 + D, S}.

random_data_tuple(_Seq) ->
    {choose_cdf([ {0.70, integer_to_list(random_digits(12))}
                , {0.90, integer_to_list(random_digits(14))}
                , {0.999, integer_to_list(random_digits(16))}
                , {1.000, random_id()}]),
     ?RNDMOD:uniform(3)}.

random_id() ->
    lists:concat([random_digits(8), "-",
                  random_digits(4), "-",
                  random_digits(4), "-",
                  random_digits(2)]).

random_improper() ->
    random_list_of(fun random_params/1, 5) ++
        choose_cdf([ {0.4, fun random_improper/0}
                   , {0.6, fun erlang:term_to_binary/1}
                   , {0.7, fun(X) -> 2*X end}
                   , {0.85, fun(X, Y) -> 2*X + 3*Y end}
                   , {0.999, fun(X, Y, Z) -> 2*X + 3*Y + 4*Z end}
                   , {1.0, fun(X, Y, Z, W) -> 2*X + 3*Y + 4*Z + 5*W end}
                   ]).

random_params(_Seq) ->
    { random(float)
    , choose_cdf([ {0.9999, random(int)}
                 , {0.99995, undefined}
                 , {1.00000, []}
                 ])
    , random(alpha)
    }.

%% Create a list of random length, where each item is a result of
%% evaluating RandItemFun, a fun with arity 1, whose argument is the
%% current item's position in the list.
%% LenP is a parameter to generate the random length.
random_list_of(RandItemFun, LenP) ->
    Length = 1 + round(LenP * random_exp(LenP)),
    [RandItemFun(Seq) || Seq <- lists:seq(1, Length)].

random_binary(Size) ->
    list_to_binary([?RNDMOD:uniform(256) - 1 || _C <- lists:seq(1, Size)]).

%% choose one of the list members as random, with equal possibility
choose_random(Choices) -> lists:nth(?RNDMOD:uniform(length(Choices)), Choices).

%% Choose from a list of choices paired with their cumulative probability,
%% Choices = [{Pcum, Choice}]. Return Choice.
choose_cdf(Choices) -> choose_cdf(Choices, ?RNDMOD:uniform()).

choose_cdf([{Pcum, Choice}|_Rest], P) when P < Pcum -> Choice;
choose_cdf([_|Rest], P) -> choose_cdf(Rest, P).

%% generate a random positive integer with N decimal digits
random_digits(N) -> random_digits(?RNDMOD:uniform(9), N-1).

random_digits(Acc, 0) -> Acc;
random_digits(Acc, N) ->
    random_digits(10 * Acc + ?RNDMOD:uniform(10) - 1, N-1).

%% Generate an exponentially distributed random value.
%% Lambda is the rate parameter of the exponential distribution.
random_exp(Lambda) ->
    math:log(1.0 - ?RNDMOD:uniform()) / -Lambda.

random(int) -> ?RNDMOD:uniform(1 bsl 30) + 16#10ffff;
random(float) -> ?RNDMOD:uniform();
random(alpha) ->
    Len = 1 + round(40 * random_exp(10)),
    [$a + ?RNDMOD:uniform(25) || _N <- lists:seq(1, Len)].
