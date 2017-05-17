%% -*- coding: utf-8 -*-

%% @doc The public user and API interface module of dumpsterl.
%%
%% All usage of dumpsterl, both programmatic and user-initiated,
%% should be done via this module.
%%
%% This module exports three API-s, from highest to lowest level:
%% <ul>
%% <li><b>User API</b>: launch interactive user interface components
%%     (the Dumpsterl shell and gui).</li>
%% <li><b>Probe API</b>: run the probe and obtain a spec.
%%     These functions are invoked by the dumpsterl shell (see User
%%     API) but might be also used manually from the Erlang shell, or
%%     called directly by third-party code.</li>
%% <li><b>Spec API</b>: low level spec manipulations, or adding terms
%%     to a spec "by hand".</li>
%% </ul>
%% Each function is marked with the API it belongs to.

-module(ds).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ %% user interface start functions
          shell/0
        , gui/0
        , gui/1

          %% probe functions
        , spec/3
        , spec/4
        , probe/3
        , probe/4

          %% core spec functions
        , new/0
        , add/2
        , add/3
        , join/2
        , postproc/1
        , spec_to_form/1
        , pretty_print/1
        ]).

-export_type([ attr_spec/0
             , field_spec/0
             , probe_spec/0
             , spec/0 ]).

-opaque spec() :: ds_spec:spec().
%% The spec data structure created by the probe and consumed by the GUI.

-type field_spec() :: []
                    | non_neg_integer()
                    | fun((Record :: term()) -> Field :: term())
                    | [field_spec()].
%% The field spec is used to select the data that is to be taken and
%% spec'd from the raw terms fed to the probe. Normally, the raw term
%% is a `Record' from a database table, and the field spec selects a
%% certain `Field'. However, more elaborate scemes are also possible:
%%
%% <ul>
%% <li>`[]' or `0': select whole record (or term)</li>
%% <li>`N' where `N' > 0: select the field `N'.</li>
%% <li>`fun()': User-defined selection. The fun takes the record as its
%%     single argument and returns the selected value.</li>
%% <li>List of accessors: multiple accessors may be chained and supplied
%%     in a list. The accessors are applied from left to right, always
%%     taking the result of the previous one as their input. E.g.,
%%     `[3,2]' is interpreted as: "Take the third field from the record
%%     read from the table, then take the second field of the result,
%%     and spec that."</li>
%% </ul>
%%
%% Any runtime error within the accessor will cause the record to be
%% skipped, so a `fun()' accessor can also act as a filter via deliberate
%% `throw()'-ing. Even if a record is skipped, it still counts toward the
%% probe limit and shows up in the output count.

-type attr_spec() :: {AttrName :: atom(), field_spec()}.
%% Specify attributes to be attached to each spec'd term. `AttrName'
%% may be arbitrary, but the following receive special attention from
%% the GUI:
%%
%% <ul>
%% <li>`key': The primary key from the raw record. This is useful to
%%     be able to pull the complete, original record from the table in
%%     case you find an oddball value while browsing the spec in the
%%     GUI.</li>
%% <li>`ts': Timestamp from the record (creation or modification time).
%%     The only restriction the probe places on this is that its Erlang
%%     term ordering must correspond to temporal order (later
%%     timestamps must compare larger than earlier ones) in order to be
%%     useful.</li>
%% </ul>

-type probe_spec() :: field_spec() | {field_spec(), [attr_spec()]}.
%% The probe specification tells the probe what to spec from the raw
%% records read, and what attributes to associate with it. The
%% attributes are optional, and reference the whole, raw record read
%% from the table, even if the field spec selects a part of that.
%% Hence, it is possible to have parts of the original record stored
%% as attributes that are <b>not</b> part of the spec'd data.

-type table_type() :: mnesia | ets | dets | disk_log.
%% The type of table to read. This selects the table iterator functions
%% for the probe.

-type table_id() :: atom() | ets:tid() | dets:tab_name() | file:filename().
%% The table to read. Its accepted format depends on the `type' setting:
%% <ul>
%% <li> mnesia or ets table name (atom);</li>
%% <li> ets table id (integer);</li>
%% <li> dets table name (any term) for an already opened dets table;</li>
%% <li> filename of a dets table file or disk_log file.</li>
%% </ul>

-type options() :: [proplists:property()].
%% Options to influence dumpsterl's behaviour.
%%
%% The easiest way to get a full reference is to start the dumpsterl
%% shell via `ds:shell()' and then entering `help all'.

%% User interface functions:
%% These are the highest level user interfaces of dumpsterl.

%% @doc <b>[User API]</b> Start the interactive probe shell.
%% The function returns only when the user quits the shell.
-spec shell() -> ok.
shell() -> ds_shell:start().

%% @doc <b>[User API]</b> Start the GUI on the default dump file.
%% This is "ds.bin" in the current directory.
-spec gui() -> wx:wx_object() | {error, term()}.
gui() -> ds_gui:start().

%% @doc <b>[User API]</b> Start the GUI on the supplied Spec.
%% Spec might be a spec data structure or a dump filename.
-spec gui(Spec :: spec() | file:filename()) ->
                 wx:wx_object() | {error, term()}.
gui(Spec) -> ds_gui:start(Spec).


%% Probe API:
%% These functions are invoked by the shell (see above) but might be
%% also used manually, or called directly by third-party code.

%% @doc <b>[Probe API]</b> Run the probe to create a spec, using
%% default options. The function blocks the caller while the probe is
%% running. The result spec is returned as a return value (but might
%% also be dumped to file according to the options; the default is to
%% dump to "ds.bin").
-spec spec(Type :: table_type(),
           Tab :: table_id(),
           ProbeSpec :: probe_spec()) -> spec().
spec(Type, Tab, ProbeSpec) -> ds_probe:spec(Type, Tab, ProbeSpec).

-spec spec(Type :: table_type(),
           Tab :: table_id(),
           ProbeSpec :: probe_spec(),
           Opts :: options()) -> spec().
%% @doc <b>[Probe API]</b> Like spec/3, but also specify options.
spec(Type, Tab, ProbeSpec, Opts) -> ds_probe:spec(Type, Tab, ProbeSpec, Opts).

%% @doc <b>[Probe API]</b> Launch the probe to run in the background, using
%% default options. The call returns (almost) immediately with a value that
%% can be used to stop the probe before completion:
%%
%% ``` {Pid, _} = ds:probe(...).
%%     ...
%%     Pid ! finish.'''
%%
%% The second value in the returned tuple is a monitor reference;
%% the probe will be monitored by the caller. This means that the
%% caller will receive a message when the probe completes:
%%
%% ``` {Pid, MonitorRef} = ds:probe(...),
%%     receive
%%         {'DOWN', MonitorRef, process, Pid, Info} ->
%%             io:format("probe done: ~p~n", [Info])
%%     end.'''
-spec probe(Type :: table_type(),
            Tab :: table_id(),
            ProbeSpec :: probe_spec()) -> {pid(), reference()}.
probe(Type, Tab, ProbeSpec) -> ds_probe:start(Type, Tab, ProbeSpec).

%% @doc  <b>[Probe API]</b> Like probe/3, but also specify options.
probe(Type, Tab, ProbeSpec, Opts) -> ds_probe:start(Type, Tab, ProbeSpec, Opts).


%% Spec API:
%% These functions are the lowest level API useful to create and manipulate
%% dumpsterl's spec trees.

%% @doc <b>[Spec API]</b> Create a new spec.
-spec new() -> spec().
new() -> ds_spec:new().

%% @doc <b>[Spec API]</b> Add a term (without attributes) to the spec.
%% @equiv add(Term, [], Spec)
-spec add(term(), spec()) -> spec().
add(Term, Spec) -> ds_spec:add({Term, []}, Spec).

%% @doc <b>[Spec API]</b> Add a term (with attributes) to the spec.
%% Due to the order of the arguments, this function can be directly
%% passed to lists:foldl/3. The attr specs in `Attrs' reference `Term'.
-spec add(term(), [attr_spec()], spec()) -> spec().
add(Term, Attrs, Spec) when is_list(Attrs) ->
    ds_spec:add({Term, Attrs}, Spec).

%% @doc <b>[Spec API]</b> Join two specs into one.
%% This is useful in case the set of terms to be spec'd was partitioned
%% into subsets that were independently spec'd. By joining these specs,
%% the spec for the whole set of terms can be obtained.
%%
%% NB. joining must be done on the "raw" spec tree, one that was
%% created only via new/0 and add/2,3. In such a tree, all the
%% abstract nodes starting from the root 'term' are always present.
%% See postproc/1 for a tree that does not qualify for this condition.
-spec join(spec(), spec()) -> spec().
join(Spec1, Spec2) -> ds_spec:join(Spec1, Spec2).

%% @doc <b>[Spec API]</b> Postprocess a spec. This is performed as a
%% pre-load step in the GUI, but could be useful in other scenarios.
%% It has a few separate steps:
%%
%% <ul>
%% <li>compact the tree by cutting empty abstract types;</li>
%% <li>propagating and joining metadata up from the leaf nodes;</li>
%% <li>sorting the children of unions by decreasing count.</li>
%% </ul>
%%
%% NB. The result of this function generally cannot be passed to the
%% add/2-3 and join/2 functions.
-spec postproc(spec()) -> spec().
postproc(Spec) -> ds_spec:postproc(Spec).

%% @doc <b>[Spec API]</b> Convert a spec to Erlang abstract from.  The
%% abstract form contains a type definition equivalent to the spec
%% tree. Its primary use is passing it to the Erlang pretty-printer
%% to obtain a nicely formatted spec type definition.
-spec spec_to_form(spec()) -> erl_parse:abstract_form().
spec_to_form(Spec) -> ds_types:spec_to_form(Spec).

%% @doc <b>[Spec API]</b> Pretty-print a spec.
%% Returns a string produced via converting the spec to the Erlang
%% abstract form of the type definition and pretty-printing that
%% with the Erlang pretty-printer.
%%
%% Naturally, this only contains a type definition and does not show
%% anything of the samples and statistics contained in the spec.
-spec pretty_print(spec()) -> string().
pretty_print(Spec) -> ds_types:pp_spec(Spec).
