%% -*- coding: utf-8 -*-
-module(ds).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

%% @doc This is the public user & API interface module of dumpsterl.
%% All usage of dumpsterl, both programmatic and user-initiated,
%% should be done via this module.

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


%% @doc User interface functions
%%
%% These are the highest level user interfaces of dumpsterl.

shell() -> ds_shell:start().

gui() -> ds_gui:start().

gui(Spec) -> ds_gui:start(Spec).


%% @doc Probe API
%%
%% These functions are invoked by the shell (see above) but might be
%% also used manually, or called directly by third-party code.

spec(Type, Tab, ProbeSpec) -> ds_probe:spec(Type, Tab, ProbeSpec).

spec(Type, Tab, ProbeSpec, Opts) -> ds_probe:spec(Type, Tab, ProbeSpec, Opts).

probe(Type, Tab, ProbeSpec) -> ds_probe:start(Type, Tab, ProbeSpec).

probe(Type, Tab, ProbeSpec, Opts) -> ds_probe:start(Type, Tab, ProbeSpec, Opts).


%% @doc Core spec functions
%%
%% These functions are the lowest level API useful to programmatically
%% create spec trees from arbitrary data.

new() -> ds_spec:new().

add(Value, Spec) -> ds_spec:add({Value, []}, Spec).

add(Value, Attrs, Spec) when is_list(Attrs) ->
    ds_spec:add({Value, Attrs}, Spec).

join(Spec1, Spec2) -> ds_spec:join(Spec1, Spec2).

postproc(Spec) -> ds_spec:postproc(Spec).

spec_to_form(Spec) -> ds_types:spec_to_form(Spec).

pretty_print(Spec) -> ds_types:pp_spec(Spec).
