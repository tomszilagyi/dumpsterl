%% -*- coding: utf-8 -*-
-module(ds_records).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ init/0
        , reinit/0
        , get_attrs/0
        , put_attrs/1
        , lookup/1]).

-define(STORAGE_KEY, dumpsterl_rec_attrs).

init() -> maybe_init(ds_opts:getopt(rec_attrs)).

%% forced reinitialization used by ds_shell on change of mnesia_dir
reinit() ->
    erase(?STORAGE_KEY),
    init().

get_attrs() -> get(?STORAGE_KEY).

put_attrs(Attrs) -> put(?STORAGE_KEY, Attrs).

maybe_init(false) ->
    erase(?STORAGE_KEY),
    ok;
maybe_init(true) ->
    case get(?STORAGE_KEY) of
        undefined -> do_init();
        _RecAttrs -> ok
    end;
maybe_init(force) -> do_init().

do_init() ->
    LoadedModules = code:all_loaded(),
    io:format("collecting record attributes from ~B loaded modules ... ",
              [length(LoadedModules)]),
    ModuleAttrs = collect(LoadedModules),
    io:format("read ~B records.\n", [length(ModuleAttrs)]),
    SchemaAttrs = mnesia_schema(),
    Attrs = merge(SchemaAttrs, ModuleAttrs),
    io:format("installed attributes for ~B records.\n", [length(Attrs)]),
    put(?STORAGE_KEY, Attrs),
    ok.

lookup(RecId) ->
    case get(?STORAGE_KEY) of
        undefined -> false;
        RecAttrs ->
            case orddict:find(RecId, RecAttrs) of
                error -> false;
                {ok, RAs} -> RAs
            end
    end.

%% Collect record attributes from Mnesia schema read from schema.DAT
mnesia_schema() ->
    case mnesia_dir() of
        undefined -> [];
        MnesiaDir ->
            mnesia_schema(filename:join(MnesiaDir, "schema.DAT"))
    end.

mnesia_schema(SchemaFile) ->
    io:format("collecting record attributes from ~s ... ", [SchemaFile]),
    {ok, _} = dets:open_file(schema, [{file, SchemaFile}, {repair, false}, {keypos, 2}]),
    R = lists:usort(dets:traverse(schema, fun dets_traverse_f/1)),
    dets:close(schema),
    io:format("read ~B records.\n", [length(R)]),
    [{RecId, Attrs, {schema, SchemaFile}} || {RecId, Attrs} <- R].

dets_traverse_f({schema, _Tab, Attrs}) ->
    {record_name, RecName} = lists:keyfind(record_name, 1, Attrs),
    {attributes, RecAttrs} = lists:keyfind(attributes, 1, Attrs),
    {continue, {{RecName, length(RecAttrs)+1}, RecAttrs}}.

%% Try to guess the Mnesia database directory.
%% Might return 'undefined' if no plausible directory is found.
mnesia_dir() ->
    case ds_opts:getopt(mnesia_dir) of
        undefined ->
            try_mnesia_dirs([ mnesia:system_info(directory)
                            , "."
                            , "db"
                            , "system/db"
                            ]);
        MDir -> MDir
    end.

try_mnesia_dirs([]) ->
    io:format("unable to guess mnesia_dir, no record attributes from Mnesia.\n"),
    undefined;
try_mnesia_dirs([Dir|Rest]) ->
    case filelib:is_file(filename:join(Dir, "schema.DAT")) of
        true  -> Dir;
        false -> try_mnesia_dirs(Rest)
    end.

%% Collect attributes of records
%% Returns an orddict keyed with {RecName, Arity}
%% where values are orddicts keyed with field attributes
%% storing a list of values specifying where those attributes
%% were found.
collect(LoadedModules) -> collect(LoadedModules, []).

collect([], Acc) -> Acc;
collect([{_Module, File}|Rest], Acc) ->
    case read_records(File, []) of
        [] ->
            collect(Rest, Acc);
        RAs when is_list(RAs) ->
            collect(Rest, merge(RAs, Acc));
        _Error ->
            collect(Rest, Acc)
    end.

merge([], Acc) -> Acc;
merge([{RecId, Fields, Loc} | Rest], Acc0) ->
    Dict0 = case orddict:find(RecId, Acc0) of
                error -> orddict:new();
                {ok, D} -> D
            end,
    Dict = orddict:update(Fields, fun(Old) -> lists:usort([Loc|Old]) end,
                          [Loc], Dict0),
    merge(Rest, orddict:store(RecId, Dict, Acc0)).

%%% Read record information from file(s)
%%% Adapted from Erlang/OTP stdlib's shell.erl

read_records(FileOrModule, Opts0) ->
    Opts = lists:delete(report_warnings, Opts0),
    case find_file(FileOrModule) of
        {files,[File]} ->
            read_file_records(File, Opts);
        {files,Files} ->
            lists:flatmap(fun(File) ->
                                  case read_file_records(File, Opts) of
                                      RAs when is_list(RAs) -> RAs;
                                      _ -> []
                                  end
                          end, Files);
        Error ->
            Error
    end.

-include_lib("kernel/include/file.hrl").

find_file(Mod) when is_atom(Mod) ->
    case code:which(Mod) of
        File when is_list(File) ->
            {files,[File]};
        preloaded ->
            {_M,_Bin,File} = code:get_object_code(Mod),
            {files,[File]};
        _Else -> % non_existing, interpreted, cover_compiled
            {error,nofile}
    end;
find_file(File) ->
    case catch filelib:wildcard(File) of
        {'EXIT',_} ->
            {error,invalid_filename};
        Files ->
            {files,Files}
    end.

read_file_records(File, Opts) ->
    case filename:extension(File) of
        ".beam" ->
            case beam_lib:chunks(File, [abstract_code,"CInf"]) of
                {ok,{_Mod,[{abstract_code,{Version,Forms}},{"CInf",CB}]}} ->
                    case record_attrs(Forms) of
                        [] when Version =:= raw_abstract_v1 ->
                            [];
                        [] ->
                            %% If the version is raw_X, then this test
                            %% is unnecessary.
                            try_source(File, CB);
                        Records ->
                            Records
                    end;
                {ok,{_Mod,[{abstract_code,no_abstract_code},{"CInf",CB}]}} ->
                    try_source(File, CB);
                Error ->
                    %% Could be that the "Abst" chunk is missing (pre R6).
                    Error
            end;
        _ ->
            parse_file(File, Opts)
    end.

%% This is how the debugger searches for source files. See int.erl.
try_source(Beam, CB) ->
    Os = case lists:keyfind(options, 1, binary_to_term(CB)) of
             false -> [];
             {_, Os0} -> Os0
         end,
    Src0 = filename:rootname(Beam) ++ ".erl",
    case is_file(Src0) of
        true -> parse_file(Src0, Os);
        false ->
            EbinDir = filename:dirname(Beam),
            Src = filename:join([filename:dirname(EbinDir), "src",
                                 filename:basename(Src0)]),
            case is_file(Src) of
                true -> parse_file(Src, Os);
                false -> {error, nofile}
            end
    end.

is_file(Name) ->
    case filelib:is_file(Name) of
        true ->
            not filelib:is_dir(Name);
        false ->
            false
    end.

parse_file(File, Opts) ->
    Cwd = ".",
    Dir = filename:dirname(File),
    IncludePath = [Cwd,Dir|inc_paths(Opts)],
    case epp:parse_file(File, IncludePath, pre_defs(Opts)) of
        {ok,Forms} ->
            record_attrs(Forms);
        Error ->
            Error
    end.

pre_defs([{d,M,V}|Opts]) ->
    [{M,V}|pre_defs(Opts)];
pre_defs([{d,M}|Opts]) ->
    [M|pre_defs(Opts)];
pre_defs([_|Opts]) ->
    pre_defs(Opts);
pre_defs([]) -> [].

inc_paths(Opts) ->
    [P || {i,P} <- Opts, is_list(P)].

record_attrs(Forms) ->
    {Result, _} = lists:foldl(fun ra_fold_f/2, {[], undefined}, Forms),
    Result.

ra_fold_f({attribute, _, file, {File, _LN}}, {RAs, _File}) ->
    {RAs, File};
ra_fold_f({attribute, LN, record, {RecName, RecAttrs}}, {RAs, File}) ->
    Fields = [mk_field(RA) || RA <- RecAttrs],
    {[{{RecName, length(Fields)+1}, Fields, {File, LN}} | RAs], File};
ra_fold_f(_, Acc) ->
    Acc.

mk_field({record_field, _LNfield, {atom, _LN, FieldName}}) ->
    FieldName;
mk_field({record_field, _LNfield, {atom, _LN, FieldName}, _InitForm}) ->
    FieldName;
mk_field({typed_record_field, RecordField, _Type}) ->
    mk_field(RecordField).
