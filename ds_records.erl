-module(ds_records).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([collect/0]).

%% Collect attributes of records
%% Returns an orddict keyed with {RecName, Arity}
%% where values are orddicts keyed with {SourceFile, LineNumber}
%% where values are the lists of attribute (field) names.
collect() ->
    LoadedModules = code:all_loaded(),
    collect(LoadedModules, []).

collect([], Acc) -> Acc;
collect([{Module, File}|Rest], Acc) ->
    case read_records(File, []) of
        [] ->
            collect(Rest, Acc);
        RAs when is_list(RAs) ->
            collect(Rest, merge(RAs, Acc));
        Error ->
            io:format("Module ~p: ~p~n", [Module, Error]),
            collect(Rest, Acc)
    end.

merge([], Acc) -> Acc;
merge([{RecId, Loc, Fields} | Rest], Acc0) ->
    Dict0 = case orddict:find(RecId, Acc0) of
                error -> orddict:new();
                {ok, D} -> D
            end,
    merge(Rest, orddict:store(RecId, orddict:store(Loc, Fields, Dict0), Acc0)).

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
    {[{{RecName, length(Fields)+1}, {File, LN}, Fields} | RAs], File};
ra_fold_f(_, Acc) ->
    Acc.

mk_field({record_field, _LNfield, {atom, _LN, FieldName}}) ->
    FieldName;
mk_field({record_field, _LNfield, {atom, _LN, FieldName}, _InitForm}) ->
    FieldName.
