-module(ds_shell).
-author("Tom Szilagyi <tomszilagyi@gmail.com>").

-export([ run/0
        , set_statusline/1]).

-define(STATUSLINE_KEY, ds_shell_status_line).
-define(PROMPT, "ds> ").

run() ->
    spawn(fun print/0),
    read_loop().

print() -> print_loop(0).

%% This loop models the ds_drv gen_server.
%% It will be in another module, calling our set_statusline/1.
print_loop(N) ->
    receive after 100 -> ok end,
    StatusLine = io_lib:format("~6B", [N]),
    set_statusline(StatusLine),
    print_loop(N+1).

read_loop() ->
    io:fwrite([get_statusline(), $\n]),
    Line = io:get_line(?PROMPT),
    %% reprint prompt one above of where it was
    io:fwrite([line_up_and_erase(), line_up_and_erase(), ?PROMPT, Line]),
    case Line of
        "help\n" ->
            io:format("This is a multi-line response\n"
                      " - option 1\n"
                      " - option 2\n"
                      " - option 3\n\n");
        _ ->
            io:format("Response to input: ~s\n", [Line]) %% ends in newline
    end,
    read_loop().

get_statusline() ->
    case get(?STATUSLINE_KEY) of
        undefined  -> "";
        StatusLine -> StatusLine
    end.

set_statusline(IoData) ->
    put(?STATUSLINE_KEY, IoData),
    io:fwrite([line_up_and_erase(), IoData, $\n]).

%% This is the only terminal-specific bit.
line_up_and_erase() -> line_up_and_erase(terminal()).

line_up_and_erase("vt100") -> "\e[A\e[2K";
line_up_and_erase("dumb")  -> "".

%% Try to find out if we have a usable terminal or not.
%% All terminals tested to be usable are categorized as 'vt100',
%% since that is the compatibility baseline we rely on.
%% Otherwise we treat the terminal as 'dumb'.
terminal() ->
    case ds_opts:getopt(term) of
        undefined ->
            case os:getenv("TERM") of
                "rxvt"        -> "vt100";
                "screen" ++ _ -> "vt100";
                "vt" ++ _     -> "vt100";
                "xterm" ++ _  -> "vt100";
                _             -> "dumb"
            end;
        T -> T
    end.
