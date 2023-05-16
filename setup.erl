-module(setup).
-export([run/0]).

run() ->
    compile_files(),
    ping_nodes().

compile_files() ->
    %% Here you can specify the list of your files to be compiled
    Files = ["naive", "crawl", "map_reduce"],
    [compile(File) || File <- Files].

compile(File) ->
    case compile:file(File, [debug_info]) of
        {ok, _} ->
            io:format("~s compiled successfully~n", [File]);
        error ->
            io:format("Failed to compile ~s~n", [File])
    end.

ping_nodes() ->
    %% Here you can specify the list of your nodes
    Nodes = ['gn1@localhost', 'gn2@localhost', 'gn3@localhost'],
    [ping(Node) || Node <- Nodes].

ping(Node) ->
    case net_adm:ping(Node) of
        pong ->
            io:format("Ping to ~p successful~n", [Node]);
        pang ->
            io:format("Ping to ~p failed~n", [Node])
    end.

