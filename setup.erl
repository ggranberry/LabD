-module(setup).
-export([run/0]).

run() ->
    Files = ["page_rank", "crawl", "map_reduce", "map_reduce_pool", "map_reduce_distributed"],
    compile_files(Files),
    ping_nodes().
    

compile_files(Files) ->
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
    Nodes = ['gn1@localhost', 'gn2@localhost', 'gn3@localhost', 'gn4@localhost', 'gn5@localhost'],
    [ping(Node) || Node <- Nodes].

ping(Node) ->
    case net_adm:ping(Node) of
        pong ->
            io:format("Ping to ~p successful~n", [Node]);
        pang ->
            io:format("Ping to ~p failed~n", [Node])
    end.

