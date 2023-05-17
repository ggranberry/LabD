-module(naive).

-compile([export_all,nowarn_export_all]).

bench() -> {Time, _} = timer:tc(naive, run, []),
            io:format("Time: ~p~n", [Time]).

benchp() -> {Time, _} = timer:tc(naive, runp, []),
            io:format("Time: ~p~n", [Time]).

run() ->
  page_rank:page_rank().

runp() ->
  page_rank:page_rank_par().
