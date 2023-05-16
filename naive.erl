-module(naive).

-compile([export_all,nowarn_export_all]).

bench() -> {Time, _} = timer:tc(naive, run, []),
            io:format("Time: ~p~n", [Time]).

run() ->
  crawl:crawl("http://www.chalmers.se/",3),
  page_rank:page_rank_par().
