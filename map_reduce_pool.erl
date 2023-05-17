-module(map_reduce_pool).
-compile([export_all,nowarn_export_all]).

reduce_seq(Reduce,KVs) ->
    [KV || {K,Vs} <- group(lists:sort(KVs)),
	   KV <- Reduce(K,Vs)].

split_into(N,L) ->
    split_into(N,L,length(L)).

split_into(1,L,_) ->
    [L];
split_into(N,L,Len) ->
    {Pre,Suf} = lists:split(Len div N,L),
    [Pre|split_into(N-1,Suf,Len-(Len div N))].

group([]) ->
    [];
group([{K,V}|Rest]) ->
    group(K,[V],Rest).

group(K,Vs,[{K,V}|Rest]) ->
    group(K,[V|Vs],Rest);
group(K,Vs,Rest) ->
    [{K,lists:reverse(Vs)}|group(Rest)].

map_reduce_par(Map,M,Reduce,R,Input) ->
    Parent = self(),
    Splits = split_into(M,Input),
    start_pool(),
    spawn_node_workers(nodes()).
    MapFns = [fun() -> map_solver(Parent, Map,R,Split) end || Split <- Splits].

map_solver(Parent, Map,R,Split) ->
			Mapped = [{erlang:phash2(K2,R),{K2,V2}}
				  || {K,V} <- Split,
				     {K2,V2} <- Map(K,V)],
                        io:format("."),
			Parent ! {self(),group(lists:sort(Mapped))}.

% Spawn worker on the node and add them to the global pool
spawn_node_workers(Nodes) ->
    [spawn_link(Node, fun() ->
          NumWorkers = erlang:system_info(schedulers)-1,
          Workers = [worker() || _ <- lists:seq(1,NumWorkers)],
          pool ! {add_workers,Workers}
        end) || Node <- Nodes].


% we're going to start an empty pool at the start and add to it
start_pool() -> 
  true = global:register_name(pool,spawn_link(fun()->pool()
end)).

pool() ->
  pool([],[]).

pool(Workers,All) ->
  receive
    {get_worker,Pid} ->
      case Workers of
        [] ->
          Pid ! {pool,no_worker},
          pool(Workers,All);
        [W|Ws] ->
          Pid ! {pool,W},
          pool(Ws,All)
      end;
    {add_workers,Ws} ->
      format:io("Adding workers to the pool~n",[Ws]),
      pool(Ws++Workers,All++Ws);
    {return_worker,W} ->
      pool([W|Workers],All);
    {stop,Pid} ->
      [unlink(W) || W <- All],
      [exit(W,kill) || W <- All],
      unregister(pool),
      Pid ! {pool,stopped}
   end.

worker() ->
  spawn_link(fun work/0).

work() ->
  receive
    {task,Pid,R,F} ->
      Res = F(),
      Pid ! {R, Res},
      try
        pool ! {return_worker,self()}
      catch
        error:Reason ->
          io:format("Error returning worker to pool: ~p~n", [Reason])
      end,
      work()
   end.




