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
    spawn_node_workers(nodes()),
    Mappers = [spawn_mapper(Parent,Map,R,Split, get_worker_from_pool()) || Split <- Splits],
	  Mappeds = [receive {Pid,L} -> L end || Pid <- Mappers],
    io:format("Map phase complete\n"),
    Reducers = [spawn_reducer(Parent,Reduce,I,Mappeds, get_worker_from_pool()) || I <- lists:seq(0,R-1)],
    Reduceds = 
	[receive {Pid,L} -> L end || Pid <- Reducers],
    io:format("Reduce phase complete\n"),
    lists:sort(lists:flatten(Reduceds)).

spawn_reducer(Parent,Reduce,I,Mappeds, Worker) ->
    Ref = make_ref(),
    F = fun() ->
            Inputs = [KV
                      || Mapped <- Mappeds,
                         {J,KVs} <- Mapped,
                         I==J,
                         KV <- KVs],
            io:format("."),
            reduce_seq(Reduce,Inputs)
        end,
    Worker ! {task,Parent, Ref, F},
    Ref.

% send a task to the worker and tell it to send the parent back a ref to the job the the results
spawn_mapper(Parent, Map,R,Split, Worker) ->
      Ref = make_ref(),
      F = fun() ->
            {ok,web} = dets:open_file(web,[{file,"web.dat"}]), % make sure that the file is open on the node
            Mapped = [{erlang:phash2(K2,R),{K2,V2}}
                      || {K,V} <- Split,
                         {K2,V2} <- Map(K,V)],
            io:format("."),
            group(lists:sort(Mapped))
          end,
      Worker ! {task,Parent,Ref,F},
      Ref.

% Spawn worker on the node and add them to the global pool
spawn_node_workers(Nodes) ->
    [spawn_link(Node, fun() ->
          NumWorkers = erlang:system_info(schedulers)-1,
          Workers = [worker() || _ <- lists:seq(1,NumWorkers)],
          global:whereis_name(pool) ! {add_workers,Workers}
        end) || Node <- Nodes].


% we're going to start an empty pool at the start and add to it
start_pool() -> 
  yes = global:register_name(pool,spawn_link(fun()->pool()
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
      io:format("Adding workers to the pool~n",[]),
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
        global:whereis_name(pool) ! {return_worker,self()}
      catch
        error:Reason ->
          io:format("Error returning worker to pool: ~p~n", [Reason])
      end,
      work()
   end.


get_worker_from_pool() ->
    global:whereis_name(pool) ! {get_worker, self()},
    receive
        {pool, no_worker} ->
            timer:sleep(100), % Sleep for 100 milliseconds
            get_worker_from_pool(); % Try again
        {pool, Worker} ->
            Worker
    end.

