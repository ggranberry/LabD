%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% This is a very simple implementation of map-reduce, in both 
%% sequential and parallel versions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(map_reduce).
-compile([export_all,nowarn_export_all]).

%% We begin with a simple sequential implementation, just to define
%% the semantics of map-reduce. 

%% The input is a collection of key-value pairs. The map function maps
%% each key value pair to a list of key-value pairs. The reduce
%% function is then applied to each key and list of corresponding
%% values, and generates in turn a list of key-value pairs. These are
%% the result.

map_reduce_seq(Map,Reduce,Input) ->
    Mapped = [{K2,V2}
	      || {K,V} <- Input,
		 {K2,V2} <- Map(K,V)],
    io:format("Map phase complete\n"),
    reduce_seq(Reduce,Mapped).

reduce_seq(Reduce,KVs) ->
    [KV || {K,Vs} <- group(lists:sort(KVs)),
	   KV <- Reduce(K,Vs)].

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
    DupNodes = generate_node_list(Splits, nodes()),
    Mappers = 
	[spawn_mapper(Parent,Map,R,Split, Node)
	 || Split <- Splits, Node <- DupNodes],
    Mappeds = 
	[receive {Pid,L} -> L end || Pid <- Mappers],
    io:format("Map phase complete\n"),
    Reducers = 
	[spawn_reducer(Parent,Reduce,I,Mappeds, Node) 
	 || I <- lists:seq(0,R-1), Node <- DupNodes],
    Reduceds = 
	[receive {Pid,L} -> L end || Pid <- Reducers],
    io:format("Reduce phase complete\n"),
    lists:sort(lists:flatten(Reduceds)).

spawn_mapper(Parent, Map, R, Split, Node) ->
    spawn_link(Node, fun() ->
            Mapped = [{erlang:phash2(K2, R), {K2, V2}}
                  || {K, V} <- Split,
                     {K2, V2} <- Map(K, V)],
            io:format("."),
            Parent ! {self(), group(lists:sort(Mapped))}
        end).

spawn_mapper(Parent,Map,R,Split) ->
    spawn_link(fun() ->
			Mapped = [{erlang:phash2(K2,R),{K2,V2}}
				  || {K,V} <- Split,
				     {K2,V2} <- Map(K,V)],
                        io:format("."),
			Parent ! {self(),group(lists:sort(Mapped))}
		end).

split_into(N,L) ->
    split_into(N,L,length(L)).

split_into(1,L,_) ->
    [L];
split_into(N,L,Len) ->
    {Pre,Suf} = lists:split(Len div N,L),
    [Pre|split_into(N-1,Suf,Len-(Len div N))].

spawn_reducer(Parent,Reduce,I,Mappeds) ->
    Inputs = [KV
	      || Mapped <- Mappeds,
		 {J,KVs} <- Mapped,
		 I==J,
		 KV <- KVs],
    spawn_link(fun() -> Result = reduce_seq(Reduce,Inputs),
                        io:format("."),
                        Parent ! {self(),Result} end).

spawn_reducer(Parent,Reduce,I,Mappeds, Node) ->
    Inputs = [KV
	      || Mapped <- Mappeds,
		 {J,KVs} <- Mapped,
		 I==J,
		 KV <- KVs],
    spawn_link(Node, fun() -> Result = reduce_seq(Reduce,Inputs),
                        io:format("."),
                        Parent ! {self(),Result} end).

generate_node_list(Splits, Nodes) ->
    LNodes = length(Nodes),
    LSplits = length(Splits),
    Factor = round_up_division(LSplits, LNodes),
    lists:flatten(lists:duplicate(Factor, Nodes)).

round_up_division(Numerator, Denominator) ->
    case Numerator rem Denominator of
        0 -> Numerator div Denominator;
        _ -> trunc(Numerator / Denominator) + 1
    end.


