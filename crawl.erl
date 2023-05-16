%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% This module defines a simple web crawler using map-reduce.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(crawl).
-compile([export_all,nowarn_export_all]).

%% Crawl from a URL, following links to depth D.
%% Before calling this function, the inets service must
%% be started using inets:start().
crawl(Url,D) ->
    dets:open_file(web,[{file,"web.dat"}]),
    Pages = follow(D,[{Url,undefined}]),
    [{U,Body} || {U,Body} <- Pages,
		 Body /= undefined].

follow(0,KVs) ->
    KVs;
follow(D,KVs) ->
    follow(D-1,
	   map_reduce:map_reduce_par(fun map/2, 100, fun reduce/2, 1, KVs)).

map(Url,undefined) ->
    Body = fetch_url(Url),
    dets:insert(web,{Url,Body}),
    [{Url,crawled}] ++
	[{U,undefined} || U <- find_urls(Url,Body)];
map(Url,Body) ->
    [{Url,Body}].

reduce(Url,Bodies) ->
    case [B || B <- Bodies, B/=undefined] of
	[] ->
	    [{Url,undefined}];
	[Body] ->
	    [{Url,Body}]
    end.

fetch_url(Url) ->
  case catch httpc:request(get, {Url, []}, [{timeout,5000}], []) of
    {ok,{_,_Headers,Body}}  ->
      Body;
    {error,Reason} ->
      io:format("Error fetching ~ts:\n  ~p\n",[Url,Reason]),
      "";
    {'EXIT',Reason} ->
      io:format("httpc:request crashed (~p) on:\n  ~p\n",[Reason,Url]),
      ""
  end.

%% Find all the urls in an Html page with a given Url.
find_urls(Url,Html) ->
    Lower = string:to_lower(Html),
    %% Find all the complete URLs that occur anywhere in the page
    Absolute = case re:run(Lower,"\"(?:http|https)://.*?(?=\")",[global]) of
		   {match,Locs} ->
		       [lists:sublist(Html,Pos+2,Len-1)
			|| [{Pos,Len}] <- Locs];
		   _ ->
		       []
	       end,
    %% Find links to files in the same directory, which need to be
    %% turned into complete URLs.
    Relative = case re:run(Lower,"href *= *\"(?!http:)(?!https:).*?(?=\")",[global]) of
		   {match,RLocs} ->
		       [lists:sublist(Html,Pos+1,Len)
			|| [{Pos,Len}] <- RLocs];
		   _ ->
		       []
	       end,
    lists:filter(fun valid/1,
    lists:map(fun convert_unicode/1,
    Absolute ++ [Url++[case lists:last(Url) of
                         $/ -> "";
                         _  -> "/"
                       end]++
		     lists:dropwhile(
		       fun(Char)->Char==$/ end,
		       tl(lists:dropwhile(fun(Char)->Char/=$" end, R)))
		 || R <- Relative])).

%% Exclude invalid URIs (that cause problems with the HTTP client), and overly long ones.
valid(Url) ->
  not lists:member($#,Url) andalso length(Url) < 256.

%% We encounter some URLs with the wrong encoding of unicode
%% characters. This is an attempt to fix some of them. 
convert_unicode(L) ->
  case unicode:characters_to_list(list_to_binary(L)) of
    Conversion when is_list(Conversion) ->
      Conversion;
    _ ->
      L
  end.
  
