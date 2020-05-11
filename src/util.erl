%%%-------------------------------------------------------------------
%%% @author froran
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. May 2020 17:38
%%%-------------------------------------------------------------------
-module(util).
-author("froran").

%% API
-export([handshake/0]).

% debug(D) ->
%    io:format("Algo: ~p~n", [D]).

-define(PRINT(S, A), io:fwrite("~w(~w): " ++ S, [?MODULE,?LINE|A])).
%%-define(PRINT(S, A), true).

handshake() ->
  {ok, C} = vigg:connect("localhost", 7687, "neo4j", "neo5j", []),

  %
  Requests = vigg:run("RETURN 1 AS num", #{}, #{}),
  Requests2 = vigg:pull(Requests, 1000),

  %{_, Message} = vigg_packstream:serialize(Requests2),
  ?PRINT("Sending ~p~n", [Requests2]),

  %
  Reply = vigg:request(C, Requests2),
  ?PRINT("Got ~p~n", [Reply]),

  %
  vigg:disconnect(C).

