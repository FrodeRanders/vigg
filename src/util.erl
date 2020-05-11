%%%-------------------------------------------------------------------
%%% @author Frode Randers
%%% @copyright (C) 2020, Försäkringskassan
%%% @doc
%%%
%%% @end
%%% Created : 10. May 2020 17:38
%%%-------------------------------------------------------------------
-module(util).
-author("Frode.Randers@forsakringskassan.se").

%% API
-export([handshake/0]).


-define(PRINT(S, A), io:fwrite("~w(~w): " ++ S, [?MODULE,?LINE|A])).

handshake() ->
  {ok, C} = vigg:connect("localhost", 7687, "neo4j", "neo5j", []),

  %
  Requests = vigg:run("RETURN 1 AS num", #{}, #{}),
  Requests2 = vigg:pull(Requests, 1000),

  %
  ?PRINT("Sending ~p~n", [Requests2]),
  Reply = vigg:request(C, Requests2),
  ?PRINT("Got ~p~n", [Reply]),

  %
  vigg:disconnect(C).

