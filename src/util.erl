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
-export([connect/0, disconnect/1, query/1, bulk_load/2, fnu/1, fnu/2]).


-define(PRINT(S, A), io:fwrite("~w(~w): " ++ S, [?MODULE, ?LINE | A])).

connect() ->
  {ok, C} = vigg:connect("localhost", 7687, "neo4j", "neo5j", []),
  C.

disconnect(C) ->
  vigg:disconnect(C).

query(C) ->
  %
  Requests = vigg:run("RETURN 1 AS num", #{}, #{}),
  Requests2 = vigg:pull(Requests, 1000),

  %
  ?PRINT("Sending ~p~n", [Requests2]),
  Reply = vigg:request(C, Requests2),
  ?PRINT("Got ~p~n", [Reply]).



bulk_load(C, FileName) ->
  {ok, Binary} = file:read_file(FileName),
  % Separate statements from each other
  Stmts = binary:split(Binary, <<$;>>, [global]),
  % Get rid of line comments (before removing newline chars)
  F = fun(<<Stmt/binary>>) ->
        Lines = lists:map(fun fnu/1, binary:split(Stmt, <<$\n>>, [global])),
        string:trim(combine(" ", Lines))
    end,
  CleanStmts = lists:map(F, Stmts),
  Load =
    fun(E, Acc) ->
      Req = vigg:run(E, #{}, #{}),
      ?PRINT("~p~n", [Req]),
      [Req | Acc]
    end,
  Requests = lists:reverse(lists:foldl(Load, [], CleanStmts)),
  %
  BeginReply = vigg:request(C, vigg:tx_begin(#{})),
  ?PRINT("~n~nTRANSACTION BEGIN ~p~n", [BeginReply]),
  Reply = vigg:request(C, lists:flatten(Requests)),
  ?PRINT("~n~nGot ~p~n", [Reply]),
  CommitReply = vigg:request(C, vigg:tx_commit()),
  ?PRINT("~n~nTRANSACTION COMMIT ~p~n", [CommitReply]).



%% Truncate comments
fnu(<<String/binary>>) ->
  lists:reverse(fnu(binary_to_list(String), [])).

fnu([], Acc) ->
  Acc;
fnu([$/, $/ | _], Acc) ->
  Acc;
fnu([$-, $- | _], Acc) ->
  Acc;
fnu([$\r|T], Acc) ->
  fnu(T, Acc);
fnu([$\n | T], Acc) ->
  fnu(T, [" " | Acc]);
fnu([H|T], Acc) ->
  fnu(T, [H | Acc]).

%% Combines lines into a statement, injecting some char/string between
combine(_Sep, []) -> [];
combine(Sep, [H|T]) -> [Sep, H|combine(Sep, T)].

