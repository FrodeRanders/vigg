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
-export([connect/0, disconnect/1, bulk_load_individual/2, bulk_load_whole/2]).


-define(PRINT(S, A), io:fwrite("~w(~w): " ++ S, [?MODULE, ?LINE | A])).

connect() ->
  {ok, C} = vigg:connect("localhost", 7687, "neo4j", "neo5j", []),
  C.

disconnect(C) ->
  vigg:disconnect(C).

bulk_load_individual(C, FileName) ->
  {ok, Log} = file:open(log_filename(FileName), [write]),
  {ok, Binary} = file:read_file(FileName),
  % Separate statements from each other
  Stmts = binary:split(Binary, <<$;>>, [global]),
  % Get rid of line comments (before removing newline chars)
  F = fun(<<Stmt/binary>>) ->
        Lines = lists:map(fun fnu/1, binary:split(Stmt, <<$\n>>, [global])),
        string:trim(combine(" ", Lines))
    end,
  CleanStmts = lists:map(F, Stmts),
  %
  io:format(Log, "~nStatements loaded from ~s~n", [FileName]),
  Prepare =
    fun(E, Acc) ->
      case E of
        [] -> Acc;
        Str ->
          io:format(Log, "~s~n", [Str]),
          Begin = vigg:tx_begin(#{}),
          Req = vigg:run(Str, #{}, #{}),
          Commit = vigg:tx_commit(),
          [Commit, Req, Begin | Acc] % List is reversed later!
      end
    end,
  Requests = lists:reverse(lists:foldl(Prepare, [], CleanStmts)),
  %
  io:format(Log, "~nExecuting statements~n", []),
  Submit = fun(I) ->
    io:format(Log, "IN: ~p~n", I),
    Reply = vigg:request(C, I),
    lists:map(fun(O) -> io:format(Log, "OUT: ~p~n", [O]) end, Reply)
    end,
  lists:map(Submit, Requests),
  file:close(Log).


bulk_load_whole(C, FileName) ->
  {ok, Log} = file:open(log_filename(FileName), [write]),
  {ok, Binary} = file:read_file(FileName),
  % Separate statements from each other
  Stmts = binary:split(Binary, <<$;>>, [global]),
  % Get rid of line comments (before removing newline chars)
  F = fun(<<Stmt/binary>>) ->
    Lines = lists:map(fun fnu/1, binary:split(Stmt, <<$\n>>, [global])),
    string:trim(combine(" ", Lines))
      end,
  CleanStmts = lists:map(F, Stmts),
  %
  io:format(Log, "~nStatements loaded from ~s~n", [FileName]),
  Load =
    fun(E, Acc) ->
      case E of
        [] -> Acc;
        Str ->
          io:format(Log,"~p~n", [Str]),
          Req = vigg:run(Str, #{}, #{}),
          [Req | Acc]
      end
    end,
  Requests1 = [vigg:tx_begin(#{}) | lists:reverse([vigg:tx_commit() | lists:foldl(Load, [], CleanStmts)])],
  Requests = lists:flatten(Requests1),
  %
  io:format(Log, "~nExecuting statements~n", []),
  lists:map(fun(E) -> io:format(Log, "IN: ~p~n", [E]) end, Requests),
  Reply = vigg:request(C, Requests),
  lists:map(fun(E) -> io:format(Log, "OUT: ~p~n", [E]) end, Reply),
  file:close(Log).

log_filename(FileName) ->
  {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:now_to_local_time(erlang:timestamp()),
  DateTime = lists:flatten(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w",[Year,Month,Day,Hour,Minute,Second])),
  FileName ++ "-" ++ DateTime ++ ".log".

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

