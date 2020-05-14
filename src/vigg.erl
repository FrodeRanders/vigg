%%%-------------------------------------------------------------------
%%% @author Frode Randers
%%% @copyright (C) 2020, Försäkringskassan
%%% @doc
%%%
%%% @end
%%% Created : 07. May 2020 20:24
%%%-------------------------------------------------------------------
-module(vigg).
-author("Frode.Randers@forsakringskassan.se").

%% API
-export([
  connect/1, connect/5,
  disconnect/1,
  request/2
]).

-export([
  run/3, run/4,
  tx_begin/1, tx_begin/2,
  tx_commit/0, tx_commit/1,
  tx_rollback/0, tx_rollback/1,
  discard/1, discard/2,
  pull/1, pull/2,
  reset/0, reset/1
]).


%%%===================================================================
%%% API
%%%===================================================================

connect(Settings) ->
  Host = proplists:get_value(host, Settings, "localhost"),
  Port = proplists:get_value(port, Settings, 7687),
  Username = proplists:get_value(username, Settings),
  Password = proplists:get_value(password, Settings),
  connect(Host, Port, Username, Password, []).

connect(Host, Port, Username, Password, Opts) ->
  {ok, C} = vigg_connection:start_link(),
  connect(C, Host, Port, Username, Password, Opts).

connect(C, Host, Port, Username, Password, Opts) ->
  case gen_server:call(C, {connect, Host, Port, Username, Password, Opts}, infinity) of
    connected ->
      {ok, C};
    Error = {error, _} ->
      Error
  end.

disconnect(C) ->
  case gen_server:call(C, {disconnect}, infinity) of
    disconnected ->
      {ok, C};
    Error = {error, _} ->
      Error
  end.


request(C, Requests) ->
  gen_server:call(C,{request, Requests}, infinity).


%%%===================================================================
%%% Helpers
%%%===================================================================

run(Statement, Params, Options) ->
  [{run, Statement, Params, Options}].

run(Acc, Statement, Params, Options) when is_list(Acc) ->
  Acc ++ [{run, Statement, Params, Options}].

tx_begin(Options) ->
  [{tx_begin, Options}].

tx_begin(Acc, Options) when is_list(Acc) ->
  Acc ++ [{tx_begin, Options}].

tx_commit() ->
  [{tx_commit}].

tx_commit(Acc) when is_list(Acc) ->
  Acc ++ [{tx_commit}].

tx_rollback() ->
  [{tx_rollback}].

tx_rollback(Acc) when is_list(Acc) ->
  Acc ++ [{tx_rollback}].

discard(N) when is_number(N) ->
  [{discard, N}].

discard(Acc, N) when is_list(Acc) andalso is_number(N) ->
  Acc ++ [{discard, N}].

pull(N) when is_number(N) ->
  [{pull, N}].

pull(Acc, N) when is_list(Acc) andalso is_number(N) ->
  Acc ++ [{pull, N}].

reset() ->
  [{reset}].

reset(Acc) when is_list(Acc) ->
  Acc ++ [{reset}].

