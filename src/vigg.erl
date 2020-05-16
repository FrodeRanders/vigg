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
  run/3,
  tx_begin/1,
  tx_commit/0,
  tx_rollback/0,
  discard/1,
  pull/1,
  reset/0
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

tx_begin(Options) ->
  [{tx_begin, Options}].

tx_commit() ->
  [{tx_commit}].

tx_rollback() ->
  [{tx_rollback}].

discard(N) when is_number(N) ->
  [{discard, N}].

pull(N) when is_number(N) ->
  [{pull, N}].

reset() ->
  [{reset}].
