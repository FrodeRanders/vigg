%%%-------------------------------------------------------------------
%%% @author Frode Randers
%%% @copyright (C) 2020, Försäkringskassan
%%% @doc
%%%
%%% @end
%%% Created : 07. May 2020 20:24
%%%-------------------------------------------------------------------
-module(vigg_connection).
-author("Frode.Randers@forsakringskassan.se").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(PRINT(S, A), io:fwrite("~w(~w): " ++ S, [?MODULE,?LINE|A])).
%%-define(PRINT(S, A), true).

%%--------------------------------------------------------------------
%% Status can be one of:
%%  'new' : a connection was established with no history but without agreements of any kind
%%  'negotiated' : protocol version is agreed upon for further communication
%%  'ready' : an authenticated session is ready for packstream messaging (according to negotiated version),
%%  'indeterminate' : the connection has ended up in an undefined/broken state
%%--------------------------------------------------------------------
-record(vigg_session, {
  socket,
  timeout = 5000,
  state = new
}).



%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawns the server and registers the local name (unique)
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec(init(Args :: term()) ->
  {ok, State :: #vigg_session{}} | {ok, State :: #vigg_session{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  {ok, #vigg_session{}}.

%% @private
%% @doc Handling call messages
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #vigg_session{}) ->
  {reply, Reply :: term(), NewState :: #vigg_session{}} |
  {reply, Reply :: term(), NewState :: #vigg_session{}, timeout() | hibernate} |
  {noreply, NewState :: #vigg_session{}} |
  {noreply, NewState :: #vigg_session{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #vigg_session{}} |
  {stop, Reason :: term(), NewState :: #vigg_session{}}).
handle_call(Request, _From, State = #vigg_session{}) ->
  call(Request, State).

%% @private
%% @doc Handling cast messages
-spec(handle_cast(Request :: term(), State :: #vigg_session{}) ->
  {noreply, NewState :: #vigg_session{}} |
  {noreply, NewState :: #vigg_session{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #vigg_session{}}).
handle_cast(_Request, State = #vigg_session{}) ->
  {noreply, State}.

%% @private
%% @doc Handling all non call/cast messages
-spec(handle_info(Info :: timeout() | term(), State :: #vigg_session{}) ->
  {noreply, NewState :: #vigg_session{}} |
  {noreply, NewState :: #vigg_session{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #vigg_session{}}).
handle_info(_Info, State = #vigg_session{}) ->
  {noreply, State}.

%% @private
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #vigg_session{}) -> term()).
terminate(_Reason, _State = #vigg_session{}) ->
  ok.

%% @private
%% @doc Convert process state when code is changed
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #vigg_session{},
    Extra :: term()) ->
  {ok, NewState :: #vigg_session{}} | {error, Reason :: term()}).
code_change(_OldVsn, State = #vigg_session{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%% @doc Handle 'connect' call
call({connect, Host, Port, UserName, Password, Options}, _State) ->
  % Important to disable Nagle algorithm, otherwise we will get latencies
  Opts = [binary, {packet, raw}, {nodelay, true}, {keepalive, true}, {active, false}],
  Timeout = 5000,
  case gen_tcp:connect(Host, Port, Opts, Timeout) of
    {ok, Sock} ->
      % Arrive at a known state buffer size-wise
      {ok, [{sndbuf, SendBufferSize}, {recbuf, ReceiveBufferSize}]} = inet:getopts(Sock, [sndbuf, recbuf]),
      inet:setopts(Sock, [{buffer, max(SendBufferSize, ReceiveBufferSize)}]),

      % Negotiate protocol version with server
      case negotiate(Sock, Timeout) of
        {ok, _} ->
          % Authenticate with server
          case authenticate(Sock, Timeout, UserName, Password, Options) of
            {authenticated, AuthenticatedState} ->
              {reply, connected, AuthenticatedState};

            {error, Reason2} ->
              {stop, Reason2, #vigg_session{}}
          end;
        {error, Reason1} ->
          {stop, Reason1, #vigg_session{}}
      end;

    {error, Reason} ->
      {stop, Reason, #vigg_session{}}
  end;

%% @private
%% @doc Handle 'disconnect' call
call({disconnect}, State) ->
  Sock = State#vigg_session.socket,
  case State#vigg_session.state of
    ready ->
      {_, Goodbye} = vigg_packstream:serialize([{goodbye}]),
      gen_tcp:send(Sock, Goodbye), % TODO error handling
      gen_tcp:close(Sock), % TODO error handling
      {reply, disconnected, #vigg_session{}};

    _ ->
      _ = gen_tcp:close(Sock),
      {reply, disconnected, #vigg_session{}}
  end;

%% @private
%% @doc Handle 'request' call
%% {request,[{run,"RETURN 1 AS num",#{},#{}},{pull,1000}]}
call({request, Requests}, State) ->
  Sock = State#vigg_session.socket,
  case State#vigg_session.state of
    ready ->
      {_, Message} = vigg_packstream:serialize(Requests),
      gen_tcp:send(Sock, Message), % TODO error handling
      {ok, Reply} = vigg_packstream:deserialize(Sock, State#vigg_session.timeout),
      {reply, Reply, State};

    _ ->
      error_logger:error_msg("Session not ready: ", [State]),
      {error, connection_not_ready, #vigg_session{}}
  end;

call(Request, State) ->
  error_logger:error_msg("Unknown call: ", [Request]),
  {stop, unknown_request, State}.


%% @private
%% @doc Negotiate protocol version with server
negotiate(Sock, Timeout) ->
  Message = [
    <<16#60:8, 16#60:8, 16#B0:8, 16#17:8>>, % GO GO BOLT!
    <<4:32/big-unsigned-integer, 0:32, 0:32, 0:32>> % Only interested in version 4!
  ],
  ok = gen_tcp:send(Sock, Message),
  case gen_tcp:recv(Sock, 4, Timeout) of
    {ok, <<4:32>>} ->
      {ok, #vigg_session{socket = Sock, state = negotiated}};

    {error, timeout} = Cause ->
      error_logger:error_msg("Timeout while waiting for handshake reply: After ", [Timeout]),
      Cause;

    Error ->
      error_logger:error_msg("Could not negotiate with server: ", [Error]),
      {error, negotiation_failed}
  end.

%% @private
%% @doc Authenticate with server
authenticate(Sock, Timeout, UserName, Password, _Options) ->
  {_MsgLen, Msg} = vigg_packstream:serialize([{hello, #{principal => UserName, scheme => "basic", credentials => Password, user_agent => "vigg/1"}}]),
  gen_tcp:send(Sock, Msg), % TODO error handling

  {ok, Reply} = vigg_packstream:deserialize(Sock, Timeout),
  case Reply of
    {success, Map} ->
      Server = maps:get(server, Map),
      ConnectionId = maps:get(connection_id, Map),
      ?PRINT("Connected to ~p (~p)~n", [Server, ConnectionId]),
      {authenticated, #vigg_session{socket = Sock, state = ready}};

    Other ->
      error_logger:error_msg("Could not authenticate with server: ", [Other]),
      {error, negotiation_failed}
  end.

