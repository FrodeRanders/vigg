%%%-------------------------------------------------------------------
%%% @author Frode Randers
%%% @copyright (C) 2020, Försäkringskassan
%%% @doc
%%%
%%% @end
%%% Created : 07. May 2020 21:37
%%%-------------------------------------------------------------------
-module(vigg_packstream).
-author("Frode.Randers@forsakringskassan.se").

%% API
-export([serialize/1, deserialize/1]).


-define(HELLO, 16#01).
-define(GOODBYE, 16#02).
-define(RESET, 16#0F).
-define(RUN, 16#10).
-define(BEGIN, 16#11).
-define(COMMIT, 16#12).
-define(ROLLBACK, 16#13).
-define(DISCARD, 16#2F).
-define(PULL, 16#3F).

-define(SUCCESS, 16#70).
-define(RECORD, 16#71).
-define(IGNORED, 16#7E).
-define(FAILURE, 16#7F).


%%%===================================================================
%%% API
%%%===================================================================

%% @doc Serializes a list of Bolt requests into Message(s)
serialize([H | T]) ->
  A = serialize_struct(H),
  B = serialize(T),
  A ++ B;
serialize([]) -> [].

%% @doc Deserializes reply from server
deserialize(<<16#B1:8, ?SUCCESS:8, Rest/binary>>) ->
  {success, deserialize(Rest)};

deserialize(<<16#B1:8, ?RECORD:8, Rest/binary>>) ->
  {record, deserialize(Rest)};

deserialize(<<16#B1:8, ?IGNORED:8, Rest/binary>>) ->
  {ignored, deserialize(Rest)};

deserialize(<<16#B1:8, ?FAILURE:8, Rest/binary>>) ->
  {failure, deserialize(Rest)};

deserialize(<<16#C0, Rest/binary>>) ->
  [null | deserialize(Rest)];

deserialize(<<16#C1, Num/float-unit:1, Rest/binary>>) ->
  [Num | deserialize(Rest)];

deserialize(<<16#C2, Rest/binary>>) ->
  [false | deserialize(Rest)];

deserialize(<<16#C3, Rest/binary>>) ->
  [true | deserialize(Rest)];

deserialize(<<16#8:4/unsigned-integer, Len:4/unsigned-integer, String:Len/binary, Rest/binary>>) ->
  [deserialize_string(String) | deserialize(Rest)];

deserialize(<<16#D0, Len:8/unsigned-integer, String:Len/binary, Rest/binary>>) ->
  [deserialize_string(String) | deserialize(Rest)];

deserialize(<<16#D1, Len:16/big-unsigned-integer, String:Len/binary, Rest/binary>>) ->
  [deserialize_string(String) | deserialize(Rest)];

deserialize(<<16#D2, Len:32/big-unsigned-integer, String:Len/binary, Rest/binary>>) ->
  [deserialize_string(String) | deserialize(Rest)];

deserialize(<<16#C8, Integer:8/signed-integer, Rest/binary>>) ->
  [Integer | deserialize(Rest)];

deserialize(<<16#C9, Integer:16/big-signed-integer, Rest/binary>>) ->
  [Integer | deserialize(Rest)];

deserialize(<<16#CA, Integer:32/big-signed-integer, Rest/binary>>) ->
  [Integer | deserialize(Rest)];

deserialize(<<16#CB, Integer:64/big-signed-integer, Rest/binary>>) ->
  [Integer | deserialize(Rest)];

deserialize(<<16#A:4, Len:4/unsigned-integer, Rest/binary>>) ->
  % Continue with deserializing and then pick the Len first (key, value)-pairs after the fact
  {List, DeserializeRest} = lists:split(Len + Len, deserialize(Rest)),
  [maps:from_list(List) | DeserializeRest];

deserialize(<<16#D8, Len:8/unsigned-integer, Rest/binary>>) ->
  % Continue with deserializing and then pick the Len first (key, value)-pairs after the fact
  {List, DeserializeRest} = lists:split(Len + Len, deserialize(Rest)),
  [maps:from_list(List) | DeserializeRest];

deserialize(<<16#D9, Len:16/big-unsigned-integer, Rest/binary>>) ->
  % Continue with deserializing and then pick the Len first (key, value)-pairs after the fact
  {List, DeserializeRest} = lists:split(Len + Len, deserialize(Rest)),
  [maps:from_list(List) | DeserializeRest];

deserialize(<<16#DA, Len:32/big-unsigned-integer, Rest/binary>>) ->
  % Continue with deserializing and then pick the Len first (key, value)-pairs after the fact
  {List, DeserializeRest} = lists:split(Len + Len, deserialize(Rest)),
  [maps:from_list(List) | DeserializeRest];

deserialize(<<16#9:4, Len:4/unsigned-integer, Rest/binary>>) ->
  % Continue with deserializing and then pick the Len first elements after the fact
  {List, DeserializeRest} = lists:split(Len, deserialize(Rest)),
  [List | DeserializeRest];

deserialize(<<16#D4, Len:8/unsigned-integer, Rest/binary>>) ->
  % Continue with deserializing and then pick the Len first elements after the fact
  {List, DeserializeRest} = lists:split(Len, deserialize(Rest)),
  [List | DeserializeRest];

deserialize(<<16#D5, Len:16/big-unsigned-integer, Rest/binary>>) ->
  % Continue with deserializing and then pick the Len first elements after the fact
  {List, DeserializeRest} = lists:split(Len, deserialize(Rest)),
  [List | DeserializeRest];

deserialize(<<16#D6, Len:32/big-unsigned-integer, Rest/binary>>) ->
  % Continue with deserializing and then pick the Len first elements after the fact
  {List, DeserializeRest} = lists:split(Len, deserialize(Rest)),
  [List | DeserializeRest];

deserialize(<<16#B:4, Len:4/unsigned-integer, Rest/binary>>) ->
  % Continue with deserializing and then pick the Len first elements after the fact
  {List, DeserializeRest} = lists:split(Len, deserialize(Rest)),
  [List | DeserializeRest]; %% I need to rethink this! What would be a suitable data type here?

deserialize(<<16#DC, Len:8/unsigned-integer, Rest/binary>>) ->
  % Continue with deserializing and then pick the Len first elements after the fact
  {List, DeserializeRest} = lists:split(Len, deserialize(Rest)),
  [List | DeserializeRest];

deserialize(<<16#DD, Len:16/big-unsigned-integer, Rest/binary>>) ->
  % Continue with deserializing and then pick the Len first elements after the fact
  {List, DeserializeRest} = lists:split(Len, deserialize(Rest)),
  [List | DeserializeRest];

deserialize(<<Integer:8/signed-integer, Rest/binary>>) ->  % have to be last!
  [Integer | deserialize(Rest)].






%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%% @doc Authenticate the session
serialize_struct({hello, Params}) ->
  {MapLen, Map} = serialize_map(Params),
  Len = (MapLen + 2), % add 2 for struct header
  {6 + MapLen, [<<Len:16/big-unsigned-integer, 16#B1:8, ?HELLO:8>>] ++ Map ++ [<<0:16>>]};

%% @private
%% @doc Close the connection with the server
serialize_struct({goodbye}) ->
  {6, [<<16#2:16/big-unsigned-integer, 16#B0:8, ?GOODBYE:8, 0:16>>]};

%% @private
%% @doc Return the current session to a "clean" state
serialize_struct({reset}) ->
  {6, [<<16#2:16/big-unsigned-integer, 16#B0:8, ?RESET:8, 0:16>>]};

%% @private
%% @doc Execute statement on server
serialize_struct({run, Statement, Params, Options}) ->
  {StrLen, Str} = serialize_string(Statement),
  {MapLen, Map} = serialize_map(Params),
  {OptLen, Opt} = serialize_map(Options),
  Len = StrLen + MapLen + OptLen + 2, % add 2 for struct header
  {6 + StrLen + MapLen + OptLen, [<<Len:16/big-unsigned-integer, 16#B3:8, ?RUN:8>>] ++ Str ++ Map ++ Opt ++ [<<0:16>>]};

%% @private
%% @doc Begin transaction
serialize_struct({tx_begin, Options}) ->
  {OptLen, Opt} = serialize_map(Options),
  Len = OptLen + 2, % add 2 for struct header
  {6 + OptLen, [<<Len:16/big-unsigned-integer, 16#B1:8, ?BEGIN:8>>] ++ Opt ++ [<<0:16>>]};

%% @private
%% @doc Commit transaction
serialize_struct({tx_commit}) ->
  {6, [<<2:16/big-unsigned-integer, 16#B0:8, ?COMMIT:8, 0:16>>]};

%% @private
%% @doc Rollback transaction
serialize_struct({tx_rollback}) ->
  {6, [<<2:16/big-unsigned-integer, 16#B0:8, ?ROLLBACK:8, 0:16>>]};

%% @private
%% @doc Discard last N issued statement(s)
serialize_struct({discard, N}) ->
  {MapLen, Map} = serialize_map(#{n => N}),
  Len = MapLen + 2, % add 2 for struct header
  {6 + MapLen, [<<Len:16/big-unsigned-integer, 16#B1:8, ?DISCARD:8>>] ++ Map ++ [<<0:16>>]};

%% @private
%% @doc Pull N results
serialize_struct({pull, N}) ->
  {MapLen, Map} = serialize_map(#{n => N}),
  Len = MapLen + 2, % add 2 for struct header
  {6 + MapLen, [<<Len:16/big-unsigned-integer, 16#B1:8, ?PULL:8>>] ++ Map ++ [<<0:16>>]}.


%% @private
%% @doc Serialize a Map
serialize_map(Map) ->
  Size = maps:size(Map),
  {PrefixLen, Prefix} =
    if
      Size < 16#10 -> {1, <<(16#A0 + Size):8/unsigned-integer>>};
      Size < 16#100 -> {2, <<16#D8:8, Size:8/unsigned-integer>>};
      Size < 16#10000 -> {3, <<16#D9:8, Size:16/big-unsigned-integer>>};
      Size < 16#100000000 -> {5, <<16#DA:8, Size:32/big-unsigned-integer>>};
      true -> throw("Map header size out of range")
    end,
  Collect =
    fun(K, V, {CumLen, Cum}) ->
      {KeyLen, Key} = serialize_string(atom_to_list(K)),
      {ValLen, Val} =
        if
          is_integer(V) ->
            serialize_integer(V);
          true ->
            serialize_string(V)
        end,
      {CumLen + KeyLen + ValLen, Cum ++ Key ++ Val}
    end,
  {Len, Collected} = maps:fold(Collect, {0, []}, Map),
  {PrefixLen + Len, [Prefix] ++ Collected}.


%% @private
%% @doc Serialize a string
serialize_string(Str) ->
  Bin = unicode:characters_to_binary(Str),
  StrLen = byte_size(Bin),
  {PrefixLen, Prefix} =
    if
      StrLen < 16#10 -> {1, <<(16#80 + StrLen):8>>};
      StrLen < 16#100 -> {2, <<16#D0:8, StrLen:8>>};
      StrLen < 16#10000 -> {3, <<16#D1:8, StrLen:16/big-unsigned-integer>>};
      StrLen < 16#100000000 -> {5, <<16#D2:8, StrLen:32/big-unsigned-integer>>};
      true -> throw("String header size out of range")
    end,
  {PrefixLen + StrLen, [Prefix, <<Bin/binary>>]}.


%% @private
%% @doc Serialize an integer
serialize_integer(Int) ->
  {Len, Bin} =
    if
    % Rearrange for performance?
      Int >= -9223372036854775808 andalso Int =< -2147483649 -> {9, <<16#CB:8, Int:64/big-signed-integer>>};
      Int >= -2147483648 andalso Int =< -32769 -> {5, <<16#CA:8, Int:32/big-signed-integer>>};
      Int >= -32768 andalso Int =< -129 -> {3, <<16#C9:8, Int:16/big-signed-integer>>};
      Int >= -128 andalso Int =< -17 -> {2, <<16#C8:8, Int:8/signed-integer>>};
      Int >= -16 andalso Int =< + 127 -> {1, <<Int:8/signed-integer>>};
      Int >= + 128 andalso Int =< + 32767 -> {3, <<16#C9:8, Int:16/big-signed-integer>>};
      Int >= + 32768 andalso Int =< + 2147483647 -> {5, <<16#CA:8, Int:32/big-signed-integer>>};
      Int >= + 2147483648 andalso Int =< + 9223372036854775807 -> {9, <<16#CB:8, Int:64/big-signed-integer>>};
      true -> throw("Integer size out of range")
    end,
  {Len, [<<Bin/binary>>]}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%% @private
%% @doc Deserialize an individual string
deserialize_string(Bin) ->
  case unicode:characters_to_list(Bin) of
    {error, _S, _Rest} -> throw({invalid_string, Bin});
    {incomplete, _S, _Rest} -> throw({invalid_string, Bin});
    String -> String
  end.
