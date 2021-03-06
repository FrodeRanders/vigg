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
serialize([H|T]) ->
  [serialize_struct(H) | serialize(T)];
serialize([]) -> [].

%% @doc Deserializes reply from server
deserialize(<<16#C0:8, Rest/binary>>) ->
  [null | deserialize(Rest)];

deserialize(<<16#C1:8, Num/float, Rest/binary>>) ->
  [Num | deserialize(Rest)];

deserialize(<<16#C2:8, Rest/binary>>) ->
  [false | deserialize(Rest)];

deserialize(<<16#C3:8, Rest/binary>>) ->
  [true | deserialize(Rest)];

deserialize(<<16#8:4/unsigned-integer, Len:4/unsigned-integer, BinStr:Len/binary, Rest/binary>>) ->
  [binary_to_list(BinStr) | deserialize(Rest)];

deserialize(<<16#D0:8, Len:8/unsigned-integer, BinStr:Len/binary, Rest/binary>>) ->
  [binary_to_list(BinStr) | deserialize(Rest)];

deserialize(<<16#D1:8, Len:16/big-unsigned-integer, BinStr:Len/binary, Rest/binary>>) ->
  [binary_to_list(BinStr) | deserialize(Rest)];

deserialize(<<16#D2:8, Len:32/big-unsigned-integer, BinStr:Len/binary, Rest/binary>>) ->
  [binary_to_list(BinStr) | deserialize(Rest)];

deserialize(<<16#C8:8, Integer:8/signed-integer, Rest/binary>>) ->
  [Integer | deserialize(Rest)];

deserialize(<<16#C9:8, Integer:16/big-signed-integer, Rest/binary>>) ->
  [Integer | deserialize(Rest)];

deserialize(<<16#CA:8, Integer:32/big-signed-integer, Rest/binary>>) ->
  [Integer | deserialize(Rest)];

deserialize(<<16#CB:8, Integer:64/big-signed-integer, Rest/binary>>) ->
  [Integer | deserialize(Rest)];

deserialize(<<16#A:4, Len:4/unsigned-integer, Rest/binary>>) ->
  deserialize_map(Len, Rest);

deserialize(<<16#D8:8, Len:8/unsigned-integer, Rest/binary>>) ->
  deserialize_map(Len, Rest);

deserialize(<<16#D9:8, Len:16/big-unsigned-integer, Rest/binary>>) ->
  deserialize_map(Len, Rest);

deserialize(<<16#DA:8, Len:32/big-unsigned-integer, Rest/binary>>) ->
  deserialize_map(Len, Rest);

deserialize(<<16#9:4, Len:4/unsigned-integer, Rest/binary>>) ->
  deserialize_list(Len, Rest);

deserialize(<<16#D4:8, Len:8/unsigned-integer, Rest/binary>>) ->
  deserialize_list(Len, Rest);

deserialize(<<16#D5:8, Len:16/big-unsigned-integer, Rest/binary>>) ->
  deserialize_list(Len, Rest);

deserialize(<<16#D6:8, Len:32/big-unsigned-integer, Rest/binary>>) ->
  deserialize_list(Len, Rest);

deserialize(<<16#B:4, Len:4/unsigned-integer, Signature:8/unsigned-integer, Rest/binary>>) ->
  deserialize_struct(Len, Signature, Rest);

deserialize(<<16#DC:8, Len:8/unsigned-integer, Signature:8/unsigned-integer, Rest/binary>>) ->
  deserialize_struct(Len, Signature, Rest);

deserialize(<<16#DD:8, Len:16/big-unsigned-integer, Signature:8/unsigned-integer, Rest/binary>>) ->
  deserialize_struct(Len, Signature, Rest);

deserialize(<<Integer:8/signed-integer, Rest/binary>>) ->  % have to be here, next to last!
  [Integer | deserialize(Rest)];

deserialize(<<>>) ->
  [].







%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%% @doc Authenticate the session
serialize_struct({hello, Params}) ->
  Map = serialize_map(Params),
  [<<16#B1:8, ?HELLO:8>>, Map];

%% @private
%% @doc Close the connection with the server
serialize_struct({goodbye}) ->
  [<<16#B0:8, ?GOODBYE:8>>];

%% @private
%% @doc Return the current session to a "clean" state
serialize_struct({reset}) ->
  [<<16#B0:8, ?RESET:8>>];

%% @private
%% @doc Execute statement on server
serialize_struct({run, Statement, Params, Options}) ->
  Str = serialize_string(Statement),
  Map = serialize_map(Params),
  Opt = serialize_map(Options),
  [<<16#B3:8, ?RUN:8>>, Str, Map, Opt];

%% @private
%% @doc Begin transaction
serialize_struct({tx_begin, Options}) ->
  Opt = serialize_map(Options),
  [<<16#B1:8, ?BEGIN:8>>, Opt];

%% @private
%% @doc Commit transaction
serialize_struct({tx_commit}) ->
  [<<16#B0:8, ?COMMIT:8>>];

%% @private
%% @doc Rollback transaction
serialize_struct({tx_rollback}) ->
  [<<16#B0:8, ?ROLLBACK:8>>];

%% @private
%% @doc Discard last N issued statement(s)
serialize_struct({discard, N}) ->
  Map = serialize_map(#{n => N}),
  [<<16#B1:8, ?DISCARD:8>>, Map];

%% @private
%% @doc Pull N results
serialize_struct({pull, N}) ->
  Map = serialize_map(#{n => N}),
  [<<16#B1:8, ?PULL:8>>, Map].


%% @private
%% @doc Serialize a Map
serialize_map(Map) ->
  Size = maps:size(Map),
  Prefix =
    if
      Size < 16#10 -> <<(16#A0 + Size):8/unsigned-integer>>;
      Size < 16#100 -> <<16#D8:8, Size:8/unsigned-integer>>;
      Size < 16#10000 -> <<16#D9:8, Size:16/big-unsigned-integer>>;
      Size < 16#100000000 -> <<16#DA:8, Size:32/big-unsigned-integer>>;
      true -> throw("Map header size out of range")
    end,
  Collect =
    fun(K, V, Cum) ->
      Key = serialize_string(atom_to_list(K)),
      Val =
        if
          is_integer(V) ->
            serialize_integer(V);
          true ->
            serialize_string(V)
        end,
      [Val, Key | Cum]       %Cum ++ Key ++ Val
    end,
  Collected = lists:reverse(maps:fold(Collect, [], Map)),
  [Prefix | Collected].


%% @private
%% @doc Serialize a string
serialize_string(Str) ->
  StrLen = string:length(Str),
  Prefix =
    if
      StrLen < 16#10 -> <<(16#80 + StrLen):8>>;
      StrLen < 16#100 -> <<16#D0:8, StrLen:8>>;
      StrLen < 16#10000 -> <<16#D1:8, StrLen:16/big-unsigned-integer>>;
      StrLen < 16#100000000 -> <<16#D2:8, StrLen:32/big-unsigned-integer>>;
      true -> throw("String header size out of range")
    end,
  [Prefix, Str].


%% @private
%% @doc Serialize an integer
serialize_integer(Int) ->
  Bin =
    if
    % Rearrange for performance?
      Int >= -9223372036854775808 andalso Int =< -2147483649 -> <<16#CB:8, Int:64/big-signed-integer>>;
      Int >= -2147483648 andalso Int =< -32769 -> <<16#CA:8, Int:32/big-signed-integer>>;
      Int >= -32768 andalso Int =< -129 -> <<16#C9:8, Int:16/big-signed-integer>>;
      Int >= -128 andalso Int =< -17 -> <<16#C8:8, Int:8/signed-integer>>;
      Int >= -16 andalso Int =< + 127 -> <<Int:8/signed-integer>>;
      Int >= + 128 andalso Int =< + 32767 -> <<16#C9:8, Int:16/big-signed-integer>>;
      Int >= + 32768 andalso Int =< + 2147483647 -> <<16#CA:8, Int:32/big-signed-integer>>;
      Int >= + 2147483648 andalso Int =< + 9223372036854775807 -> <<16#CB:8, Int:64/big-signed-integer>>;
      true -> throw("Integer size out of range")
    end,
  [<<Bin/binary>>].



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%% @private
deserialize_map(Len, Rest) ->
  % Continue with deserializing and then pick the Len first (key, value)-pairs after the fact
  {List, DeserializedRest} = lists:split(Len + Len, deserialize(Rest)),
  [maps:from_list(key_value_pairs(List)) | DeserializedRest].

%% @private
deserialize_list(Len, Rest) ->
  % Continue with deserializing and then pick the Len first elements after the fact
  {List, DeserializedRest} = lists:split(Len, deserialize(Rest)),
  [List | DeserializedRest].

%% @private
deserialize_struct(Len, Signature, Rest) ->
  % Continue with deserializing and then pick the Len first elements after the fact
  {List, DeserializedRest} = lists:split(Len, deserialize(Rest)),
  Type =
    case Signature of
      ?SUCCESS -> success;
      ?RECORD -> record;
      ?IGNORED -> ignored;
      ?FAILURE -> failure
    end,
  [{Type, lists:flatten(List)} | DeserializedRest].


%% @private
key_value_pairs(List) ->
  case List of
    [] -> [];
    %[E1, E2 | Rest] -> [{list_to_atom(E1), E2} | key_value_pairs(Rest)]
    [E1, E2 | Rest] -> [{E1, E2} | key_value_pairs(Rest)]
  end.
