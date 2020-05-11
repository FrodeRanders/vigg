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
-export([serialize/1, deserialize/3]).


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
  {ALen, A} = serialize_struct(H),
  {BLen, B} = serialize(T),
  {ALen + BLen, A ++ B};
serialize([]) -> {0, []}.


%% @doc Deserialize reply from server
%% We observe that these reply structs only contain one field
deserialize(Sock, Timeout, Head) ->
  case Head of
    <<Size:16/big-unsigned-integer, 16#B1:8, ?SUCCESS:8>> ->
      ExpectedSize = Size - 2, % TODO arithmetics around wrapping need to be verified!
      {ok, Data} = deserialize_struct(Sock, Timeout, ExpectedSize),
      {success, Data};

    <<Size:16/big-unsigned-integer, 16#B1:8, ?RECORD:8>> ->
      ExpectedSize = Size - 2,
      {ok, Data} = deserialize_struct(Sock, Timeout, ExpectedSize),
      {record, Data};

    <<Size:16/big-unsigned-integer, 16#B1:8, ?IGNORED:8>> ->
      ExpectedSize = Size - 2,
      {ok, Data} = deserialize_struct(Sock, Timeout, ExpectedSize),
      {ignored, Data};

    <<Size:16/big-unsigned-integer, 16#B1:8, ?FAILURE:8>> ->
      ExpectedSize = Size - 2,
      {ok, Data} = deserialize_struct(Sock, Timeout, ExpectedSize),
      {failure, Data}
  end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%% @doc Authenticate the session
serialize_struct({hello, Params}) ->
  {MapLen, Map} = serialize_map(Params),
  Len = (MapLen + 2) rem 16#100, % add 2 for struct header
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
  Len = (StrLen + MapLen + OptLen + 2) rem 16#100, % add 2 for struct header
  {6 + StrLen + MapLen + OptLen, [<<Len:16/big-unsigned-integer, 16#B3:8, ?RUN:8>>] ++ Str ++ Map ++ Opt ++ [<<0:16>>]};

%% @private
%% @doc Begin transaction
serialize_struct({tx_begin, Options}) ->
  {OptLen, Opt} = serialize_map(Options),
  Len = (OptLen + 2) rem 16#100, % add 2 for struct header
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
  Len = (MapLen + 2) rem 16#100, % add 2 for struct header
  {6 + MapLen, [<<Len:16/big-unsigned-integer, 16#B1:8, ?DISCARD:8>>] ++ Map ++ [<<0:16>>]};

%% @private
%% @doc Pull N results
serialize_struct({pull, N}) ->
  {MapLen, Map} = serialize_map(#{n => N}),
  Len = (MapLen + 2) rem 16#100, % add 2 for struct header
  {6 + MapLen, [<<Len:16/big-unsigned-integer, 16#B1:8, ?PULL:8>>] ++ Map ++ [<<0:16>>]}.


%% @private
%% @doc Serialize a Map
serialize_map(Map) ->
  Size = maps:size(Map),
  {PrefixLen, Prefix} = if
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


%% @private
%% @doc Deserialize a struct
deserialize_struct(Sock, Timeout, ExpectedSize) ->
  % Does always contain a map
  {Size, Value} = deserialize_object(Sock, Timeout),
  case Size rem 16#100 of
    ExpectedSize ->
      {ok, Value};

    L ->
      error_logger:error_msg("Protocol violation: Struct size mismatch: Was ~.16B (~.16B modulo 0x100), expected ~.16B", [Size, L, ExpectedSize]),
      {error, mismatch}

  end.


%% @private
%% @doc Deserialize an individual object
deserialize_object(Sock, Timeout) ->
  case gen_tcp:recv(Sock, 1, Timeout) of
    {ok, <<Byte:8/unsigned-integer>>} ->
      {Type, PrefixSize, Size, Value} =
        if
        % Special values
          Byte == 16#C0 ->
            {null, 0, 1, []};

          Byte == 16#C2 ->
            {boolean, 0, 1, [false]};

          Byte == 16#C3 ->
            {boolean, 0, 1, [true]};

        % Various strings
          Byte >= 16#80 andalso Byte =< 16#8F ->
            {string, 1, Byte - 16#80, []}; % A single byte for size

          Byte >= 16#D0 andalso Byte =< 16#D2 ->
            SizeOfSize = (Byte - 16#D0) + 1, % Variable number of bytes for size
            {ok, <<SpecifiedSize/big-unsigned-integer>>} = gen_tcp:recv(Sock, SizeOfSize, Timeout),
            {string, SizeOfSize, SpecifiedSize, []};

        % Various integers
          Byte == 16#CB ->
            {integer, 1, 8, []};

          Byte == 16#CA ->
            {integer, 1, 4, []};

          Byte == 16#C9 ->
            {integer, 1, 2, []};

          Byte == 16#C8 ->
            {integer, 1, 1, []};

          Byte >= -16 andalso Byte =< + 127 ->
            {integer, 0, 1, [Byte]}; % Special case

        % Floating point
          Byte == 16#C1 ->
            {float, 1, 8, []};

        % Various maps
          Byte >= 16#A0 andalso Byte =< 16#AF ->
            {map, 1, Byte - 16#A0, []};

          Byte >= 16#D8 andalso Byte =< 16#DA ->
            SizeOfSize = Byte - 16#D8 + 1,
            {ok, <<SpecifiedSize/big-unsigned-integer>>} = gen_tcp:recv(Sock, SizeOfSize, Timeout),
            {map, SizeOfSize, SpecifiedSize, []};

        % Various lists
          Byte >= 16#90 andalso Byte =< 16#9F ->
            {list, 1, Byte - 16#90, []};

          Byte >= 16#D4 andalso Byte =< 16#D6 ->
            SizeOfSize = (Byte - 16#D4) + 1, % Variable number of bytes for size
            {ok, <<SpecifiedSize/big-unsigned-integer>>} = gen_tcp:recv(Sock, SizeOfSize, Timeout),
            {list, SizeOfSize, SpecifiedSize, []};

        % Various structs
          Byte >= 16#B0 andalso Byte =< 16#BF ->
            {struct, 1, Byte - 16#B0, []};

          Byte >= 16#DC andalso Byte =< 16#DD ->
            SizeOfSize = (Byte - 16#DC) + 1, % Variable number of bytes for size
            {ok, <<SpecifiedSize/big-unsigned-integer>>} = gen_tcp:recv(Sock, SizeOfSize, Timeout),
            {struct, SizeOfSize, SpecifiedSize, []};


          true ->
            throw(io:format("Protocol violation: Cannot determine context from marker \"~.16B\"", [Byte]))
        end,

      case Type of
        null ->
          {Size, Value};

        boolean ->
          {Size, Value};

        integer ->
          case PrefixSize of
            0 ->
              {Size, Value};
            _ ->
              {PrefixSize + Size, deserialize_integer(Sock, Timeout, Size)}
          end;

        float ->
          {PrefixSize + Size, deserialize_float(Sock, Timeout, Size)};

        string ->
          {PrefixSize + Size, deserialize_string(Sock, Timeout, Size)};

        map ->
          {MapSize, List} = deserialize_map_pairs(Sock, Timeout, Size),
          Map = maps:from_list(List),
          {PrefixSize + MapSize, Map};

        list ->
          {ListSize, List} = deserialize_list_elements(Sock, Timeout, Size),
          {PrefixSize + ListSize, List};

        struct ->
          throw({not_implemented, io:format("embedded struct with ~.10B fields", [Size])})
      end;

    {error, timeout} ->
      throw({timeout, io:format("Timeout while waiting for deserealizing object: After ~.10B ms", [Timeout])});

    Error ->
      throw({read_error, Error})
  end.

%% @private
%% @doc Deserialize an individual integer of known size
deserialize_integer(Sock, Timeout, Size) ->
  case gen_tcp:recv(Sock, Size, Timeout) of
    {ok, <<Integer:Size/big-signed-integer>>} ->
      Integer;

    {error, timeout} ->
      throw({timeout, io:format("Timeout while waiting for integer: After ~.10B ms", [Timeout])});

    Error ->
      throw({read_error, Error})
  end.

%% @private
%% @doc Deserialize an individual floating point number
deserialize_float(Sock, Timeout, Size) ->
  Bits = Size * 8,
  case gen_tcp:recv(Sock, Size, Timeout) of
    {ok, <<Float:Bits/float-unit:1>>} ->
      Float;

    {error, timeout} ->
      throw({timeout, io:format("Timeout while waiting for floating point number: After ~.10B ms", [Timeout])});

    Error ->
      throw({read_error, Error})
  end.

%% @private
%% @doc Deserialize an individual string of known length
deserialize_string(Sock, Timeout, Size) ->
  case gen_tcp:recv(Sock, Size, Timeout) of
    {ok, <<Bin/binary>>} ->
      case unicode:characters_to_list(Bin) of
        {error, _S, _Rest} -> throw({invalid_string, Bin});
        {incomplete, _S, _Rest} -> throw({invalid_string, Bin});
        String -> String
      end;

    {error, timeout} ->
      throw({timeout, io:format("Timeout while waiting for string: After ~.10B ms", [Timeout])});

    Error ->
      throw({read_error, Error})
  end.


%% @private
%% @doc Deserialize (key, value) pairs from a map of known size/length
deserialize_map_pairs(_Sock, _Timeout, 0) -> {0, []};
deserialize_map_pairs(Sock, Timeout, Count) ->
  {KeySize, Key} = deserialize_object(Sock, Timeout), % should be a string
  {ValueSize, Value} = deserialize_object(Sock, Timeout),
  {SizeOfRest, Rest} = deserialize_map_pairs(Sock, Timeout, Count - 1), % currently not tail recursion
  {KeySize + ValueSize + SizeOfRest, [{list_to_atom(Key), Value}] ++ Rest}.


%% @private
%% @doc Deserialize a elements from a list of known size/length
deserialize_list_elements(_Sock, _Timeout, 0) -> {0, []};
deserialize_list_elements(Sock, Timeout, Count) ->
  {ElementSize, Element} = deserialize_object(Sock, Timeout),
  {SizeOfRest, Rest} = deserialize_list_elements(Sock, Timeout, Count - 1), % currently not tail recursion
  {ElementSize + SizeOfRest, [Element] ++ Rest}.

