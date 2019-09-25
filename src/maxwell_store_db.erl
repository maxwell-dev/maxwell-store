%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Apr 2018 8:34 PM
%%%-------------------------------------------------------------------
-module(maxwell_store_db).

%% API
-export([
  open/2,
  close/1,
  create_topic/2,
  delete_topic/2,
  seek_first_topic/1,
  seek_last_topic/1,
  seek_prev_topic/2,
  seek_next_topic/2,
  put/4,
  put/5,
  put_batch/3,
  delete/3,
  delete_batch/4,
  delete_to/4,
  delete_range/4,
  get/3,
  get_from/4,
  seek_min_offset/2,
  seek_max_offset/2,
  seek_min_offset_since/3,
  seek_max_offset_until/3,
  foreach/2
]).

-define(INFO_TOPIC_ID, 0).
-define(MAX_ITEM_ID, 65534).
-define(SEED_ITEM_ID, 0).

-define(NAME_TO_ID_TOPIC_ID, 1).
-define(ID_TO_NAME_TOPIC_ID, 2).
-define(MAX_TOPIC_NAME_SIZE, 256).

-define(MIN_USERLAND_TOPIC_ID, 1024).
-define(MAX_USERLAND_TOPIC_ID, 4294967294).

-define(MIN_OFFSET, 0).
-define(MAX_OFFSET, 18446744073709551614).

%%%===================================================================
%%% API
%%%===================================================================

open(Dir, Spec) ->
  rocksdb:open(Dir, Spec).

close(DbRef) ->
  rocksdb:close(DbRef).

create_topic(DbRef, TopicName) ->
  validate_topic_name(TopicName),
  case rocksdb:get(DbRef, pack_name_to_id_key(TopicName), []) of
    {ok, PackedValue} -> 
      {ok, unpack_name_to_id_value(PackedValue)};
    not_found -> 
        case generate_next_topic_id(DbRef) of
          {ok, TopicId} -> register_topic(DbRef, TopicName, TopicId);
          Error -> Error
        end;
    Error -> Error
  end.

delete_topic(DbRef, TopicName) ->
  PackedKey = pack_name_to_id_key(TopicName), 
  case rocksdb:get(DbRef, PackedKey, []) of
    {ok, PackedValue} -> 
      TopicId = unpack_name_to_id_value(PackedValue),
      case delete_all_entries(DbRef, TopicId) of
        ok -> unregister_topic(DbRef, TopicName, TopicId);
        Error -> Error
      end;
    not_found -> ok;
    Error -> Error
  end.

seek_first_topic(DbRef) ->
  {ok, Iter} = new_iterator(DbRef),
  try
    Result = rocksdb:iterator_move(
      Iter, pack_id_to_name_key(?MIN_USERLAND_TOPIC_ID)),
    case Result of
      {ok, PackedKey, PackedValue} -> 
        TopicId = extract_right_topic_id(PackedKey),
        {ok, unpack_id_to_name_value(PackedValue), TopicId};
      {error, invalid_iterator} -> not_found;
      Error -> Error
    end
  after
    rocksdb:iterator_close(Iter)
  end.

seek_last_topic(DbRef) ->
  {ok, Iter} = new_iterator(DbRef),
  try
    PackedKey = pack_id_to_name_key(?MAX_USERLAND_TOPIC_ID),
    case rocksdb:iterator_move(Iter, {seek_for_prev, PackedKey}) of
      {ok, PackedKey2, PackedValue2} ->
        {ok, unpack_id_to_name_value(PackedValue2),
          extract_right_topic_id(PackedKey2)};
      {error, invalid_iterator} -> not_found;
      Error -> Error
    end
  after
    rocksdb:iterator_close(Iter)
  end.

seek_prev_topic(DbRef, TopicId) ->  
  {ok, Iter} = new_iterator(DbRef),
  try
    Result = rocksdb:iterator_move(
      Iter, {seek_for_prev, pack_id_to_name_key(TopicId)}),
    case Result of
      {ok, PackedKey, PackedValue} ->
        FoundTopicId = extract_right_topic_id(PackedKey),
        case FoundTopicId < TopicId of
          true -> {ok, unpack_id_to_name_value(PackedValue), FoundTopicId};
          false -> 
            case rocksdb:iterator_move(Iter, prev) of 
              {ok, PackedKey2, PackedValue2} ->
                {ok, unpack_id_to_name_value(PackedValue2), 
                  extract_right_topic_id(PackedKey2)};
              {error, invalid_iterator} -> not_found;
              Error -> Error
            end
        end;
      {error, invalid_iterator} -> not_found;
      Error -> Error
    end
  after
    rocksdb:iterator_close(Iter)
  end.

seek_next_topic(DbRef, TopicId) ->
  {ok, Iter} = new_iterator(DbRef),
  try
    case rocksdb:iterator_move(Iter, pack_id_to_name_key(TopicId)) of
      {ok, PackedKey, PackedValue} ->
        FoundTopicId = extract_right_topic_id(PackedKey),
        case FoundTopicId > TopicId of
          true -> {ok, unpack_id_to_name_value(PackedValue), FoundTopicId};
          false -> 
            case rocksdb:iterator_move(Iter, next) of 
              {ok, PackedKey2, PackedValue2} ->
                {ok, unpack_id_to_name_value(PackedValue2), 
                  extract_right_topic_id(PackedKey2)};
              {error, invalid_iterator} -> not_found;
              Error -> Error
            end
        end;
      {error, invalid_iterator} -> not_found;
      Error -> Error        
    end
  after
    rocksdb:iterator_close(Iter)
  end.

put(DbRef, TopicId, Offset, Value) ->
  put(DbRef, TopicId, Offset, Value, get_current_timestamp()).

put(DbRef, TopicId, Offset, Value, Timestamp) ->
  validate_offset(Offset),
  rocksdb:put(
    DbRef, 
    pack_userland_key(TopicId, Offset), 
    pack_userland_value(Value, Timestamp), 
    []
  ).

put_batch(DbRef, TopicId, Entries) ->
  {ok, Batch} = rocksdb:batch(),
  try
    lists:foreach(fun(Entry) -> 
        case Entry of
          {Offset, Value, Timestamp} ->
            validate_offset(Offset),
            rocksdb:batch_put(
              Batch, 
              pack_userland_key(TopicId, Offset), 
              pack_userland_value(Value, Timestamp)
            );
          {Offset, Value} ->
            validate_offset(Offset),
            rocksdb:batch_put(
              Batch, 
              pack_userland_key(TopicId, Offset), 
              pack_userland_value(Value)
            )
        end
      end, Entries
    ),
    rocksdb:write_batch(DbRef, Batch, [])
  after
    rocksdb:release_batch(Batch)
  end.

delete(DbRef, TopicId, Offset) ->
  rocksdb:delete(DbRef, pack_userland_key(TopicId, Offset), []).

delete_batch(DbRef, TopicId, FromOffset, ToOffset) ->
  {ok, Batch} = rocksdb:batch(),
  try
    build_delete_batch(Batch, TopicId, FromOffset, ToOffset),
    rocksdb:write_batch(DbRef, Batch, [])
  after
    rocksdb:release_batch(Batch)
  end.

delete_to(DbRef, TopicId, ToOffset, Limit) ->
  validate_limit(Limit),
  MinResult = seek_min_offset(DbRef, TopicId),
  MaxResult = seek_max_offset(DbRef, TopicId),
  case MinResult =:= undefined orelse MaxResult =:= undefined of
    true -> ok;
    false ->
      {ok, MinOffset} = MinResult,
      {ok, MaxOffset} = MaxResult,
      case ToOffset < MinOffset of
        true -> ok;
        false ->
          ToOffset2 = case ToOffset > MaxOffset of 
            true -> MaxOffset;
            false -> ToOffset
          end,
          ToOffset3 = MinOffset + Limit,
          ToOffset4 = case ToOffset2 > ToOffset3 of
            true -> ToOffset3;
            false -> ToOffset2
          end,
          delete_batch(DbRef, TopicId, MinOffset, ToOffset4)
      end
  end.

% Removes the database entries in the range ["BeginKey", "EndKey"), i.e.,
%% including "BeginKey" and excluding "EndKey". Returns OK on success, and
%% a non-OK status on error. It is not an error if no keys exist in the range
%% ["BeginKey", "EndKey").
delete_range(DbRef, TopicId, FromOffset, ToOffset) ->
  rocksdb:delete_range(
    DbRef, 
    pack_userland_key(TopicId, FromOffset), 
    pack_userland_key(TopicId, ToOffset), 
    []
  ).

get(DbRef, TopicId, Offset) ->
  case rocksdb:get(DbRef, pack_userland_key(TopicId, Offset), []) of
    {ok, PackedValue} -> {ok, unpack_userland_value(PackedValue)};
    Other -> Other
  end.

get_from(DbRef, TopicId, FromOffset, Limit) ->
  validate_limit(Limit),
  {ok, Iter} = new_iterator(DbRef),
  try get_from2(Iter, TopicId, FromOffset, Limit) of
    Entries -> lists:reverse(Entries)
  catch
    _:_ = Any -> {error, Any}
  after
    rocksdb:iterator_close(Iter)
  end.

seek_min_offset(DbRef, TopicId) ->
  {ok, Iter} = new_iterator(DbRef),
  try
    case rocksdb:iterator_move(Iter, pack_min_userland_key(TopicId)) of
      {ok, PackedKey, _} -> {ok, extract_right_offset(PackedKey)};
      {error, invalid_iterator} -> undefined;
      Error -> Error 
    end
  after
    rocksdb:iterator_close(Iter)
  end.

seek_max_offset(DbRef, TopicId) ->
  {ok, Iter} = new_iterator(DbRef),
  try
    Result = rocksdb:iterator_move(
      Iter, {seek_for_prev, pack_max_userland_key(TopicId)}),
    case Result of
      {ok, PackedKey, _} -> {ok, extract_right_offset(PackedKey)};
      {error, invalid_iterator} -> undefined;
      Error -> Error
    end
  after
    rocksdb:iterator_close(Iter)
  end.

seek_min_offset_since(DbRef, TopicId, Timestamp) ->
  {ok, Iter} = new_iterator(DbRef),
  try seek_min_offset_since2(Iter, TopicId, Timestamp) of
    Result -> Result
  catch
    _:_ = Any -> {error, Any}
  after
    rocksdb:iterator_close(Iter)
  end.

seek_max_offset_until(DbRef, TopicId, Timestamp) ->
  {ok, Iter} = new_iterator(DbRef),
  try seek_max_offset_until2(Iter, TopicId, Timestamp) of
    Result -> Result
  catch
    _:_ = Any -> {error, Any}
  after
    rocksdb:iterator_close(Iter)
  end.

foreach(DbRef, Callback) ->
  rocksdb:fold(
    DbRef,
    fun({PackedKey, PackedValue}, Acc) ->
      {TopicId, Rest} = unpack_key(PackedKey),
      case is_userland_topic_id(TopicId) of
        true -> 
          Offset = unpack_offset(Rest),
          {TopicId, Offset} = unpack_userland_key(PackedKey),
          {Value, Timestamp} = unpack_userland_value(PackedValue),
          Callback(TopicId, {Offset, Value, Timestamp}),
          Acc;
        false -> Acc
      end
    end, [], []),
  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

generate_next_topic_id(DbRef) ->
  PackedKey = pack_info_key(?SEED_ITEM_ID),
  case rocksdb:get(DbRef, PackedKey, []) of
    {ok, PackedValue} -> 
      CurrValue = unpack_seed_value(PackedValue),
      NextValue = CurrValue + 1,
      case rocksdb:put(DbRef, PackedKey, pack_seed_value(NextValue), []) of 
        ok -> {ok, NextValue};
        Error -> Error
      end;
    not_found ->
      NextValue = ?MIN_USERLAND_TOPIC_ID,
      case rocksdb:put(DbRef, PackedKey, pack_seed_value(NextValue), []) of 
        ok -> {ok, NextValue};
        Error -> Error
      end;
    Error -> Error
  end.

register_topic(DbRef, TopicName, TopicId) ->
  {ok, Batch} = rocksdb:batch(),
  try
    rocksdb:batch_put(
      Batch, pack_name_to_id_key(TopicName), pack_name_to_id_value(TopicId)
    ),
    rocksdb:batch_put(
      Batch, pack_id_to_name_key(TopicId), pack_id_to_name_value(TopicName)
    ),
    case rocksdb:write_batch(DbRef, Batch, []) of
      ok -> {ok, TopicId};
      Error -> Error
    end
  after
    rocksdb:release_batch(Batch)
  end.

unregister_topic(DbRef, TopicName, TopicId) -> 
  {ok, Batch} = rocksdb:batch(),
  try
    rocksdb:batch_delete(Batch, pack_name_to_id_key(TopicName)),
    rocksdb:batch_delete(Batch, pack_id_to_name_key(TopicId)),
    rocksdb:write_batch(DbRef, Batch, [])
  after
    rocksdb:release_batch(Batch)
  end.

delete_all_entries(DbRef, TopicId) ->
  rocksdb:delete_range(
    DbRef, 
    pack_min_userland_key(TopicId), 
    pack_userland_key(TopicId, ?MAX_OFFSET + 1), 
    []
  ).

build_delete_batch(Batch, TopicId, ToOffset, ToOffset) ->
  rocksdb:batch_delete(Batch, pack_userland_key(TopicId, ToOffset));
build_delete_batch(Batch, TopicId, FromOffset, ToOffset) ->
  rocksdb:batch_delete(Batch, pack_userland_key(TopicId, FromOffset)),
  build_delete_batch(Batch, TopicId, FromOffset + 1, ToOffset).

validate_topic_name(TopicName) ->
  Size = byte_size(TopicName),
  case Size > ?MAX_TOPIC_NAME_SIZE of
    false -> pass;
    true -> erlang:error({too_long_name, Size})
  end.

validate_offset(Offset) ->
  case Offset < ?MIN_OFFSET orelse Offset > ?MAX_OFFSET of
    false -> pass;
    true -> erlang:error({out_of_range, ?MIN_OFFSET, ?MAX_OFFSET})
  end.

validate_limit(Limit) ->
  case Limit =< 0 of
    false -> pass;
    true -> erlang:error({invalid_limit, Limit})
  end.

get_from2(Iter, TopicId, Offset, Limit) ->
  case seek_entry(Iter, TopicId, Offset) of
    {error, invalid_iterator} -> [];
    {ok, Entry} ->
      case Limit > 1 of
        false -> [Entry];
        true -> get_from3(Iter, TopicId, Limit - 1, [Entry])
      end
  end.

get_from3(Iter, TopicId, 1, Entries) ->
  case seek_next_entry(Iter, TopicId) of
    {error, invalid_iterator} -> Entries;
    {ok, NextEntry} -> [NextEntry | Entries]
  end;
get_from3(Iter, TopicId, Limit, Entries) ->
  case seek_next_entry(Iter, TopicId) of
    {error, invalid_iterator} -> Entries;
    {ok, NextEntry} -> 
      get_from3(Iter, TopicId, Limit - 1, [NextEntry | Entries])
  end.

seek_min_offset_since2(Iter, TopicId, Timestamp) ->
  case seek_last_entry(Iter, TopicId) of
    {ok, {LastOffset, _, LastTimestamp}} ->
      case LastTimestamp >= Timestamp of
        true -> seek_min_offset_since3(Iter, TopicId, Timestamp, LastOffset);
        false -> undefined %% not match at all
      end;
    {error, invalid_iterator} -> undefined;
    Error -> Error 
  end.

seek_min_offset_since3(Iter, TopicId, Timestamp, CurrOffset) ->
  case seek_prev_entry(Iter, TopicId) of
    {ok, {PrevOffset, _, PrevTimestamp}} ->
      case PrevTimestamp >= Timestamp of
        true -> seek_min_offset_since3(Iter, TopicId, Timestamp, PrevOffset);
        false -> {ok, CurrOffset} %% hit
      end;
    {error, invalid_iterator} -> {ok, CurrOffset}; %% hit
    Error -> Error 
  end.

seek_max_offset_until2(Iter, TopicId, Timestamp) ->
  case seek_first_entry(Iter, TopicId) of
    {ok, {FirstOffset, _, FirstTimestamp}} ->
      case FirstTimestamp =< Timestamp of
        true -> seek_max_offset_until3(Iter, TopicId, Timestamp, FirstOffset);
        false -> undefined %% not match at all
      end;
    {error, invalid_iterator} -> undefined;
    Error -> Error 
  end.

seek_max_offset_until3(Iter, TopicId, Timestamp, CurrOffset) ->
  case seek_next_entry(Iter, TopicId) of
    {ok, {NextOffset, _, NextTimestamp}} ->
      case NextTimestamp =< Timestamp of
        true -> seek_max_offset_until3(Iter, TopicId, Timestamp, NextOffset);
        false -> {ok, CurrOffset} %% hit
      end;
    {error, invalid_iterator} -> {ok, CurrOffset}; %% hit
    Error -> Error 
  end.

seek_first_entry(Iter, TopicId) ->
  PackedKey = pack_min_userland_key(TopicId),
  case rocksdb:iterator_move(Iter, PackedKey) of
    {ok, PackedKey2, PackedValue2} ->
      {Value, Timestamp} = unpack_userland_value(PackedValue2),
      {ok, {extract_right_offset(PackedKey2), Value, Timestamp}};
    {error, invalid_iterator} = Error -> Error;
    Error -> Error 
  end.

seek_last_entry(Iter, TopicId) ->
  PackedKey = pack_max_userland_key(TopicId),
  case rocksdb:iterator_move(Iter, {seek_for_prev, PackedKey}) of
    {ok, PackedKey2, PackedValue2} ->
      {Value, Timestamp} = unpack_userland_value(PackedValue2),
      {ok, {extract_right_offset(PackedKey2), Value, Timestamp}};
    {error, invalid_iterator} = Error -> Error;
    Error -> Error 
  end.

seek_prev_entry(Iter, _TopicId) ->
  case rocksdb:iterator_move(Iter, prev) of
    {ok, PackedKey, PackedValue} ->
      {Value, Timestamp} = unpack_userland_value(PackedValue),
      {ok, {extract_right_offset(PackedKey), Value, Timestamp}};
    {error, invalid_iterator} = Error -> Error;
    Error -> Error 
  end.

seek_next_entry(Iter, _TopicId) ->
  case rocksdb:iterator_move(Iter, next) of
    {ok, PackedKey, PackedValue} ->
      {Value, Timestamp} = unpack_userland_value(PackedValue),
      {ok, {extract_right_offset(PackedKey), Value, Timestamp}};
    {error, invalid_iterator} = Error -> Error;
    Error -> Error 
  end.

seek_entry(Iter, TopicId, Offset) -> %% current or next entry
  PackedKey = pack_userland_key(TopicId, Offset),
  case rocksdb:iterator_move(Iter, PackedKey) of
    {ok, PackedKey2, PackedValue2} ->
      {Value, Timestamp} = unpack_userland_value(PackedValue2),
      {ok, {extract_right_offset(PackedKey2), Value, Timestamp}};
    {error, invalid_iterator} = Error -> Error;
    Error -> Error
  end.

pack_info_key(ItemId) ->
  <<?INFO_TOPIC_ID:8/big-unsigned-integer-unit:4, 
    ItemId:8/big-unsigned-integer-unit:2>>.

pack_seed_value(Value) ->
  <<Value:8/big-unsigned-integer-unit:4>>.

unpack_seed_value(PackedValue) ->
  <<Value:8/big-unsigned-integer-unit:4>> = PackedValue,
  Value.

pack_name_to_id_key(TopicName) ->
  <<?NAME_TO_ID_TOPIC_ID:8/big-unsigned-integer-unit:4, 
    TopicName/binary>>.

pack_name_to_id_value(TopicId) ->
  <<TopicId:8/big-unsigned-integer-unit:4>>.

unpack_name_to_id_value(PackedValue) ->
  <<TopicId:8/big-unsigned-integer-unit:4>> = PackedValue,
  TopicId.

pack_id_to_name_key(TopicId) ->
  <<?ID_TO_NAME_TOPIC_ID:8/big-unsigned-integer-unit:4, 
    TopicId:8/big-unsigned-integer-unit:4>>.

pack_id_to_name_value(TopicName) ->
  <<TopicName/binary>>.

unpack_id_to_name_value(PackedValue) -> 
  <<TopicName/binary>> = PackedValue,
  TopicName.

pack_userland_key(TopicId, Offset) ->
  <<TopicId:8/big-unsigned-integer-unit:4, 
    Offset:8/big-unsigned-integer-unit:8>>.

unpack_userland_key(PackedKey) ->
  <<TopicId:8/big-unsigned-integer-unit:4, 
    Offset:8/big-unsigned-integer-unit:8>> = PackedKey,
  {TopicId, Offset}.

pack_userland_value(Value) ->
  pack_userland_value(Value, get_current_timestamp()).

pack_userland_value(Value, Timestamp) ->
  <<Timestamp:8/big-unsigned-integer-unit:8, Value/binary>>.

unpack_userland_value(PackedValue) ->
  <<Timestamp:8/big-unsigned-integer-unit:8, Value/binary>> = PackedValue,
  {Value, Timestamp}.

pack_min_userland_key(TopicId) ->
  pack_userland_key(TopicId, ?MIN_OFFSET).

pack_max_userland_key(TopicId) ->
  pack_userland_key(TopicId, ?MAX_OFFSET).

unpack_key(PackedKey) ->
  <<TopicId:8/big-unsigned-integer-unit:4, Rest/binary>> = PackedKey,
  {TopicId, Rest}.

unpack_offset(PackedOffset) -> 
  <<Offset:8/big-unsigned-integer-unit:8>> = PackedOffset,
  Offset.

is_userland_topic_id(TopicId) ->
  TopicId >= ?MIN_USERLAND_TOPIC_ID andalso TopicId =< ?MAX_USERLAND_TOPIC_ID. 

extract_right_topic_id(PackedKey) ->
  <<?ID_TO_NAME_TOPIC_ID:8/big-unsigned-integer-unit:4, 
    TopicId:8/big-unsigned-integer-unit:4>> = PackedKey,
  TopicId.

extract_right_offset(PackedKey) ->
  <<_:8/big-unsigned-integer-unit:4, 
    Offset:8/big-unsigned-integer-unit:8>> = PackedKey,
  Offset.

new_iterator(DbRef) ->
  rocksdb:iterator(DbRef, [
    {prefix_same_as_start, true}, {ignore_range_deletions, true}
  ]).

get_current_timestamp() ->
  {MegaSec, Sec, MicroSec} = os:timestamp(),
  1000000000 * MegaSec + Sec * 1000 + erlang:trunc(MicroSec / 1000).