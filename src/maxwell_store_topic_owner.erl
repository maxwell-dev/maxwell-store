%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Apr 2018 8:56 PM
%%%-------------------------------------------------------------------
-module(maxwell_store_topic_owner).
-behaviour(gen_server).

-include("maxwell_store.hrl").

%% API
-export([
  start_link/1,
  ensure_started/1,
  put_values/2,
  put_entries/2,
  get_from/3,
  seek_max_offset/1,
  seek_min_offset_since/2,
  seek_max_offset_until/2,
  add_watcher/2
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(PROCESS_NAME(TopicName), {topic_owner, TopicName}).
-define(VIA_PROCESS_NAME(TopicName),
  {via, maxwell_store_proc_registry, ?PROCESS_NAME(TopicName)}
).
-define(MODE_AUTO, auto).
-define(MODE_MANUAL, manual).
-record(state, {db_ref, topic_name, topic_id, offset, put_mode, watchers}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(TopicName) ->
  gen_server:start_link(
    ?VIA_PROCESS_NAME(TopicName), ?MODULE, [TopicName], []
  ).

ensure_started(TopicName) ->
  case maxwell_store_proc_registry:whereis_name(?PROCESS_NAME(TopicName)) of
    undefined ->
      case maxwell_store_topic_owner_sup:start_child(TopicName) of
        {error, {already_started, Pid}} -> {ok, Pid};
        {ok, _} = Result -> Result
      end;
    Pid -> {ok, Pid}
  end.

put_values(Ref, Values) ->
  gen_server:call(server_ref(Ref), {put_values, Values}).

put_entries(Ref, Entries) ->
  gen_server:call(server_ref(Ref), {put_entries, Entries}).

get_from(Ref, Offset, Limit) ->
  gen_server:call(server_ref(Ref), {get_from, Offset, Limit}).

seek_max_offset(Ref) ->
  gen_server:call(server_ref(Ref), seek_max_offset).

seek_min_offset_since(Ref, Timestamp) ->
  gen_server:call(server_ref(Ref), {seek_min_offset_since, Timestamp}).

seek_max_offset_until(Ref, Timestamp) ->
  gen_server:call(server_ref(Ref), {seek_max_offset_until, Timestamp}).

add_watcher(Ref, Pid) ->
  gen_server:call(server_ref(Ref), {add_watcher, Pid}).

server_ref(Ref) when is_pid(Ref) -> Ref;
server_ref(Ref) -> ?VIA_PROCESS_NAME(Ref).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([TopicName]) ->
  State = init_state(TopicName),
  lager:info("Initializing ~p: state: ~p", [?MODULE, State]),
  {ok, State}.

handle_call({put_values, Values}, _From, State) ->
  reply(put_values0(Values, State));
handle_call({put_entries, Entries}, _From, State) ->
  reply(put_entries0(Entries, State));
handle_call({get_from, Offset, Limit}, _From, State) ->
  reply(get_from0(Offset, Limit, State));
handle_call(seek_max_offset, _From, State) ->
  reply(seek_max_offset0(State));
handle_call({seek_min_offset_since, Timestamp}, _From, State) ->
  reply(seek_min_offset_since0(Timestamp, State));
handle_call({seek_max_offset_until, Timestamp}, _From, State) ->
  reply(seek_max_offset_until0(Timestamp, State));
handle_call({add_watcher, Pid}, _From, State) ->
  reply(add_watcher0(Pid, State));
handle_call(Request, _From, State) ->
  lager:error("Recevied unknown request: ~p", [Request]),
  reply({ok, State}).

handle_cast(Request, State) ->
  lager:error("Recevied unknown cast: ~p", [Request]),
  noreply(State).

handle_info({'DOWN', WatcherRef, process, _, Reason}, State) ->
  lager:info(
    "Watcher was down: topic_name: ~p, watcher_ref: ~p, reason: ~p",
    [State#state.topic_name, WatcherRef, Reason]
  ),
  {noreply, on_watcher_down(WatcherRef, State)};
handle_info(Info, State) ->
  lager:error("Recevied unknown info: ~p", [Info]),
  noreply(State).

terminate(Reason, State) ->
  lager:info(
    "Terminating ~p: state: ~p, reason: ~p",
    [?MODULE, State, Reason]
  ),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_state(TopicName) ->
  {ok, DbRef} = maxwell_store_db_owner:get_ref(),
  {ok, TopicId} = maxwell_store_db_owner:create_topic(TopicName),
  Offset =
    case maxwell_store_db:seek_max_offset(DbRef, TopicId) of
      undefined -> -1;
      MaxOffset -> MaxOffset
    end,
  #state{
    db_ref = DbRef, 
    topic_name = TopicName, topic_id = TopicId,
    offset = Offset, put_mode = ?MODE_AUTO,
    watchers = dict:new()
  }.

put_values0(Values, State) ->
  CurrOffset = %% the result depends on prev put mode
  case State#state.put_mode of
    ?MODE_MANUAL -> maxwell_store_db:seek_max_offset(
      State#state.db_ref, State#state.topic_id);
    ?MODE_AUTO -> State#state.offset
  end,
  {NewOffset, NewEntries} = lists:foldl(
    fun(Value, {Offset, Entries}) ->
      NextOffset = Offset + 1,
      {NextOffset, [{NextOffset, Value} | Entries]}
    end, {CurrOffset, []}, Values
  ),
  ok = maxwell_store_db:put_batch(
    State#state.db_ref, State#state.topic_id, NewEntries
  ),
  notify(NewOffset, State),
  {ok, State#state{
    offset = NewOffset, put_mode = ?MODE_AUTO
  }}.

put_entries0(Entries, State) ->
  ok = maxwell_store_db:put_batch(
    State#state.db_ref, State#state.topic_id, Entries
  ),
  case lists:last(Entries) of
    {MaxOffset, _, _} ->
      notify(MaxOffset, State);
    {MaxOffset, _} ->
      notify(MaxOffset, State)
  end,
  {ok, State#state{put_mode = ?MODE_MANUAL}}.

get_from0(-1, Limit, State) ->
  MaxOffset = maxwell_store_db:seek_max_offset(
    State#state.db_ref, State#state.topic_id
  ),
  get_from0(MaxOffset, Limit, State);
get_from0(Offset, Limit, State) when Offset =< -2 ->
  Timestamp = get_current_timestamp() - Offset * 1000,
  MinOffset = maxwell_store_db:seek_min_offset_since(
    State#state.db_ref, State#state.topic_id, Timestamp
  ),
  get_from0(MinOffset, Limit, State);
get_from0(Offset, Limit, State) ->
  Entries = maxwell_store_db:get_from(
    State#state.db_ref, State#state.topic_id, Offset, Limit
  ),
  {Entries, State}.

seek_max_offset0(State) ->
  MaxOffset = maxwell_store_db:seek_max_offset(
    State#state.db_ref, State#state.topic_id
  ),
  {MaxOffset, State}.

seek_min_offset_since0(Timestamp, State) ->
  MinOffset = maxwell_store_db:seek_min_offset_since(
    State#state.db_ref, State#state.topic_id, Timestamp
  ),
  {MinOffset, State}.

seek_max_offset_until0(Timestamp, State) ->
  MaxOffset = maxwell_store_db:seek_max_offset_until(
    State#state.db_ref, State#state.topic_id, Timestamp
  ),
  {MaxOffset, State}.

add_watcher0(Pid, State) ->
  Ref = erlang:monitor(process, Pid),
  Watchers = dict:store(Ref, Pid, State#state.watchers),
  {ok, State#state{watchers = Watchers}}.

notify(MaxOffset, State) ->
  dict:fold(
    fun(_, Pid, _) -> 
      Pid ! ?maxwell_store_NOTIFY_CMD(State#state.topic_name, MaxOffset) 
    end,
    undefined, State#state.watchers
  ).

on_watcher_down(WatcherRef, State) ->
  Watchers = dict:erase(WatcherRef, State#state.watchers),
  State#state{watchers = Watchers}.

reply({Reply, State}) ->
  {reply, Reply, State}.

noreply(State) ->
  {noreply, State}.

get_current_timestamp() ->
  {MegaSec, Sec, MicroSec} = os:timestamp(),
  1000000000 * MegaSec + Sec * 1000 + erlang:trunc(MicroSec / 1000).