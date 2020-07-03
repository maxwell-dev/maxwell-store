%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Jun 2018 3:06 PM
%%%-------------------------------------------------------------------
-module(maxwell_store_cleaner).
-behaviour(gen_server).

%% API
-export([
  start_link/0
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

-define(SERVER, ?MODULE).
-define(VIA_PROCESS_NAME(DbRef),
  {via, maxwell_store_proc_registry, {cleaner, DbRef}}
).
-define(CLEAN_CMD, '$clean').
-define(CLEAN_SLEEP, 3).

-record(state, {topic_id}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
  lager:info("Initializing ~p", [?MODULE]),
  send_cmd(?CLEAN_CMD, ?CLEAN_SLEEP),
  {ok, #state{topic_id=undefined}}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(?CLEAN_CMD, State) ->
  {noreply, clean(State)};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(Reason, _State) ->
  lager:info("Terminating ~p, reason: ~p", [?MODULE, Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

clean(State) ->
  {ok, DbRef} = maxwell_store_db_owner:get_ref(),
  NewState = select_topic(DbRef, State),
  case NewState#state.topic_id =/= undefined of
    true -> clean_topic(DbRef, NewState);
    false -> ignore 
  end,
  send_cmd(?CLEAN_CMD, maxwell_store_config:get_clean_interval() * 1000),
  NewState.

select_topic(DbRef, #state{topic_id = TopicId} = State) ->
  Result = case TopicId of
    undefined -> maxwell_store_db:seek_first_topic(DbRef);
    _ -> maxwell_store_db:seek_next_topic(DbRef, TopicId)
  end,
  case Result of 
    {ok, _, NewTopicId} -> State#state{topic_id = NewTopicId};
    _ -> State
  end.  

clean_topic(DbRef, #state{topic_id = TopicId}) ->
  case maxwell_store_db:seek_min_offset(DbRef, TopicId) of
    {ok, MinOffset} ->
      Result = maxwell_store_db:seek_max_offset_until(
        DbRef, TopicId, get_max_timestamp()),
      case Result of 
        {ok, Offset} -> try_delete_range(DbRef, TopicId, MinOffset, Offset);
        _ -> ignore
      end;
    _ -> ignore
  end.

try_delete_range(DbRef, TopicId, FromOffset, ToOffset) ->
  case ToOffset > FromOffset of
    true ->
      lager:debug(
        "Deleting: topic_id: ~p, from_offset: ~p, to_offset: ~p", 
        [TopicId, FromOffset, ToOffset]
      ),
      A = maxwell_store_db:get_from(DbRef, TopicId, FromOffset, 10),
      lager:debug(
        "get from: topic_id: ~p, from_offset: ~p, result: ~p", 
        [TopicId, FromOffset, A]
      ),
      R = maxwell_store_db:delete_range(DbRef, TopicId, FromOffset, ToOffset),
      B = maxwell_store_db:get_from(DbRef, TopicId, FromOffset, 10),
      lager:debug(
        "get from: topic_id: ~p, from_offset: ~p, result: ~p, R: ~p", 
        [TopicId, FromOffset, B, R]
      );
    false -> ignore
  end.

get_max_timestamp() ->
  get_current_timestamp() - maxwell_store_config:get_retention_age() * 1000.

get_current_timestamp() ->
  {MegaSec, Sec, MicroSec} = os:timestamp(),
  1000000000 * MegaSec + Sec * 1000 + erlang:trunc(MicroSec / 1000).

send_cmd(Cmd, DelayMs) ->
  erlang:send_after(DelayMs, self(), Cmd).