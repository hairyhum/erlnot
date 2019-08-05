-module(consumer_subscriber).
-include_lib("brod/include/brod.hrl"). %% needed for the #kafka_message record definition

-behaviour(brod_group_subscriber_2).

-export([start/1]).
-export([init/2, handle_message/4]). %% callback api


-record(state, {
  receiver,
  target_batch_size,
  filters,
  count,
  messages,
  last_offsets, %%:: #{{Topic, Partition} => Offset}
  delayed,
  buffer_full,
  sent_batch,
  sent_batch_id,
  last_batch_id
}).

%% brod_group_subscriber behaviour callback
init(_GroupId,
     #{receiver := Receiver,
       target_batch_size := BatchSize,
       filters := Filters}) ->
  {ok, #state{
    receiver = Receiver,
    target_batch_size = BatchSize,
    filters = Filters,
    count = 0,
    messages = [],
    last_offsets = #{},
    delayed = [],
    buffer_full = false,
    sent_batch = none,
    sent_batch_id = none,
    last_batch_id = init_batch_id()
  }}.

init_batch_id() ->
  {make_ref(), 1}.

new_batch_id({Ref, Id}) ->
  {Ref, Id + 1}.

%% brod_group_subscriber behaviour callback
handle_message(Topic, Partition, Message, #state{buffer_full = true, delayed = Delayed} = State) ->
  %% Do not ack the messgae. Prefetch should stop consuming at some point.
  {ok, State#state{delayed = Delayed ++ [{Topic, Partition, Message}]}};
handle_message(Topic, Partition, Message, #state{buffer_full = false} = State) ->
  #kafka_message{ offset = Offset
                , key   = Key
                , value = Value
                } = Message,
  error_logger:info_msg("~p ~p: offset:~w key:~s value:~s\n",
                        [self(), Partition, Offset, Key, Value]),
  State1 = maybe_add_message(Topic, Partition, Message, Offset, State),
  %% TODO: do we need to wait until the batch is full?
  State2 = maybe_send_batch(State1),
  {ok, ack_no_commit, State2}.

handle_info(validate, #state{} = State) ->
  {ok, State};
handle_info({ack_batch, BatchId}, #state{sent_batch_id = BatchId, sent_batch = Batch} = State) ->
  %% Commit batch offsets
  ok = commit_batch(Batch),
  %% TODO: do we need to wait until the batch is full?
  State1 = State#state{sent_batch_id = none, sent_batch = none},
  State2 = maybe_send_batch(State1),
  {ok, process_delayed_messages(State1)}.

process_delayed_messages(#state{delayed = [], sent_batch_id = BatchId} = State) ->
  %% No more delayed messages. Buffer is still full if there is a batch id
  State#state{buffer_full = (BatchId =/= none)};
process_delayed_messages(#state{delayed = [{Topic, Partition, Message} | Delayed]} = State) ->
  #kafka_message{ offset = Offset } = Message,
  State1 = maybe_add_message(Topic, Partition, Message, Offset, State),
  %% TODO: optimise acks
  brod_group_subscriber:ack(self(), Topic, Partition, Offset, false),
  case batch_ready(State1) of
    %% There is a batch already. Stop processing
    true  -> maybe_send_batch(State1);
    false -> process_delayed_messages(State1)
  end.

commit_batch({_, _, LastOffsets}) ->
  maps:map(
    fun({Topic, Partition}, Offset) ->
      ok = brod_group_subscriber:commit(self(), Topic, Partition, Offset)
    end,
    LastOffsets),
  ok.

maybe_send_batch(State) ->
  %% TODO: do we need to wait until the batch is full?
  case batch_ready(State) of
    true  -> send_batch(State);
    false -> State
  end.

maybe_add_message(Topic, Partition, Message, Offset,
                  #state{messages = Messages,
                         filters = Filters,
                         count = Count,
                         last_offsets = LastOffsets} = State) ->
  case filter_message(Message, State#state.filters) of
    %% Take messge
    true  -> State#state{messages = [Message | Messages],
                         count = Count + 1,
                         last_offsets = maps:put({Topic, Partition}, Offset, LastOffsets)};
    %% Discard message
    false -> State#state{last_offsets = maps:put({Topic, Partition}, Offset, LastOffsets)}
  end.

batch_ready(#state{count = Count, target_batch_size = TargetBatchSize}) ->
  Count >= TargetBatchSize.

send_batch(#state{messages = Messages,
                  receiver = Receiver,
                  last_offsets = LastOffsets,
                  sent_batch_id = none,
                  last_batch_id = LastBatchId} = State) ->
  BatchId = new_batch_id(LastBatchId),
  Batch = {BatchId, Messages, LastOffsets},
  Receiver ! Batch,
  State#state{sent_batch_id = BatchId,
              last_batch_id = BatchId,
              sent_batch = Batch,
              messages = [],
              count = 0,
              last_offsets = #{}};
send_batch(#state{sent_batch_id = SentBatchId}) ->
  State#state{buffer_full = true}.


%% @doc The brod client identified ClientId should have been started
%% either by configured in sys.config and started as a part of brod application
%% or started by brod:start_client/3
%% @end
-spec start(brod:client_id()) -> {ok, pid()}.
start(ClientId) ->
  Topic  = <<"brod-test-topic-1">>,
  %% commit offsets to kafka every 5 seconds
  GroupConfig = [{offset_commit_policy, commit_to_kafka_v2},
                 {offset_commit_interval_seconds, 5}
                ],
  GroupId = <<"my-unique-group-id-shared-by-all-members">>,
  ConsumerConfig = [{begin_offset, earliest}],
  brod:start_link_group_subscriber(ClientId, GroupId, [Topic],
                                   GroupConfig, ConsumerConfig,
                                   _CallbackModule  = ?MODULE,
                                   _CallbackInitArg = []).

