-module(consumer_subscriber).
-include_lib("brod/include/brod.hrl"). %% needed for the #kafka_message record definition

-behaviour(brod_group_subscriber_2).

-export([start_subscriber/3]).
-export([init/2, handle_message/4, handle_info/2]). %% callback api


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

start_subscriber(GroupId, Filters, BatchSize) ->
    Client = kafka_client:get_client(GroupId),
    Topics = filters:get_topics(Filters),
    %% TODO: review config
    GroupConfig = [],
    %% TODO: prefetch_count is 10 by default.
    %% It does not make much sense yet, because we're acking every message.
    ConsumerConfig = [],
    brod_group_subscriber_2:start_link(
      Client, GroupId, Topics, GroupConfig,
      ConsumerConfig, ?MODULE, #{receiver => self(),
                                 target_batch_size => BatchSize,
                                 filters => Filters}).

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

%% brod_group_subscriber behaviour callback
%% TODO: alternatively this process may just send filtered data to the consumer mailbox
%% up to the batch size and let the consumer ack or confirm the messages.
%% In that case the consumer may control the frequency and filter will fill up to N messages in the
%% consumer message box.
%% Ack would mean "give more messages", confirm - commit offset and give more messages
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
  {ok, process_delayed_messages(State2)}.

init_batch_id() ->
  {make_ref(), 1}.

new_batch_id({Ref, Id}) ->
  {Ref, Id + 1}.

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
    true  -> maybe_send_batch(State1#state{delayed = Delayed});
    false -> process_delayed_messages(State1#state{delayed = Delayed})
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
  case message_filters:filter_message(Message, Filters) of
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
send_batch(#state{sent_batch_id = _SentBatchId} = State) ->
  State#state{buffer_full = true}.


