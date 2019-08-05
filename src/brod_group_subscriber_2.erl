-module(brod_group_subscriber_2).
-behaviour(gen_server).

%% API.
-export([start_link/7, start_link/8]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state,
        { client
        , client_mref
        , groupId
        , memberId
        , generationId
        , coordinator
        , consumers = []
        , consumer_config
        , is_blocked = false
        , subscribe_tref
        , cb_module
        , cb_state
        , message_type
        }).

-type cb_state() :: term().

%% Initialize the callback module s state.
-callback init(brod:group_id(), term()) -> {ok, cb_state()}.

%% Handle a message. Return one of:
%%
%% {ok, NewCallbackState}:
%%   The subscriber has received the message for processing async-ly.
%%   It should call brod_group_subscriber:ack/4 to acknowledge later.
%%
%% {ok, ack, NewCallbackState}
%%   The subscriber has completed processing the message.
%%
%% {ok, ack_no_commit, NewCallbackState}
%%   The subscriber has completed processing the message, but it
%%   is not ready to commit offset yet. It should call
%%   brod_group_subscriber:commit/4 later.
%%
%% While this callback function is being evaluated, the fetch-ahead
%% partition-consumers are fetching more messages behind the scene
%% unless prefetch_count and prefetch_bytes are set to 0 in consumer config.
%%
-callback handle_message(brod:topic(),
                         brod:partition(),
                         brod:message() | brod:message_set(),
                         cb_state()) -> {ok, cb_state()} |
                                        {ok, ack, cb_state()} |
                                        {ok, ack_no_commit, cb_state()}.

-callback handle_info(term(), cb_state()) -> {ok, cb_state()}.

%% API.

-spec start_link(brod:client(), brod:group_id(), [brod:topic()],
                 brod:group_config(), brod:consumer_config(),
                 module(), term()) -> {ok, pid()} | {error, any()}.
start_link(Client, GroupId, Topics, GroupConfig,
           ConsumerConfig, CbModule, CbInitArg) ->
  brod_group_subscriber:start_link(Client, GroupId, Topics, GroupConfig,
                                   ConsumerConfig, CbModule, CbInitArg).

-spec start_link(brod:client(), brod:group_id(), [brod:topic()],
                 brod:group_config(), brod:consumer_config(),
                 message | message_set,
                 module(), term()) -> {ok, pid()} | {error, any()}.
start_link(Client, GroupId, Topics, GroupConfig, ConsumerConfig, MessageType, CbModule, CbInitArg) ->
    brod_group_subscriber:start_link(Client, GroupId, Topics, GroupConfig,
                                     ConsumerConfig, MessageType, CbModule, CbInitArg).

%% gen_server.

init(Arg) ->
    %% Check that the state format is still up to date.
	{ok, #state{cb_module = CbModule, cb_state = CbState} = State} = brod_group_subscriber:init(Arg),
    {ok, CbState} = CbModule:handle_info(validate, CbState),
    {ok, State}.

handle_call(Request, From, State) ->
	brod_group_subscriber:handle_call(Request, From, State).

handle_cast(Msg, State) ->
	brod_group_subscriber:handle_cast(Msg, State).

handle_info({cb_info, Info}, #state{cb_module = CbModule, cb_state = CbState} = State) ->
    {ok, NewCbState} = CbModule:handle_info(Info, CbState),
    {noreply, State#state{cb_state = NewCbState}};
handle_info(Info, State) ->
	brod_group_subscriber:handle_info(Info, State).

terminate(Reason, State) ->
	brod_group_subscriber:terminate(Reason, State).

code_change(OldVsn, State, Extra) ->
	brod_group_subscriber:code_change(OldVsn, State, Extra).
