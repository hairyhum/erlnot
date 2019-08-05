-module(batch_receive).

-export([get_batch/0, confirm_batch/1]).

get_batch(_Subscriber) ->
    receive {BatchId, BatchData, BatchOffset} ->
        {BatchId, BatchData, BatchOffset}
    end.

confirm_batch(Subscriber, BatchId) ->
    Subscriber ! {cb_info, {ack_batch, BatchId}}.

