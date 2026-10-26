using ToyStream.Protocol;
using ToyStream.Protocol.Messages;
using ToyStream.Protocol.Responses;
using ToyStream.Shared;

namespace ToyStream.Server;

public readonly struct FetchHandler(ITopicService topicService) :
    IRequestHandler<FetchRequestMessage, FetchResponseMessage>
{
    /// <summary>
    /// Answers without parking when there is data. Records are pulled through the plan as the body
    /// is streamed, so storage never materialises more than the record currently going out.
    /// </summary>
    public ValueTask DispatchRequest(FetchRequestMessage message,
        ResponseWriter<FetchResponseMessage> responseWriter,
        CancellationToken cancellationToken)
    {
        var maxBytes = Math.Max(message.MaxBytes, 0);
        var plan = FetchPlan.Rent(message.Topics.Count, message.Entries.Count);
        var transferred = false;
        try
        {
            var failure = plan.Resolve(topicService, message);
            if (failure != FetchResult.Success)
            {
                responseWriter.WriteResponse(new FetchResponseMessage { Result = failure, Topics = default });

                // An empty body still follows, so a reader has one shape to handle rather than two.
                return CompleteEmptyBody(responseWriter.BeginBody(), cancellationToken);
            }

            // The plan owns the rented buffers from here: the request's views die at the first await.
            transferred = true;
            return plan.HasRecords() || message.MaxWaitMilliseconds <= 0
                ? RespondAndReturn(plan, maxBytes, responseWriter, cancellationToken)
                : WaitAndRespond(plan, maxBytes, message.MaxWaitMilliseconds, responseWriter, cancellationToken);
        }
        finally
        {
            if (!transferred)
            {
                plan.Return();
            }
        }
    }

    private static async ValueTask CompleteEmptyBody(MessageBodyWriter body, CancellationToken cancellationToken)
    {
        try
        {
            await new FetchedRecordWriter(body).CompleteAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            body.Complete();
        }
    }

    private static async ValueTask RespondAndReturn(FetchPlan plan, int maxBytes,
        ResponseWriter<FetchResponseMessage> responseWriter, CancellationToken cancellationToken)
    {
        try
        {
            await plan.WriteResponse(responseWriter, maxBytes, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            plan.Return();
        }
    }

    private static async ValueTask WaitAndRespond(FetchPlan plan, int maxBytes, int maxWaitMilliseconds,
        ResponseWriter<FetchResponseMessage> responseWriter, CancellationToken cancellationToken)
    {
        try
        {
            await plan.WaitForAppend(maxWaitMilliseconds, cancellationToken).ConfigureAwait(false);

            // A wake says only that something changed, so the answer always comes from a re-read.
            await plan.WriteResponse(responseWriter, maxBytes, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            plan.Return();
        }
    }
}
