using System;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Logging;

public class MyHandler :
    IHandleMessages<MyMessage>
{
    static ILog log = LogManager.GetLogger<MyHandler>();

    public Task Handle(MyMessage message, IMessageHandlerContext context)
    {
        Program.received.AddOrUpdate(message.Attempt, (message.SentAt, message.Batch), (l, tuple) => (message.SentAt, message.Batch));
        Program.stats.Add(new Program.StatsEntry(message.Attempt, message.Batch, message.SentAt, DateTime.UtcNow));   
        
        return Task.CompletedTask;
    }
}