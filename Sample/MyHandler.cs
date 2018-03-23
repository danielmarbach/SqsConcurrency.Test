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
        if (Program.sentAndReceived.TryRemove(message.Attempt, out var sentAt))
        {
            Program.stats.Add(new Program.StatsEntry(message.Attempt, sentAt.batch, sentAt.sentAt, DateTime.UtcNow));    
        }
        
        return Task.CompletedTask;
    }
}