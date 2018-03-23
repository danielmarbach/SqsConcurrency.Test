using System;
using NServiceBus;

public class MyMessage :
    IMessage
{
    public long Attempt { get; set; }
    public long Batch { get; set; }
    public DateTime SentAt { get; set; }
}