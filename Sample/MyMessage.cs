using NServiceBus;

public class MyMessage :
    IMessage
{
    public long Attempt { get; set; }
}