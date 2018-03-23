using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using NServiceBus;

class Program
{
    public static ConcurrentDictionary<string, (DateTime sentAt, long batch)> sentAndReceived = new ConcurrentDictionary<string, (DateTime, long)>();
    
    public static ConcurrentBag<StatsEntry> stats = new ConcurrentBag<StatsEntry>();

    public struct StatsEntry
    {
        public string Id;
        public DateTime ReceivedAt;
        public DateTime SentAt;
        public long Batch;

        public StatsEntry(string id, long batch, DateTime sentAt, DateTime receivedAt)
        {
            Id = id;
            SentAt = sentAt;
            ReceivedAt = receivedAt;
            Batch = batch;
        }
    }
    
    static async Task Main()
    {
        Console.Title = "Sqs.Concurrency.Tests";

        Console.WriteLine("Purging queues");
        var client = new AmazonSQSClient();
        try
        {
            var inputQueue = await client.GetQueueUrlAsync("Sqs-Concurrency-Tests");
            await client.PurgeQueueAsync(inputQueue.QueueUrl);
        }
        catch (QueueDoesNotExistException)
        {
        }

        Console.WriteLine("Queues purged.");
        
        var endpointConfiguration = new EndpointConfiguration("Sqs.Concurrency.Tests");
        var transport = endpointConfiguration.UseTransport<SqsTransport>();

        endpointConfiguration.SendFailedMessagesTo("error");
        endpointConfiguration.EnableInstallers();
        endpointConfiguration.UsePersistence<InMemoryPersistence>();
        endpointConfiguration.LimitMessageProcessingConcurrencyTo(32);

        var endpointInstance = await Endpoint.Start(endpointConfiguration)
            .ConfigureAwait(false);

        var cts = new CancellationTokenSource(TimeSpan.FromHours(1));
        var syncher = new TaskCompletionSource<bool>();
        
        var sendTask = Task.Run(() => Sending(endpointInstance, cts.Token, syncher), CancellationToken.None);
        var checkTask = Task.Run(() => DumpCurrentState(cts.Token), CancellationToken.None);
        
        await Task.WhenAll(sendTask, checkTask);

        await CheckState(syncher);
        
        await endpointInstance.Stop()
            .ConfigureAwait(false);

        Console.WriteLine("Press any key to exit.");
        Console.ReadKey();
    }

    static async Task Sending(IMessageSession endpointInstance, CancellationToken token, TaskCompletionSource<bool> syncher)
    {
        try
        {
            var attempt = 0L;
            var batchSend = 0L;
            var random = new Random();

            while (!token.IsCancellationRequested)
            {
                var numberOfMessagesToSend = random.Next(1, 128);
                var sendTask = new List<Task>(numberOfMessagesToSend);
                batchSend++;
                for (var i = 0; i < numberOfMessagesToSend; i++)
                {
                    async Task SendMessage(long localAttempt, long batchNr)
                    {
                        var now = DateTime.UtcNow;
                        var myMessage = new MyMessage
                        {
                            Attempt = $"MyMessageSmall/Batch {batchNr}/Attempt {localAttempt}/Sent at '{now.ToString("yyyy-MM-dd HH:mm:ss:ffffff", CultureInfo.InvariantCulture)}'"
                        };

                        await endpointInstance.SendLocal(myMessage)
                            .ConfigureAwait(false);

                        sentAndReceived.AddOrUpdate(myMessage.Attempt, (now, batchNr), (s, v) => (now, batchNr));
                    }

                    sendTask.Add(SendMessage(attempt++, batchSend));
                }

                await Task.WhenAll(sendTask);
                await Task.Delay(TimeSpan.FromSeconds(random.Next(1, 5)), token);
            }
        }
        catch (OperationCanceledException)
        {
            // ignore
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Sending error {ex.Message}. Aborting");
        }
        finally
        {
            Console.WriteLine();
            Console.WriteLine("--- Sending ---");
            Console.WriteLine("Done sending...");
            Console.WriteLine("--- Sending ---");
            syncher.TrySetResult(true);
        }
    }

    static async Task DumpCurrentState(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            Console.Clear();
            Console.WriteLine("--- Current state ---");
            if (!sentAndReceived.IsEmpty)
            {
                foreach (var entry in sentAndReceived.OrderBy(x => x.Value))
                {
                    Console.WriteLine($"'{entry.Key}'");
                }
            }
            else
            {
                Console.WriteLine("empty.");
            }
            Console.WriteLine("--- Current state ---");

            await WriteStats();
            
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(20), token);
            }
            catch (OperationCanceledException)
            {
            }
        }   
    }
    
    static async Task CheckState(TaskCompletionSource<bool> syncher)
    {
        await syncher.Task;
        
        while (!sentAndReceived.IsEmpty)
        {
            Console.Clear();
            Console.WriteLine("--- Not yet received ---");
            foreach (var entry in sentAndReceived.OrderBy(e => e.Value))
            {
                Console.WriteLine($"'{entry.Key}' to be received at '{entry.Value.sentAt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)}'");
            }

            Console.WriteLine("--- Not yet received ---");

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(20));
            }
            catch (OperationCanceledException)
            {
            }
        }
        
        await WriteStats();
        
        Console.WriteLine();
        Console.WriteLine("--- Summary ---");
        Console.WriteLine("Received everything. Done");
        Console.WriteLine("--- Summary ---");
    }

    static async Task WriteStats()
    {
        using (var writer = new StreamWriter(@".\stats.csv", false))
        {
            await writer.WriteLineAsync($"{nameof(StatsEntry.Id)},{nameof(StatsEntry.Batch)},{nameof(StatsEntry.SentAt)},{nameof(StatsEntry.ReceivedAt)},Delta");
            foreach (var statsEntry in stats.OrderBy(s => s.SentAt))
            {
                var delta = statsEntry.ReceivedAt - statsEntry.SentAt;
                await writer.WriteLineAsync($"{statsEntry.Id},{statsEntry.Batch},{statsEntry.SentAt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)},{statsEntry.ReceivedAt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)},{delta.ToString(@"hh\:mm\:ss\.fff", CultureInfo.InvariantCulture)}");
            }

            await writer.FlushAsync();
            writer.Close();
        }
    }
}