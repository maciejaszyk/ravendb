var api = "http://localhost:5000/api/Streaming/Stream ";

var cts = new CancellationTokenSource();
//var cts = new CancellationToken();
using (var httpClient = new HttpClient())
{
    var tasks = new List<Task>();
    for (int j = 0; j < 100; ++j)
    {
        var c = j;
        tasks.Add(Task.Run(async () =>
        {
            Console.WriteLine($"Created {c}");
            var request = httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Post, api));
            var result = request.GetAwaiter().GetResult();

            var reader = result.Content.ReadAsStream();
            var buffer = new byte[64 * 1024];
            //
            while (true)
            {
                await reader.ReadAsync(buffer, cts.Token);
                cts.Token.ThrowIfCancellationRequested();
            }
        }, cts.Token));
    }

    while (true)
    {
        var q = Console.ReadLine();
        if (string.Equals(q, "q"))
        {
            Console.WriteLine($"started");
            cts.Cancel();
            await Task.WhenAll(tasks);
            break;
        }
    }
}