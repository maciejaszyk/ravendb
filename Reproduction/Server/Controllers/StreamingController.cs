using System.Diagnostics;
using Microsoft.AspNetCore.Mvc;

namespace Server.Controllers;

[Route("api/[controller]")]
[ApiController]
public class StreamingController : ControllerBase
{
    private readonly ILogger _logger;

    public StreamingController()
    {
    }

    void CpuUsage()
    {
        var sw = Stopwatch.StartNew();

        int i = 3;
        while (true)
        {
            Calculate(i++);

            if (sw.Elapsed > TimeSpan.FromSeconds(3))
            {
                return;
            }
        }
    }

    bool Calculate(int num)
    {
        for (int i = 2; i < Math.Sqrt(num); ++i)
        {
            if (num % i == 0)
                return false;
        }

        return true;
    }
    
    [HttpPost("Stream")]
    public async Task PostStreaming()
    {
        Console.WriteLine($"Start...");

        var textToWrite = new byte[64 * 1024];
        textToWrite.AsSpan().Fill(1);
        var responseStream = (HttpContext.Response.Body);
        try
        {
            while (HttpContext.RequestAborted.IsCancellationRequested == false)
            {
                await responseStream.WriteAsync(textToWrite, 0, textToWrite.Length);
                CpuUsage();
                HttpContext.RequestAborted.ThrowIfCancellationRequested();
                await responseStream.FlushAsync();
                
                //Console.Write(".");
            }
            Console.WriteLine($"End");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }

    }
}