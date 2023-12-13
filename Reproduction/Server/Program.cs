using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseKestrel(ConfigureKestrel);
builder.Logging.AddConsole(options => options.LogToStandardErrorThreshold = LogLevel.Trace) ;
builder.Logging.SetMinimumLevel(LogLevel.Trace); // Set the minimum log level to Debug

// Add services to the container.
builder.Services.Configure<FormOptions>(options => { options.MultipartBodyLengthLimit = long.MaxValue; });

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
var app = builder.Build();

app.UseAuthorization();

app.MapControllers();

app.Run();


void ConfigureKestrel(KestrelServerOptions options)
{
    options.AllowSynchronousIO = false;
    options.Limits.MaxRequestLineSize = (int) 16 * 1024; //16Kb
    options.Limits.MaxRequestBodySize = null; // no limit!
    options.Limits.MinResponseDataRate = null; // no limit!
    options.Limits.MinRequestBodyDataRate = null; // no limit!
    options.Limits.Http2.MaxStreamsPerConnection = int.MaxValue; // no limit!
    options.ConfigureEndpointDefaults(listenOptions => listenOptions.Protocols = HttpProtocols.Http1AndHttp2);
}