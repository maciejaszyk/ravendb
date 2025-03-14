using System;
using NLog;
using Sparrow.Logging;
using LogLevel = Sparrow.Logging.LogLevel;

namespace Sparrow.Server.Logging;

public sealed class RavenLogger : IRavenLogger
{
    private readonly Logger _logger;

    public RavenLogger(Logger logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public bool IsErrorEnabled => false;

    public bool IsInfoEnabled => false;

    public bool IsDebugEnabled => false;

    public bool IsFatalEnabled => false;

    public bool IsTraceEnabled => false;

    public bool IsWarnEnabled => false;

    public void Error(string message)
    {
        _logger.Error(message);
    }

    public void Error(string message, Exception exception)
    {
        _logger.Error(exception, message);
    }

    public void Info(string message)
    {
        _logger.Info(message);
    }

    public void Info(string message, Exception exception)
    {
        _logger.Info(exception, message);
    }

    public void Debug(string message)
    {
        _logger.Debug(message);
    }

    public void Debug(string message, Exception exception)
    {
        _logger.Debug(exception, message);
    }
    
    public void Warn(string message)
    {
        _logger.Warn(message);
    }

    public void Warn(string message, Exception exception)
    {
        _logger.Warn(exception, message);
    }

    public void Fatal(string message)
    {
        _logger.Fatal(message);

        Console.Error.WriteLine(message);
    }

    public void Fatal(string message, Exception exception)
    {
        _logger.Fatal(exception, message);

        Console.Error.WriteLine(message + Environment.NewLine + exception);
    }

    public void Trace(string message)
    {
        _logger.Trace(message);
    }

    public void Trace(string message, Exception exception)
    {
        _logger.Trace(exception, message);
    }

    public bool IsEnabled(LogLevel logLevel)
    {
        return _logger.IsEnabled(logLevel.ToNLogLogLevel());
    }

    public void Log(LogLevel logLevel, string message)
    {
        _logger.Log(logLevel.ToNLogLogLevel(), message);
    }

    public void Log(LogLevel logLevel, string message, Exception exception)
    {
        _logger.Log(logLevel.ToNLogLogLevel(), exception, message);
    }

    public IRavenLogger WithProperty(string propertyKey, object propertyValue)
    {
        return new RavenLogger(_logger.WithProperty(propertyKey, propertyValue));
    }
}
