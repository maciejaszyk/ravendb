﻿using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Indexes.Sharding;

public class ShardedIndexStateController : AbstractIndexStateController
{
    private readonly ShardedDatabaseContext _context;

    public ShardedIndexStateController([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
        : base(serverStore)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    protected override string GetDatabaseName() => _context.DatabaseName;

    protected override void ValidateIndex(string name, IndexState state)
    {
        if (_context.Indexes.GetIndex(name) == null)
            IndexDoesNotExistException.ThrowFor(name);
    }

    protected override ValueTask WaitForIndexNotificationAsync(long index) => _context.Cluster.WaitForExecutionOnAllNodesAsync(index);
}