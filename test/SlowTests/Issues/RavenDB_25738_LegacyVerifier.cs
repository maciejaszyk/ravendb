using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_25738_LegacyVerifier : RavenTestBase
    {
        private const string SOH = "";

        public enum LegacySource
        {
            Dump,
            FullBackup,
            Snapshot
        }

        public RavenDB_25738_LegacyVerifier(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.BackupExportImport)]
        [InlineData(LegacySource.Dump)]
        [InlineData(LegacySource.FullBackup)]
        [InlineData(LegacySource.Snapshot)]
        public async Task LegacyControlCharDocumentIdShouldBeLoadableById(LegacySource source)
        {
            var docId = "companies/" + SOH + "/ctrl-1";

            using var store = OpenLegacyStore(source);
            await ImportLegacyAsync(store, source);

            using var session = store.OpenAsyncSession();
            var loaded = await session.LoadAsync<Company>(docId);
            Assert.NotNull(loaded);
            Assert.Equal("Ctrl Co", loaded.Name);
        }

        [RavenTheory(RavenTestCategory.BackupExportImport)]
        [InlineData(LegacySource.Dump)]
        [InlineData(LegacySource.FullBackup)]
        [InlineData(LegacySource.Snapshot)]
        public async Task LegacyControlCharCounterNameShouldBeLoadableByName(LegacySource source)
        {
            const string docId = "items/1";
            var counterName = "weird" + SOH + "Counter";
            const long expected = 42;

            using var store = OpenLegacyStore(source);
            await ImportLegacyAsync(store, source);

            using var session = store.OpenAsyncSession();
            var actual = await session.CountersFor(docId).GetAsync(counterName);
            Assert.NotNull(actual);
            Assert.Equal(expected, actual);
            WaitForUserToContinueTheTest(store);
        }

        [RavenTheory(RavenTestCategory.BackupExportImport)]
        [InlineData(LegacySource.Dump)]
        [InlineData(LegacySource.FullBackup)]
        [InlineData(LegacySource.Snapshot)]
        public async Task LegacyControlCharTimeSeriesNameShouldBeLoadableByName(LegacySource source)
        {
            const string docId = "items/1";
            var tsName = "Temp" + SOH + "Series";
            var t0 = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            using var store = OpenLegacyStore(source);
            await ImportLegacyAsync(store, source);

            using var session = store.OpenAsyncSession();
            var range = await session.TimeSeriesFor(docId, tsName).GetAsync(t0.AddYears(-1), t0.AddYears(1));
            Assert.NotNull(range);
            Assert.NotEmpty(range);
        }

        private Raven.Client.Documents.IDocumentStore OpenLegacyStore(LegacySource source)
        {
            // For dump import: create a fresh DB without the new ThrowControlCharactersInIdentifier feature
            // so the database is treated as legacy. For backup/snapshot: skip DB creation — the restore
            // brings the v6.2 database record (which already lacks the feature flag).
            return source switch
            {
                LegacySource.Dump => GetDocumentStore(),
                _ => GetDocumentStore(new Options { CreateDatabase = false })
            };
        }

        private async Task ImportLegacyAsync(Raven.Client.Documents.IDocumentStore store, LegacySource source)
        {
            switch (source)
            {
                case LegacySource.Dump:
                    await using (var stream = OpenResource("Dump.ravendbdump"))
                    {
                        var importOp = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                        await importOp.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
                    }

                    return;

                case LegacySource.FullBackup:
                    RestoreFromResource(store, "ControlChars.ravendb-full-backup");
                    return;

                case LegacySource.Snapshot:
                    RestoreFromResource(store, "ControlChars.ravendb-snapshot");
                    return;

                default:
                    throw new ArgumentOutOfRangeException(nameof(source));
            }
        }

        private void RestoreFromResource(Raven.Client.Documents.IDocumentStore store, string resourceFileName)
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var destFile = Path.Combine(backupPath, resourceFileName);

            using (var file = File.Create(destFile))
            using (var stream = OpenResource(resourceFileName))
            {
                stream.CopyTo(file);
            }

            Backup.RestoreDatabase(store, new RestoreBackupConfiguration
            {
                BackupLocation = backupPath,
                DatabaseName = store.Database
            });
        }

        private static Stream OpenResource(string fileName)
        {
            var stream = typeof(RavenDB_25738_LegacyVerifier).Assembly
                .GetManifestResourceStream("SlowTests.Data.RavenDB_25738." + fileName);
            Assert.NotNull(stream);
            return stream;
        }

        private sealed class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
