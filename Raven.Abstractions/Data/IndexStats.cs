//-----------------------------------------------------------------------
// <copyright file="IndexStats.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Raven.Abstractions.Indexing;

namespace Raven.Abstractions.Data
{
	public class IndexStats
	{
		/// <summary>
		/// Index identifier.
		/// </summary>
		public int Id { get; set; }

		/// <summary>
		/// Index name.
		/// </summary>
		public string Name { get; set; }

		/// <summary>
		/// Indicates how many times database tried to index documents (map) using this index.
		/// </summary>
		public int IndexingAttempts { get; set; }

		/// <summary>
		/// Indicates how many indexing attempts succeeded.
		/// </summary>
		public int IndexingSuccesses { get; set; }

		/// <summary>
		/// Indicates how many indexing attempts failed.
		/// </summary>
		public int IndexingErrors { get; set; }

		/// <summary>
		/// This value represents etag of last document indexed (using map) by this index.
		/// </summary>
		public Etag LastIndexedEtag { get; set; }

		/// <summary>
		/// Shows the difference between last document etag available in database and last indexed etag.
		/// </summary>
		public int? IndexingLag { get; set; }

		/// <summary>
		/// Time of last indexing for this index.
		/// </summary>
        public DateTime LastIndexedTimestamp { get; set; }

		/// <summary>
		/// Time of last query for this index.
		/// </summary>
		public DateTime? LastQueryTimestamp { get; set; }

		public int TouchCount { get; set; }

		/// <summary>
		/// Index priority (Normal, Disabled, Idle, Abandoned, Error)
		/// </summary>
        public IndexingPriority Priority { get; set; }

		/// <summary>
		/// Indicates how many times database tried to index documents (reduce) using this index.
		/// </summary>
		public int? ReduceIndexingAttempts { get; set; }

		/// <summary>
		/// Indicates how many reducing attempts succeeded.
		/// </summary>
		public int? ReduceIndexingSuccesses { get; set; }

		/// <summary>
		/// Indicates how many reducing attempts failed.
		/// </summary>
		public int? ReduceIndexingErrors { get; set; }

		/// <summary>
		/// This value represents etag of last document indexed (using reduce) by this index.
		/// </summary>
		public Etag LastReducedEtag { get; set; }

		/// <summary>
		/// Time of last reduce for this index.
		/// </summary>
		public DateTime? LastReducedTimestamp { get; set; }

		/// <summary>
		/// Date of index creation.
		/// </summary>
        public DateTime CreatedTimestamp { get; set; }

		/// <summary>
		/// Time of last indexing (map or reduce) for this index.
		/// </summary>
		public DateTime LastIndexingTime { get; set; }

		/// <summary>
		/// Indicates if index is in-memory only.
		/// </summary>
		public string IsOnRam { get; set; }

		/// <summary>
		/// Indicates current lock mode:
		/// - Unlock - all index definition changes acceptable
		/// - LockedIgnore - all index definition changes will be ignored, only log entry will be created
		/// - LockedError - all index definition changes will raise exception
		/// </summary>
		public IndexLockMode LockMode { get; set; }

		/// <summary>
		/// List of all entity names (collections) for which this index is working.
		/// </summary>
		public List<string> ForEntityName { get; set; } 

		/// <summary>
		/// Performance statistics for this index.
		/// </summary>
		public IndexingPerformanceStats[] Performance { get; set; }

		/// <summary>
		/// Total number of entries in this index.
		/// </summary>
		public int DocsCount { get; set; }

		/// <summary>
		/// Indicates if this is a test index (works on a limited data set - for testing purposes only)
		/// </summary>
        public bool IsTestIndex { get; set; }

		/// <summary>
		/// Determines if index is invalid. If more thant 15% of attemps (map or reduce) are errors then value will be <c>true</c>.
		/// </summary>
	    public bool IsInvalidIndex
	    {
	        get
	        {
	            return IndexFailureInformation.CheckIndexInvalid(IndexingAttempts, IndexingErrors, ReduceIndexingAttempts, ReduceIndexingErrors);
	        }
	    }

	    public override string ToString()
		{
		    return Id.ToString(CultureInfo.InvariantCulture);
		}

		public void SetLastDocumentEtag(Etag lastDocEtag)
		{
			if (lastDocEtag == null)
				return;

			IndexingLag = (int) (lastDocEtag.Changes - LastIndexedEtag.Changes);

			if (lastDocEtag.Restarts != LastIndexedEtag.Restarts)
			{
				IndexingLag *= -1;
			}
		}
	}

    [Flags]
    public enum IndexingPriority
    {
		None = 0,

        Normal = 1,
		
		Disabled = 2,
        
		Idle = 4,
		
		Abandoned = 8,

        Error = 16,

        Forced = 512,
    }

    public class IndexingPerformanceStats
	{
	    public IndexingPerformanceStats()
	    {
			LucenePerformance = new LucenePerformanceStats();
			MapStoragePerformance = new MapStoragePerformanceStats();
	    }

	    public string Operation { get; set; }
	    public int ItemsCount { get; set; }
	    public int InputCount { get; set; }
	    public int OutputCount { get; set; }
		public DateTime Started { get; set; }
		public DateTime Completed { get; set; }
	    public TimeSpan Duration { get; set; }
	    public double DurationMilliseconds { get { return Math.Round(Duration.TotalMilliseconds, 2); } }
		public LoadDocumentPerformanceStats[] LoadDocumentPerformance { get; set; }
		public LinqExecutionPerformanceStats[] LinqExecutionPerformance { get; set; }
		public LucenePerformanceStats LucenePerformance { get; set; }
		public MapStoragePerformanceStats MapStoragePerformance { get; set; }
	    public TimeSpan WaitingTimeSinceLastBatchCompleted { get; set; }
	}

	public class ParallelExecutionStats
	{
		public ParallelExecutionStats()
		{
			StartedAt = SystemTime.UtcNow;
		}

		public DateTime StartedAt { get; set; }
		public long DurationMs { get; set; }
	}

	public class LinqExecutionPerformanceStats
	{
		public LinqExecutionPerformanceStats()
		{
			StartedAt = SystemTime.UtcNow;
			MapLinqExecutionDurationMs = -1;
			ReduceLinqExecutionDurationMs = -1;
		}

		public DateTime StartedAt { get; set; }
		public long MapLinqExecutionDurationMs { get; set; }
		public long ReduceLinqExecutionDurationMs { get; set; }
	}

	public class LoadDocumentPerformanceStats
	{
		public LoadDocumentPerformanceStats()
		{
			StartedAt = SystemTime.UtcNow;
		}

		public DateTime StartedAt { get; set; }
		public int LoadDocumentCount { get; set; }
		public long LoadDocumentDurationMs { get; set; }
	}

	public class MapStoragePerformanceStats
	{
		public MapStoragePerformanceStats()
		{
			DeleteMappedResultsDurationMs = -1;
		}

		public long DeleteMappedResultsDurationMs { get; set; }
		public ParallelExecutionStats[] ConvertToRavenJObjects { get; set; }
		public ParallelExecutionStats[] PutMappedResults { get; set; }
		public ParallelExecutionStats[] ScheduleReductions { get; set; }
		public ParallelExecutionStats[] StorageCommits { get; set; }
	}

	public class LucenePerformanceStats
	{
		public long DeleteExistingDocumentsDurationMs { get; set; }
		public ParallelExecutionStats[] ConvertToLuceneDocuments { get; set; }
		public ParallelExecutionStats[] AddDocuments { get; set; }
		public long FlushToDiskDurationMs { get; set; }
		public long RecreateSearcherDurationMs { get; set; }
	}

	public class ReducingPerformanceStats
	{
		public ReducingPerformanceStats(ReduceType reduceType)
		{
			ReduceType = reduceType;
			LevelStats = new List<ReduceLevelPeformanceStats>();
		}

		public ReduceType ReduceType { get; private set; }
		public List<ReduceLevelPeformanceStats> LevelStats { get; set; } 
	}

	public class ReduceLevelPeformanceStats
	{
		public ReduceLevelPeformanceStats()
		{
			LinqExecutionPerformance = new LinqExecutionPerformanceStats[]{};
			LucenePerformance = new LucenePerformanceStats()
			{
				AddDocuments = new ParallelExecutionStats[]{},
				ConvertToLuceneDocuments = new ParallelExecutionStats[] { }
			};
			ReduceStoragePerformance = new ReduceStoragePerformanceStats();
		}

		public int Level { get; set; }
		public int ItemsCount { get; set; }
		public int InputCount { get; set; }
		public int OutputCount { get; set; }
		public DateTime Started { get; set; }
		public DateTime Completed { get; set; }
		public TimeSpan Duration { get; set; }
		public double DurationMs{ get { return Math.Round(Duration.TotalMilliseconds, 2); } }
		public LinqExecutionPerformanceStats[] LinqExecutionPerformance { get; set; }
		public LucenePerformanceStats LucenePerformance { get; set; }
		public ReduceStoragePerformanceStats ReduceStoragePerformance { get; set; }

		public void Add(IndexingPerformanceStats other)
		{
			ItemsCount += other.ItemsCount;
			InputCount += other.InputCount;
			OutputCount += other.OutputCount;

			LinqExecutionPerformance = LinqExecutionPerformance.Union(other.LinqExecutionPerformance).ToArray();

			LucenePerformance.DeleteExistingDocumentsDurationMs += other.LucenePerformance.DeleteExistingDocumentsDurationMs;
			LucenePerformance.ConvertToLuceneDocuments = LucenePerformance.ConvertToLuceneDocuments.Union(other.LucenePerformance.ConvertToLuceneDocuments).ToArray();
			LucenePerformance.AddDocuments = LucenePerformance.AddDocuments.Union(other.LucenePerformance.AddDocuments).ToArray();
			LucenePerformance.FlushToDiskDurationMs += other.LucenePerformance.FlushToDiskDurationMs;
			LucenePerformance.RecreateSearcherDurationMs += other.LucenePerformance.RecreateSearcherDurationMs;
		}
	}

	public class ReduceStoragePerformanceStats
	{
		public ParallelExecutionStats[] GetItemsToReduceDurationMs { get; set; }
		public ParallelExecutionStats[] DeletePreviouslyScheduledReductionsMs { get; set; }
		public long ScheduleReductionsDurationMs { get; set; }
		public long GetMappedResultsDurationMs { get; set; }
		public ParallelExecutionStats[] RemoveReduceResultsDurationMs { get; set; }
		public long StorageCommitDurationMs { get; set; }
	}
}
