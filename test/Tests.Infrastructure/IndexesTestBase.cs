using System;
using System.IO;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;

namespace FastTests
{
    public abstract partial class RavenTestBase
    {
        private Lazy<IndexesTestBase> _indexes = new Lazy<IndexesTestBase>();
        public class IndexesTestBase
        {
            private SearchEngineType _searchEngineType = SearchEngineType.Lucene;
            private int _termsSkip = 0;

            public IndexesTestBase()
            {
            }

            public IndexesTestBase SetSearchEngine(string searchEngine)
            {
                if (Enum.TryParse(searchEngine, out _searchEngineType) == false)
                    _searchEngineType = SearchEngineType.Lucene;
                return this;
            }
            
            public GetTermsOperation GetTerms(string indexName, string field, string fromValue, int? pageSize = null, string searchEngine = null)
            {
                if (Enum.TryParse(typeof(SearchEngineType), searchEngine, out var searchEngineType) == false)
                    searchEngineType = _searchEngineType;

                switch (searchEngineType)
                {
                    case SearchEngineType.Lucene:
                        break;
                    case SearchEngineType.Corax:
                        fromValue = _termsSkip.ToString();
                        _termsSkip += pageSize ?? 0;
                        break;
                    default:
                        throw new InvalidDataException(
                            $"Invalid SearchEngine type. Got unknown engine: {searchEngine}.");
                }

                return new GetTermsOperation(indexName, field, fromValue, pageSize);
            }
        }
    }
}
