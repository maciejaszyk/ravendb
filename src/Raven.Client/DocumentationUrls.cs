namespace Raven.Client;

internal static class DocumentationUrls
{
    internal static class Session
    {
        internal static class Querying
        {
            ///<remarks><seealso ref="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/document-query/what-is-document-query"/></remarks>
            public const string WhatIsDocumentQuery = nameof(WhatIsDocumentQuery);

            ///<remarks><seealso href="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/debugging/include-explanations"/></remarks>
            public const string IncludeExplanations = nameof(IncludeExplanations);

            ///<remarks><seealso ref="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/text-search/highlight-query-results"/></remarks>
            public const string HighlightQueryResults = nameof(HighlightQueryResults);

            ///<remarks><seealso ref="https://ravendb.net/docs/article-page/6.0/csharp/client-api/session/querying/how-to-stream-query-results"/></remarks>
            public const string StreamingQuery = nameof(StreamingQuery);

            /// <remarks><seealso ref="https://ravendb.net/docs/article-page/6.0/Csharp/client-api/session/querying/sort-query-results#order-by-score"/></remarks>
            public const string BoostingOrdering = nameof(BoostingOrdering);
        }
    }
}
