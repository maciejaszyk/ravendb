using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.IndexMerging
{
    public class IndexMerger
    {
        private readonly Dictionary<string, IndexDefinition> _indexDefinitions;

        public IndexMerger(Dictionary<string, IndexDefinition> indexDefinitions)
        {
            // IndexMerger operates only on static indexes. Auto indexes are merged automatically.
            _indexDefinitions = indexDefinitions
                .Where(i => i.Value.Type.IsAuto() == false && i.Value.Type.IsJavaScript() == false)
                .ToDictionary(i => i.Key, i=> i.Value);
        }

        public IndexMergeResults ProposeIndexMergeSuggestions()
        {
            var indexes = GetIndexData();
            var mergedIndexesData = MergeIndexes(indexes);
            var mergedResults = CreateMergeIndexDefinition(mergedIndexesData);
            return mergedResults;
        }
        
        private List<MergeProposal> MergeIndexes(List<IndexData> indexes)
        {
            var mergedIndexesData = new List<MergeProposal>();
            foreach (var indexData in indexes.Where(indexData => indexData.IsSuitedForMerge && indexData.IsAlreadyMerged == false))
            {
                indexData.IsAlreadyMerged = true;
                if (IndexMergerHelper.IndexCanBeMerged(indexData, mergedIndexesData) == false)
                    continue;
                
                var mergeData = new MergeProposal();
                mergeData.ProposedForMerge.Add(indexData);

                foreach (IndexData current in indexes) // Note, we have O(N**2) here, known and understood
                {
                    
                    if (mergeData.ProposedForMerge.All(other => IndexMergerHelper.CanMergeIndexes(other, current)) == false)
                        continue;

                    if (AreSelectClausesCompatible(current, indexData) == false)
                        continue;

                    current.IsSuitedForMerge = true;
                    mergeData.ProposedForMerge.Add(current);
                }

                mergedIndexesData.Add(mergeData);
            }

            return mergedIndexesData;
        }
        

        // Get index data. This will gather all fields etc into IndexData
        private List<IndexData> GetIndexData()
        {
            var indexes = new List<IndexData>();

            foreach (var kvp in _indexDefinitions)
            {
                var index = kvp.Value;
                var indexData = new IndexData(index) {IndexName = index.Name, OriginalMaps = index.Maps};

                indexes.Add(indexData);

                if (index.Type == IndexType.MapReduce || index.Maps.Count > 1)
                {
                    indexData.IsMapReduceOrMultiMap = true;
                    continue;
                }

                var map = SyntaxFactory.ParseExpression(indexData.OriginalMaps.FirstOrDefault()).NormalizeWhitespace();
                var visitor = new IndexVisitor(indexData);
                visitor.Visit(map);
            }

            return indexes;
        }
        
        // We've to check if our index selects accept conditions:
        // 1. Same amount select expression in both indexes.
        // 2. No name conflicts
        // 3. When field exists in both indexes we've to check if we store there the same value.
        private static bool AreSelectClausesCompatible(IndexData firstIndex, IndexData secondIndex)
        {
            if (firstIndex.SelectExpressions.Count != secondIndex.SelectExpressions.Count)
                return false;

            var selectClausesCount = firstIndex.SelectExpressions.Count;

            // Select at 0 index is our projection clause.
            for (var selectClauseIdx = 0; selectClauseIdx < selectClausesCount; ++selectClauseIdx)
            {
                var firstIndexSelectClauseFields = firstIndex.SelectExpressions[selectClauseIdx];
                var secondIndexSelectClauseFields = secondIndex.SelectExpressions[selectClauseIdx];

                foreach (var fieldData in firstIndexSelectClauseFields.SelectExpressions)
                {
                    
                    if (secondIndexSelectClauseFields.SelectExpressions.TryGetValue(fieldData.Key, out ExpressionSyntax expressionFromSecondIndex) == false)
                        continue; //Field doesn't exists in another index. This is good because there is no conflict.

                    //We've to take into account that identifier from higher lvl can be different. 
                    var firstSelectExpr = ExtractValueFromExpression(fieldData.Value);
                    var secondSelectExpr = ExtractValueFromExpression(expressionFromSecondIndex);
                    
                    // for the same key, they have to be the same
                    if (firstSelectExpr != secondSelectExpr)
                        return false;

                }
            }
         
            return true;
        }

        // We want to check if one index is a subset of another.
        private static bool AreSelectClausesTheSame(IndexData index, Dictionary<int, SelectClause> selectsFromOther)
        {
            // We want to delete an index when that index is a subset of another.
            if (index.SelectExpressions.Count != selectsFromOther.Count)
                 return false;

            var count = selectsFromOther.Count;
            
            for (int selectIdx = 0; selectIdx < count; ++selectIdx)
            {
                var currentDocIdentifier = count == 1 ? "doc" : $"this{count - selectIdx}";
                var selectExpressionDict = selectsFromOther[selectIdx].SelectExpressions;
                
                
                
                var selectExpressionFromCurrent = index.SelectExpressions[selectIdx].SelectExpressions;
                foreach (var pair in selectExpressionFromCurrent)
                {
                    if (selectExpressionDict.TryGetValue(pair.Key, out ExpressionSyntax expressionValue) == false)
                        return false;
                    
                    var ySelectExpr = TransformAndExtractValueFromExpression(expressionValue, currentDocIdentifier, selectsFromOther[selectIdx]);
                    var xSelectExpr = TransformAndExtractValueFromExpression(pair.Value, currentDocIdentifier, index.SelectExpressions[selectIdx]);
                    if (xSelectExpr != ySelectExpr)
                    {
                        return false;
                    }
                }
            }
            
            return true;

            string TransformAndExtractValueFromExpression(ExpressionSyntax expr, string currentDocIdentifier, SelectClause selectClause) => expr switch
            {
                InvocationExpressionSyntax ies => RecursivelyTransformInvocationExpressionSyntax(index, ies, currentDocIdentifier, selectClause, out var _).ToString(),
                _ => ExtractValueFromExpression(expr)
            };
        }

        private IndexMergeResults CreateMergeIndexDefinition(List<MergeProposal> indexDataForMerge)
        {
            var indexMergeResults = new IndexMergeResults();
            foreach (var mergeProposal in indexDataForMerge.Where(m => m.ProposedForMerge.Count == 0 && m.MergedData != null))
            {
                indexMergeResults.Unmergables.Add(mergeProposal.MergedData.IndexName, mergeProposal.MergedData.Comment);
            }

            foreach (var mergeProposal in indexDataForMerge)
            {
                if (mergeProposal.ProposedForMerge.Count == 0)
                    continue;

                var mergeSuggestion = new MergeSuggestions();
                var selectExpressionDict = new Dictionary<string, ExpressionSyntax>();

                if (TryMergeSelectExpressionsAndFields(mergeProposal, selectExpressionDict, mergeSuggestion, out var mergingComment) == false)
                {
                    indexMergeResults.Unmergables.Add(mergeProposal.MergedData.IndexName, mergingComment); 
                    continue;
                }

                TrySetCollectionName(mergeProposal, mergeSuggestion);

                var map = mergeProposal.ProposedForMerge[0].BuildExpression(selectExpressionDict);
                mergeSuggestion.MergedIndex.Maps.Add(SourceCodeBeautifier.FormatIndex(map).Expression);
                RemoveMatchingIndexes(mergeProposal, selectExpressionDict, mergeSuggestion, indexMergeResults);

                if (mergeProposal.ProposedForMerge.Count == 1 && mergeProposal.ProposedForMerge[0].IsSuitedForMerge == false)
                {
                    const string comment = "Can't find any other index to merge this with";
                    indexMergeResults.Unmergables.Add(mergeProposal.ProposedForMerge[0].IndexName, comment);
                }
            }

            indexMergeResults = ExcludePartialResults(indexMergeResults);
            return indexMergeResults;
        }

        private static void RemoveMatchingIndexes(MergeProposal mergeProposal, Dictionary<string, ExpressionSyntax> selectExpressionDict,
            MergeSuggestions mergeSuggestion,
            IndexMergeResults indexMergeResults)
        {
            if (mergeProposal.ProposedForMerge.Count > 1)
            {
                var matchingExistingIndexes = mergeProposal.ProposedForMerge.Where(x =>
                        AreSelectClausesTheSame(x, selectExpressionDict) &&
                        (x.Index.Compare(mergeSuggestion.MergedIndex) == IndexDefinitionCompareDifferences.None
                         || x.Index.Compare(mergeSuggestion.MergedIndex) == IndexDefinitionCompareDifferences.Maps))
                    .OrderBy(x => x.IndexName.StartsWith("Auto/", StringComparison.CurrentCultureIgnoreCase))
                    .ToList();

                if (matchingExistingIndexes.Count > 0)
                {
                    var surpassingIndex = matchingExistingIndexes.First();
                    mergeSuggestion.SurpassingIndex = surpassingIndex.IndexName;

                    mergeSuggestion.MergedIndex = null;
                    mergeSuggestion.CanMerge.Clear();
                    mergeSuggestion.CanDelete = mergeProposal.ProposedForMerge.Except(new[] {surpassingIndex}).Select(x => x.IndexName).ToList();
                }

                indexMergeResults.Suggestions.Add(mergeSuggestion);
            }
        }

        private static void TrySetCollectionName(MergeProposal mergeProposal, MergeSuggestions mergeSuggestion)
        {
            if (mergeProposal.ProposedForMerge[0].Collection != null)
            {
                mergeSuggestion.Collection = mergeProposal.ProposedForMerge[0].Collection;
            }

            else if (mergeProposal.ProposedForMerge[0].FromExpression is SimpleNameSyntax name)
            {
                mergeSuggestion.Collection = name.Identifier.ValueText;
            }

            else if (mergeProposal.ProposedForMerge[0].FromExpression is MemberAccessExpressionSyntax member)
            {
                var identifier = ExtractIdentifierFromExpression(member);
                if (identifier == "docs")
                    mergeSuggestion.Collection = ExtractValueFromExpression(member);
            }
        }

        private static bool TryMergeSelectExpressionsAndFields(MergeProposal mergeProposal, Dictionary<string, ExpressionSyntax> selectExpressionDict,
            MergeSuggestions mergeSuggestion, out string message)
        {
            message = null;
            foreach (var curProposedData in mergeProposal.ProposedForMerge)
            {
                foreach (var curExpr in curProposedData.SelectExpressions)
                {
                    var expression = curExpr.Value as MemberAccessExpressionSyntax;
                    var identifierName = ExtractIdentifierFromExpression(expression);

                    if (identifierName != null && identifierName == curProposedData.FromIdentifier)
                    {
                        expression = ChangeParentInMemberSyntaxToDoc(expression);
                        selectExpressionDict[curExpr.Key] = expression ?? curExpr.Value;
                    }
                    else if (expression is null && curExpr.Value is InvocationExpressionSyntax ies)
                    {
                        selectExpressionDict[curExpr.Key] = RecursivelyTransformInvocationExpressionSyntax(curProposedData, ies, out message);
                        if (message != null)
                            return false;
                    }
                    else
                    {
                        selectExpressionDict[curExpr.Key] = curExpr.Value;
                    }
                }

                mergeSuggestion.CanMerge.Add(curProposedData.IndexName);
                IndexMergerHelper.DataDictionaryMerge(mergeSuggestion.MergedIndex.Fields, curProposedData.Index.Fields);
            }

            return true;
        }

        private static InvocationExpressionSyntax RecursivelyTransformInvocationExpressionSyntax(IndexData curProposedData, InvocationExpressionSyntax ies,
            string newIdentifier, SelectClause selectClause, out string message)
        {
            message = null;
            if (RuntimeHelpers.TryEnsureSufficientExecutionStack() == false)
            {
                message = "Index is too complex. Cannot apply merging on it.";
                return null;
            }

            List<ArgumentSyntax> rewrittenArguments = new();
            foreach (var argument in ies.ArgumentList.Arguments)
            {
                ExpressionSyntax result = RewriteExpressionSyntax(curProposedData, argument.Expression, selectClause, newIdentifier, out message);

                if (result == null)
                {
                    message = $"Currently, {nameof(IndexMerger)} doesn't handle {argument.Expression.GetType()}.";
                    return null;
                }

                rewrittenArguments.Add(SyntaxFactory.Argument(result));
            }

            ExpressionSyntax invocationExpression = ChangeParentInMemberSyntaxToNewIdentifier(ies.Expression as MemberAccessExpressionSyntax, newIdentifier) ?? ies.Expression;

            return SyntaxFactory.InvocationExpression(invocationExpression,
                SyntaxFactory.ArgumentList(SyntaxFactory.SeparatedList<ArgumentSyntax>(rewrittenArguments)));
            
        }

        private static ExpressionSyntax RewriteExpressionSyntax(IndexData indexData, ExpressionSyntax originalExpression, SelectClause selectClause, string newIdentifier, out string message)
        {
            if (RuntimeHelpers.TryEnsureSufficientExecutionStack() == false)
            {
                message = "Index is too complex. Cannot apply merging on it.";
                return null;
            }
            message = null;
            return originalExpression switch
            {
                MemberAccessExpressionSyntax maes => ChangeParentInMemberSyntaxToNewIdentifier(maes, newIdentifier),
                InvocationExpressionSyntax iesInner => RecursivelyTransformInvocationExpressionSyntax(indexData, iesInner, newIdentifier, selectClause, out message),
                SimpleLambdaExpressionSyntax =>  originalExpression,
                IdentifierNameSyntax ins => ChangeIdentifierToIndexMergerDefaultWhenNeeded(ins), 
                BinaryExpressionSyntax bes => RewriteBinaryExpression(indexData, bes), 
                _ => null
            };
            
            IdentifierNameSyntax ChangeIdentifierToIndexMergerDefaultWhenNeeded(IdentifierNameSyntax original)
            {
                if (original.ToFullString() == selectClause.FromIdentifier)
                    return SyntaxFactory.IdentifierName(newIdentifier);

                return original;
            }
        }
        
        private static BinaryExpressionSyntax RewriteBinaryExpression(IndexData indexData, BinaryExpressionSyntax original)
        {
            var leftSide = RewriteExpressionSyntax(indexData, original.Left, out var _);
            var rightSide = RewriteExpressionSyntax(indexData, original.Right, out var _);

            return SyntaxFactory.BinaryExpression(original.Kind(), leftSide, original.OperatorToken, rightSide);
        }
        
        private static MemberAccessExpressionSyntax ChangeParentInMemberSyntaxToNewIdentifier(MemberAccessExpressionSyntax memberAccessExpression, string newIdentifier)
        {
            if (memberAccessExpression?.Expression is MemberAccessExpressionSyntax)
            {
                var valueStr = ExtractValueFromExpression(memberAccessExpression);
                var valueExp = SyntaxFactory.ParseExpression(valueStr).NormalizeWhitespace();
                var innerName = ExtractIdentifierFromExpression(valueExp as MemberAccessExpressionSyntax);
                var innerMember = SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName(newIdentifier), SyntaxFactory.IdentifierName(innerName));
                return SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression, innerMember, memberAccessExpression.Name);
            }

            if (memberAccessExpression?.Expression is SimpleNameSyntax)
            {
                return SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression, SyntaxFactory.IdentifierName(newIdentifier),
                    memberAccessExpression.Name);
            }

            return null;
        }

        private static IndexMergeResults ExcludePartialResults(IndexMergeResults originalIndexes)
        {
            var resultingIndexMerge = new IndexMergeResults();

            foreach (var suggestion in originalIndexes.Suggestions)
            {
                suggestion.CanMerge.Sort();
            }

            var hasMatch = false;
            for (var i = 0; i < originalIndexes.Suggestions.Count; i++)
            {
                var sug1 = originalIndexes.Suggestions[i];
                for (var j = i + 1; j < originalIndexes.Suggestions.Count; j++)
                {
                    var sug2 = originalIndexes.Suggestions[j];
                    if (sug1 != sug2 && sug1.CanMerge.Count <= sug2.CanMerge.Count)
                    {
                        var sugCanMergeSet = new HashSet<string>(sug1.CanMerge);
                        hasMatch = sugCanMergeSet.IsSubsetOf(sug2.CanMerge);
                        if (hasMatch)
                            break;
                    }
                }

                if (!hasMatch)
                {
                    resultingIndexMerge.Suggestions.Add(sug1);
                }

                hasMatch = false;
            }

            resultingIndexMerge.Unmergables = originalIndexes.Unmergables;
            return resultingIndexMerge;
        }

        private static string ExtractValueFromExpression(ExpressionSyntax expression)
        {
            if (expression == null)
                return null;

            var memberExpression = expression as MemberAccessExpressionSyntax;
            if (memberExpression == null)
                return expression.ToString();
            
            var identifier = ExtractIdentifierFromExpression(memberExpression);
            var value = expression.ToString();

            if (identifier == null)
                return value;
            var parts = value.Split('.');
            return parts[0] == identifier ? value.Substring(identifier.Length + 1) : value;
        }

        private static string ExtractIdentifierFromExpression(MemberAccessExpressionSyntax expression)
        {
            var node = expression?.Expression;
            while (node != null)
            {
                if (!(node is MemberAccessExpressionSyntax))
                    break;

                node = (node as MemberAccessExpressionSyntax).Expression;
            }

            if (node == null)
                return null;

            var identifier = node as IdentifierNameSyntax;
            return identifier?.Identifier.ValueText;
        }
    }
}
