using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Mono.Unix.Native;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.IndexMerging;

public static class IndexMergerHelper
{
    internal static bool IndexCanBeMerged(IndexData indexData, List<MergeProposal> mergedIndexesData)
    {
        var failComments = new List<string>();
        if (indexData.Index.Type == IndexType.MapReduce)
        {
            failComments.Add("Cannot merge map/reduce indexes");
        }

        if (indexData.Index.Maps.Count > 1)
        {
            failComments.Add("Cannot merge multi map indexes");
        }

        if (indexData.NumberOfFromClauses > 1)
        {
            failComments.Add("Cannot merge indexes that have more than a single from clause");
        }

        if (indexData.NumberOfSelectClauses > 1)
        {
            failComments.Add("Cannot merge indexes that have more than a single select clause");
        }

        if (indexData.HasWhere)
        {
            failComments.Add("Cannot merge indexes that have a where clause");
        }

        if (indexData.HasGroup)
        {
            failComments.Add("Cannot merge indexes that have a group by clause");
        }

        if (indexData.HasLet)
        {
            failComments.Add("Cannot merge indexes that are using a let clause");
        }

        if (indexData.HasOrder)
        {
            failComments.Add("Cannot merge indexes that have an order by clause");
        }

        if (indexData.IsMapReduceOrMultiMap)
        {
            failComments.Add("Cannot merge MultiMap/MapReduce indexes.");
        }

        if (failComments.Count != 0)
        {
            // studio will show msg only in one line.
            indexData.Comment = string.Join(" | ", failComments);
            indexData.IsSuitedForMerge = false;
            mergedIndexesData.Add(new MergeProposal() {MergedData = indexData});
            return false;
        }

        return true;
    }

    internal static bool CompareIndexFieldOptions(IndexData index1Data, IndexData index2Data)
    {
        // Our index fields are stored in selectexpression at index 0.
        var intersectNames = index2Data.SelectExpressions[0].SelectExpressions.Keys.Intersect(index1Data.SelectExpressions[0].SelectExpressions.Keys)
            .ToArray();
        return DataDictionaryCompare(index1Data.Index.Fields, index2Data.Index.Fields, intersectNames);
    }

    private static bool DataDictionaryCompare<T>(IDictionary<string, T> dataDict1, IDictionary<string, T> dataDict2, IEnumerable<string> names)
    {
        bool found1, found2;

        foreach (string kvp in names)
        {
            found1 = dataDict1.TryGetValue(kvp, out T v1);
            found2 = dataDict2.TryGetValue(kvp, out T v2);

            if (found1 && found2 && Equals(v1, v2) == false)
                return false;

            // exists only in 1 - check if contains default value
            if (found1 && !found2)
                return false;

            if (found2 && !found1)
                return false;
        }

        return true;
    }
    
    internal static bool CanMergeIndexes(IndexData other, IndexData current)
    {

        if (current.Collection != other.Collection)
            return false;
            
        if (current.IndexName == other.IndexName)
            return false;
            
        // Fanout indexes
        if (current.NumberOfFromClauses != 1)
            return false;
            
        //Mutliple select clauses e.g. docs.Orders.Select(i => new {this0 = i}).Select(z => new {Field = z.this0.Field});
        if (current.NumberOfSelectClauses != other.NumberOfSelectClauses)
            return false;

        if (current.HasWhere)
            return false;

        if (current.HasGroup)
            return false;
        if (current.HasOrder)
            return false;
        if (current.HasLet)
            return false;

        var currentFromExpression = current.FromExpression as MemberAccessExpressionSyntax;
        var otherFromExpression = other.FromExpression as MemberAccessExpressionSyntax;

        if (currentFromExpression != null || otherFromExpression != null)
        {
            if (currentFromExpression == null || otherFromExpression == null)
                return false;

            if (currentFromExpression.Name.Identifier.ValueText != otherFromExpression.Name.Identifier.ValueText)
                return false;
        }

        return CompareIndexFieldOptions(other, current);
    }
}
