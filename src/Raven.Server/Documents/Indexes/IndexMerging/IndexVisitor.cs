using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using NetTopologySuite.GeometriesGraph;

namespace Raven.Server.Documents.Indexes.IndexMerging
{
    internal class IndexVisitor : CSharpSyntaxRewriter
    {
        private readonly IndexData _indexData;

        public IndexVisitor(IndexData indexData)
        {
            _indexData = indexData;
            indexData.NumberOfFromClauses = 0;
            indexData.SelectExpressions = new();
            _indexData.Collection = null;
        }

        public override SyntaxNode VisitQueryExpression(QueryExpressionSyntax node)
        {
            AssertStack();

            _indexData.FromExpression = node.FromClause.Expression;
            _indexData.FromIdentifier = node.FromClause.Identifier.ValueText;
            _indexData.NumberOfFromClauses++;
         //   VisitQueryBody(node.Body);


            return base.VisitQueryExpression(node);
        }

        public override SyntaxNode VisitMemberAccessExpression(MemberAccessExpressionSyntax node)
        {
            AssertStack();
            
            // last token we got is our Collection
            _indexData.Collection = node.Name.Identifier.ValueText;
            return base.VisitMemberAccessExpression(node);
        }
        
        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax invocationExpression)
        {
            AssertStack();

            var memberAccessExpressionSyntax = invocationExpression.Expression as MemberAccessExpressionSyntax;
            switch (memberAccessExpressionSyntax?.Name.Identifier.ValueText)
            {
                case "Where":
                    _indexData.HasWhere = true;
                    base.VisitInvocationExpression(invocationExpression);
                break;
                default:
                    break;
            }
            
            
            var arguments = invocationExpression.ArgumentList.Arguments;
            if (arguments.Count != 1)
                CantHandle();

            var lambdaExpression = arguments[0].Expression as SimpleLambdaExpressionSyntax;
            if (lambdaExpression is null)
                CantHandle();

            var fromIdentifier = lambdaExpression!.Parameter.Identifier.ValueText;
            var expressionSyntaxes = new Dictionary<string, ExpressionSyntax>();
            var evaluator = new CaptureSelectExpressionsAndNewFieldNamesVisitor(false, new(), expressionSyntaxes);
            switch (lambdaExpression!.ExpressionBody)
            {
                case AnonymousObjectCreationExpressionSyntax aoces:
                    evaluator.VisitAnonymousObjectCreationExpression(aoces);
                    break;
            }

            _indexData.SelectExpressions[_indexData.SelectExpressions.Count] = new SelectClause()
            {
                FromIdentifier = fromIdentifier, 
                SelectExpressions = expressionSyntaxes
            };

            if (invocationExpression.Expression is InvocationExpressionSyntax ies)
                return VisitInvocationExpression(ies);

            //Lets allow default crawler to go through tree
            return base.VisitInvocationExpression(invocationExpression);
        }

        private static void CantHandle()
        {
            throw new NotSupportedException("Don't know how to handle that");
        }

        public override SyntaxNode VisitQueryBody(QueryBodySyntax node)
        {
            AssertStack();
            
            if ((node.SelectOrGroup is SelectClauseSyntax) == false)
            {
                return base.VisitQueryBody(node);
            }


            var selectExpressions = new Dictionary<string, ExpressionSyntax>();
            var visitor = new CaptureSelectExpressionsAndNewFieldNamesVisitor(false, new HashSet<string>(), selectExpressions);
            node.Accept(visitor);

            if (_indexData.SelectExpressions.Count == 1)
                _indexData.IsSuitedForMerge = false;
            
            _indexData.SelectExpressions[_indexData.SelectExpressions.Count] =
                new SelectClause() {FromExpression = _indexData.FromExpression, SelectExpressions = selectExpressions};
            _indexData.NumberOfSelectClauses++;
           
            return base.VisitQueryBody(node);
        }

        public override SyntaxNode VisitWhereClause(WhereClauseSyntax queryWhereClause)
        {
            AssertStack();
            _indexData.HasWhere = true;
            return base.VisitWhereClause(queryWhereClause);
        }

        public override SyntaxNode VisitOrderByClause(OrderByClauseSyntax queryOrderClause)
        {
            AssertStack();
            _indexData.HasOrder = true;
            return base.VisitOrderByClause(queryOrderClause);
        }

        public override SyntaxNode VisitOrdering(OrderingSyntax queryOrdering)
        {
            AssertStack();
            _indexData.HasOrder = true;
            return base.VisitOrdering(queryOrdering);
        }

        public override SyntaxNode VisitGroupClause(GroupClauseSyntax queryGroupClause)
        {
            AssertStack();
            _indexData.HasGroup = true;
            return base.VisitGroupClause(queryGroupClause);
        }

        public override SyntaxNode VisitLetClause(LetClauseSyntax queryLetClause)
        {
            AssertStack();
            _indexData.HasLet = true;
            return base.VisitLetClause(queryLetClause);
        }

        private void AssertStack()
        {
            if (RuntimeHelpers.TryEnsureSufficientExecutionStack() == false)
                throw new InvalidDataException("Cannot parse the index.");
        }
        
        internal static void DataDictionaryMerge<TKey, TVal>(IDictionary<TKey, TVal> dest, IDictionary<TKey, TVal> src)
        {
            foreach (var val in src)
            {
                dest[val.Key] = val.Value;
            }
        }
    }
}
