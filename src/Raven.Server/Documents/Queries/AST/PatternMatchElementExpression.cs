﻿using System;
using System.Collections.Generic;
using System.Text;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.AST
{
    public class PatternMatchElementExpression : QueryExpression
    {
        public MatchPath[] Path;

        public bool Reversed;

        public override string ToString() => GetText();

        public override string GetText(IndexQueryServerSide parent) => GetText();

        public string GetText()
        {
            var sb = new StringBuilder();

            for (int i = 0; i < Path.Length; i++)
            {
                sb.Append(Path[i].IsEdge ? "[" : "(");
                sb.Append(Path[i].Alias.Value);
                sb.Append(Path[i].IsEdge ? "]" : ")");

                if (i + 1 < Path.Length)
                {
                    if (Path[i+1].Recursive != null)
                    {
                        sb.Append(Path[i + 1].Recursive.Value);
                        continue;
                    }

                    switch (Path[i + 1].EdgeType)
                    {
                        case EdgeType.Right:
                            sb.Append(Path[i+1].IsEdge ? "-" : "->");
                            break;
                        case EdgeType.Left:
                            sb.Append(Path[i].IsEdge ? "-" : "<-");
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(Path[i + 1].EdgeType + " is not known");
                    }
                }
            }
            return sb.ToString();
        }

        public override bool Equals(QueryExpression other)
        {
            if (!(other is PatternMatchElementExpression pe))
                return false;

            if (Path.Length != pe.Path.Length)
                return false;

            for (int i = 0; i < Path.Length; i++)
            {
                if (Path[i].EdgeType != pe.Path[i].EdgeType ||
                    Path[i].Alias != pe.Path[i].Alias)
                    return false;
            }
            return true;
        }
    }

    public enum EdgeType
    {
        Right,
        Left
    }

    public enum RecursiveMatchType
    {
        Lazy,
        Longest,
        Shortest,
        All,
    }

    public struct RecursiveMatch
    {
        public StringSegment Alias;
        public List<MatchPath> Pattern;
        public HashSet<StringSegment> Aliases;
        public List<ValueExpression> Options;

        public override string ToString()
        {
            var sp = new StringBuilder(" recursive ");
            if (Alias.Length != 0)
                sp.Append(" as ").Append(Alias.Value).Append(" ");

            if (Options?.Count > 0)
            {
                sp.Append("(");
                foreach (var item in Options)
                {
                    sp.Append(item);
                }
                sp.Append(") ");
            }

            sp.Append("{ ");

            foreach (var item in Pattern)
            {
                sp.Append(item);
                sp.Append(" ");
            }

            sp.Append("} ");

            return sp.ToString();
        }

        public struct RecursiveOptions
        {
            public int Max, Min;
            public RecursiveMatchType Type;
        }

        public RecursiveOptions GetOptions(QueryMetadata queryMetadata, BlittableJsonReaderObject queryParameters)
        {
            var min = 1;
            var max = int.MaxValue;
            var type = RecursiveMatchType.Lazy;

            (object Value, ValueTokenType Type) value;
            switch (Options?.Count)
            {
                case null:
                case 0:
                    break;
                case 1:
                    value = LuceneQueryBuilder.GetValue(queryMetadata.Query, queryMetadata, queryParameters, Options[0]);
                    switch (value.Type)
                    {
                        case ValueTokenType.Long:
                            min = ValidateNumber(value);
                            break;
                        case ValueTokenType.String:
                            if (Enum.TryParse(value.Value.ToString(), true, out type) == false)
                                throw new InvalidOperationException("Unexpected value for recursive match type option, got: " + value.Value + " but expected (all, longest, shortest) on " + this);
                            break;
                        default:
                            throw new InvalidOperationException("Unexpected type for recursive match option, got: " + value.Type + " for " + this);
                    }
                    break;
                case 2:
                    value = LuceneQueryBuilder.GetValue(queryMetadata.Query, queryMetadata, queryParameters, Options[0]);
                    switch (value.Type)
                    {
                        case ValueTokenType.Long:
                            min = ValidateNumber(value);
                            break;
                          default:
                            throw new InvalidOperationException("Unexpected type for recursive match option, got: " + value.Type + ", but expected min numeric value for " + this);
                    }
                    value = LuceneQueryBuilder.GetValue(queryMetadata.Query, queryMetadata, queryParameters, Options[1]);
                    switch (value.Type)
                    {
                        case ValueTokenType.Long:
                            max = ValidateNumber(value);
                            break;
                        case ValueTokenType.String:
                            if (Enum.TryParse(value.Value.ToString(), true, out type) == false)
                                throw new InvalidOperationException("Unexpected value for recursive match type option, got: " + value.Value + " but expected (all, longest, shortest) on " + this);
                            break;
                        default:
                            throw new InvalidOperationException("Unexpected type for recursive match option, got: " + value.Type + " for " + this);
                    }
                    break;

                case 3:
                    value = LuceneQueryBuilder.GetValue(queryMetadata.Query, queryMetadata, queryParameters, Options[0]);
                    switch (value.Type)
                    {
                        case ValueTokenType.Long:
                            min = ValidateNumber(value);
                            break;
                        default:
                            throw new InvalidOperationException("Unexpected type for recursive match option, got: " + value.Type + ", but expected min numeric value for " + this);
                    }
                    value = LuceneQueryBuilder.GetValue(queryMetadata.Query, queryMetadata, queryParameters, Options[1]);
                    switch (value.Type)
                    {
                        case ValueTokenType.Long:
                            max= ValidateNumber(value);
                            break;
                        default:
                            throw new InvalidOperationException("Unexpected type for recursive match option, got: " + value.Type + ", but expected max numeric value for " + this);
                    }
                    value = LuceneQueryBuilder.GetValue(queryMetadata.Query, queryMetadata, queryParameters, Options[2]);
                    switch (value.Type)
                    {
                        case ValueTokenType.Long:
                            min = ValidateNumber(value);
                            break;
                        case ValueTokenType.String:
                            if (Enum.TryParse(value.Value.ToString(), true, out type) == false)
                                throw new InvalidOperationException("Unexpected value for recursive match type option, got: " + value.Value + " but expected (all, longest, shortest) on " + this);
                            break;
                        default:
                            throw new InvalidOperationException("Unexpected type for recursive match option, got: " + value.Type + " for " + this);
                    }
                    break;
                default:
                    throw new InvalidOperationException("Unexpected number of recursive match options, a max of three is expected, but got: " + Options.Count + " for " + this);
            }
            return new RecursiveOptions
            {
                Max = max,
                Min = min,
                Type = type
            };
        }

        private int ValidateNumber((object Value, ValueTokenType Type) value)
        {
            var l = (long)value.Value;
            if (l < 0 || l > int.MaxValue)
                throw new InvalidOperationException("Unexpected number for recursive match length option, got: " + l + ", but requires positive int32 on " + this);
            var t = (int)l;
            return t;
        }
    }

    public struct MatchPath
    {
        public FieldExpression Field;
        public StringSegment Alias;
        public EdgeType EdgeType;
        public bool IsEdge;
        public RecursiveMatch? Recursive;
        public bool IsImplicit;

        public MatchPath CloneWithDifferentEdgeType(EdgeType type, bool cloneWithField = true)
        {
            return new MatchPath
            {
                Alias = Alias,
                EdgeType = type,
                IsEdge = IsEdge,
                Recursive = Recursive,
                Field = cloneWithField ? Field : null,
                IsImplicit = IsImplicit
            };
        }

        public override string ToString()
        {
            if (Recursive != null)
            {
                if (EdgeType == EdgeType.Left)
                    return "<-" + Recursive.Value + "-";

                return "-" + Recursive.Value.ToString() + "->";
            }

            return (IsEdge ? "[" : "(") +  Alias + (IsEdge ? "]" : ")") + (EdgeType == EdgeType.Left ? "<-" : "->"); 
        }
    }
}
