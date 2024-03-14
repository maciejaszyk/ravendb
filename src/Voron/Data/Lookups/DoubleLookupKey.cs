﻿using System;
using System.Diagnostics.Contracts;
using Sparrow.Extensions;

namespace Voron.Data.Lookups;

public struct DoubleLookupKey : ILookupKey
{
    public double Value;

    public void Reset()
    {
        
    }

    public long ToLong()
    {
        return BitConverter.DoubleToInt64Bits(Value);
    }
    
    public static implicit operator DoubleLookupKey(double d)
    {
        return new DoubleLookupKey(d);
    }

    public DoubleLookupKey(double value)
    {
        Value = value;
    }

    public override string ToString()
    {
        return Value.ToString();
    }

    public static T FromLong<T>(long l)
    {
        if (typeof(T) != typeof(DoubleLookupKey))
        {
            throw new NotSupportedException(typeof(T).FullName);
        }

        return (T)(object)new DoubleLookupKey(BitConverter.Int64BitsToDouble(l));
    }

    
    public static long MinValue => BitConverter.DoubleToInt64Bits(double.MinValue);

    public void Init<T>(Lookup<T> parent) where T : struct, ILookupKey
    {
        
    }

    [Pure]
    public int CompareTo<T>(Lookup<T> parent, long l) where T : struct, ILookupKey
    {
        var d = BitConverter.Int64BitsToDouble(l);
        return Value.CompareTo(d);
    }

    [Pure]
    public bool IsEqual<T>(T k) where T : ILookupKey
    {
        if (typeof(T) != typeof(DoubleLookupKey))
        {
            throw new NotSupportedException(typeof(T).FullName);
        }

        var o = (DoubleLookupKey)(object)k;

        if (Value is 0 && o.Value is 0)
        {
            // It's possible that we have a negative zero, in which case BitConverter will return completely different numbers for 0 and -0. In such case we use standard double comparer.
            return true;
        }
        
        return BitConverter.DoubleToInt64Bits(Value) == BitConverter.DoubleToInt64Bits(o.Value);
    }

    public void OnNewKeyAddition<T>(Lookup<T> parent) where T : struct, ILookupKey
    {
        
    }

    public void OnKeyRemoval<T>(Lookup<T> parent) where T : struct, ILookupKey
    {
    }

    public string ToString<T>(Lookup<T> parent) where T : struct, ILookupKey
    {
        return ToString();
    }

    public int CompareTo<T>(T l) where T : ILookupKey
    {
        if (typeof(T) != typeof(DoubleLookupKey))
        {
            throw new NotSupportedException(typeof(T).FullName);
        }

        var o = (DoubleLookupKey)(object)l;
        return Value.CompareTo(o.Value);
    }
}
