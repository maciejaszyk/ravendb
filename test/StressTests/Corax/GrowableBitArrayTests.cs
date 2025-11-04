using System;
using Corax.Utils;
using FastTests;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Corax;

public class GrowableBitArrayTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Corax, architecture: RavenArchitecture.AllX64)]
    public void CanSetSecondBitmap()
    {
       
    }

}
