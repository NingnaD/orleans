using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace TestInternalGrainInterfaces
{
    internal interface ISimpleSelfManagedGrain : IGrainWithIntegerKey
    {
        // duplicate to verify identity
        Task<long> GetKey();

        // separate label that can be set
        Task<string> GetLabel();

        Task SetLabel(string label);

        Task<string> GetRuntimeInstanceId();

        Task<string> GetActivationId();

        Task<ISimpleSelfManagedGrain> GetGrainReference();

        Task<Tuple<string, string>> TestRequestContext();

        Task<IGrain[]> GetMultipleGrainInterfaces_Array();

        Task<List<IGrain>> GetMultipleGrainInterfaces_List();

        Task StartTimer();

        Task DoLongAction(TimeSpan timespan, string str);
    }

    internal interface IGuidSimpleSelfManagedGrain : IGrainWithGuidKey
    {
        // duplicate to verify identity
        Task<Guid> GetKey();

        // separate label that can be set
        Task<string> GetLabel();

        Task SetLabel(string label);

        Task<string> GetRuntimeInstanceId();

        Task<string> GetActivationId();
    }

    public interface IProxyGrain : IGrainWithIntegerKey
    {
        Task CreateProxy(long key);

        Task<string> GetRuntimeInstanceId();

        Task<string> GetProxyRuntimeInstanceId();
    }

    internal interface IStressSelfManagedGrain : IGrainWithIntegerKey
    {
        Task<string> GetLabel();

        Task SetLabel(string label);

        Task PingOthers(long[] others);

        Task<List<Tuple<GrainId, int, List<Tuple<SiloAddress, ActivationId>>>>> LookUpMany(SiloAddress destination, List<Tuple<GrainId, int>> grainAndETagList, int retries = 0);

        Task Send(byte[] data);

        Task<byte[]> Echo(byte[] data);

        Task Ping(byte[] data);

        Task PingWithDelay(byte[] data, TimeSpan delay);

        Task<IStressSelfManagedGrain> GetGrainReference();

        Task DeactivateSelf();
    }

    internal interface IReentrantStressSelfManagedGrain : IGrainWithIntegerKey
    {
        Task<byte[]> Echo(byte[] data);

        Task<string> GetRuntimeInstanceId();

        Task Ping(byte[] data);

        Task PingWithDelay(byte[] data, TimeSpan delay);

        Task PingMutableArray(byte[] data, long nextGrain, bool nextGrainIsRemote);

        Task PingImmutableArray(Immutable<byte[]> data, long nextGrain, bool nextGrainIsRemote);

        Task PingMutableDictionary(Dictionary<int, string> data, long nextGrain, bool nextGrainIsRemote);

        Task PingImmutableDictionary(Immutable<Dictionary<int, string>> data, long nextGrain, bool nextGrainIsRemote);

        Task InterleavingConsistencyTest(int numItems);
    }

    public interface IReentrantLocalStressSelfManagedGrain : IGrainWithIntegerKey
    {
        Task<byte[]> Echo(byte[] data);

        Task<string> GetRuntimeInstanceId();

        Task Ping(byte[] data);

        Task PingWithDelay(byte[] data, TimeSpan delay);

        Task PingMutableArray(byte[] data, long nextGrain, bool nextGrainIsRemote);

        Task PingImmutableArray(Immutable<byte[]> data, long nextGrain, bool nextGrainIsRemote);

        Task PingMutableDictionary(Dictionary<int, string> data, long nextGrain, bool nextGrainIsRemote);

        Task PingImmutableDictionary(Immutable<Dictionary<int, string>> data, long nextGrain, bool nextGrainIsRemote);
    }
}
