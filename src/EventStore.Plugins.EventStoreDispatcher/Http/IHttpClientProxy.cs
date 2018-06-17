using System.Net.Http;
using System.Threading.Tasks;

namespace EventStore.Plugins.EventStoreDispatcher.Http
{
    public interface IHttpClientProxy
    {
        Task<HttpResponseMessage> SendAsync(HttpClient client, HttpRequestMessage request);
    }
}