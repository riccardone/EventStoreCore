using System.Net.Http;
using System.Threading.Tasks;

namespace EventStore.Plugins.GeoReplica.Dispatcher.Http
{
    public interface IHttpClientProxy
    {
        Task<HttpResponseMessage> SendAsync(HttpClient client, HttpRequestMessage request);
    }
}