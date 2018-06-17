using System.Net.Http;
using System.Threading.Tasks;

namespace EventStore.Plugins.EventStoreDispatcher.Http
{
    public class HttpClientProxy : IHttpClientProxy
    {
        public async Task<HttpResponseMessage> SendAsync(HttpClient client, HttpRequestMessage request)
        {
            return await client.SendAsync(request).ConfigureAwait(false);
        }
    }
}
