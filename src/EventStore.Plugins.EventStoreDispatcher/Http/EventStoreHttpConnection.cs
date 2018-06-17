using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace EventStore.Plugins.EventStoreDispatcher.Http
{
    public class EventStoreHttpConnection : IEventStoreHttpConnection, IDisposable
    {
        private readonly ConnectionSettings _settings;
        private readonly IHttpClientProxy _httpClientProxy;
        private readonly Action<IEventStoreHttpConnection, Exception> _errorHandler;
        private readonly HttpClient _httpClient;

        /// <summary>
        /// Creates a new <see cref="IEventStoreHttpConnection"/> to single node using default <see cref="ConnectionSettings"/>
        /// </summary>
        /// <param name="connectionSettings">The <see cref="ConnectionSettings"/> to apply to the new connection</param>
        /// <param name="endpoint">The endpoint to connect to.</param>
        /// <returns>a new <see cref="IEventStoreHttpConnection"/></returns>
        public static EventStoreHttpConnection Create(ConnectionSettings connectionSettings, Uri endpoint)
        {
            return new EventStoreHttpConnection(connectionSettings, endpoint.ToString());
        }

        /// <summary>
        /// Creates a new <see cref="IEventStoreHttpConnection"/> to single node using default <see cref="ConnectionSettings"/>
        /// </summary>
        /// <param name="endpoint">The endpoint to connect to.</param>
        /// <returns>a new <see cref="IEventStoreHttpConnection"/></returns>
        public static EventStoreHttpConnection Create(string endpoint)
        {
            return new EventStoreHttpConnection(ConnectionSettings.Default, endpoint);
        }

        /// <summary>
        /// Creates a new <see cref="IEventStoreHttpConnection"/> to single node using specific <see cref="ConnectionSettings"/>
        /// </summary>
        /// <param name="settings">The <see cref="ConnectionSettings"/> to apply to the new connection</param>
        /// <param name="endpoint">The endpoint to connect to.</param>
        /// <returns>a new <see cref="IEventStoreHttpConnection"/></returns>
        internal EventStoreHttpConnection(ConnectionSettings settings, string endpoint)
        {
            Ensure.NotNull(settings, "settings");
            Ensure.NotNull(endpoint, "endpoint");

            _httpClientProxy = settings.HttpClientProxy;
            _settings = settings;
            Endpoint = endpoint;
            _errorHandler = settings.ErrorHandler;
            ConnectionName = settings.ConnectionName;
            _httpClient = GetClient();
        }

        public string ConnectionName { get; }

        public string Endpoint { get; }

        public async Task AppendToStreamAsync(long expectedVersion, dynamic evt, byte[] metadata)
        {
            var url = Endpoint + "/streams/" + evt.Event.EventStreamId;
            using (var request = new HttpRequestMessage(HttpMethod.Post, url))
            {
                request.Content = new StringContent(JsonConvert.SerializeObject(new List<dynamic>
                {
                    new
                    {
                        evt.Event.EventId,
                        evt.Event.EventType,
                        Data = Encoding.UTF8.GetString(evt.Event.Data),
                        Metadata = Encoding.UTF8.GetString(metadata)
                    }
                }), Encoding.UTF8, "application/vnd.eventstore.events+json");
                request.Headers.Add("ES-ExpectedVersion", expectedVersion.ToString());
                request.Headers.Add("ES-EventId", evt.Event.EventId.ToString());
                request.Headers.Add("ES-EventType", evt.Event.EventType);
                var result = await _httpClientProxy.SendAsync(_httpClient, request).ConfigureAwait(false);
                if (!result.IsSuccessStatusCode)
                {
                    // TODO differentiate the WrongExpectedVersion and any other error
                    throw new EventStoreHttpException(result.Content.ToString(), result.ReasonPhrase, result.StatusCode);
                }
            }
        }

        /// <summary>
        /// Create a new <see cref="HttpClient"/> with the configured timeout.
        /// </summary>
        private HttpClient GetClient()
        {
            var handler = GetHandler();

            var client = new HttpClient(handler, true);

            if (_settings.ConnectionTimeout.HasValue)
            {
                client.Timeout = _settings.ConnectionTimeout.Value;
            }

            return client;
        }

        private HttpClientHandler GetHandler()
        {
            var defaultCredentials = _settings.DefaultUserCredentials;
            var credentials = defaultCredentials == null
                ? null
                : new NetworkCredential(defaultCredentials.Username, defaultCredentials.Password);

            return new HttpClientHandler { Credentials = credentials };
        }

        private void HandleError(Exception ex)
        {
            _errorHandler?.Invoke(this, ex);
        }

        public void Dispose()
        {
            _httpClient.Dispose();
        }
    }
}
