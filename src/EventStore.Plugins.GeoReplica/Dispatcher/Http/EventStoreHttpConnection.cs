using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Newtonsoft.Json;

namespace EventStore.Plugins.GeoReplica.Dispatcher.Http
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

        public async Task AppendToStreamAsync(string stream, dynamic[] events)
        {
            var url = Endpoint + "streams/" + stream;
            var eventDatas = ToEventData(events);
            var body = ToHttpEvents(eventDatas);
            var json = JsonConvert.SerializeObject(body);
            var responseMessage = await _httpClient.PostAsync(url, new StringContent(json, Encoding.UTF8, "application/vnd.eventstore.atom+json"));
            await responseMessage.Content.ReadAsStringAsync();
            if (!responseMessage.IsSuccessStatusCode)
                throw new Exception($"Request failed: {responseMessage.ReasonPhrase}");
        }

        private static IEnumerable<EventData> ToEventData(dynamic[] eventData)
        {
            return eventData.Cast<EventData>().ToArray();
        }

        private static HttpEvent[] ToHttpEvents(IEnumerable<EventData> eventDatas)
        {
            return eventDatas.Select(eventData => new HttpEvent(eventData.EventId.ToString(), eventData.Type,
                Encoding.UTF8.GetString(eventData.Data), Encoding.UTF8.GetString(eventData.Metadata))).ToArray();
        }

        /// <summary>
        /// Create a new <see cref="HttpClient"/> with the configured timeout.
        /// </summary>
        private HttpClient GetClient()
        {
            var handler = GetHandler();
            var client = new HttpClient(handler, true);
            client.DefaultRequestHeaders.Accept.Clear();
            if (_settings.ConnectionTimeout.HasValue)
                client.Timeout = _settings.ConnectionTimeout.Value;
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Add("ES-EventType", "GeoReplicaEventDispatched"); // Without this the operation throw an exception
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

    public class HttpEvent
    {
        public string EventId { get; }
        public string EventType { get; }
        public string Data { get; }
        public string Metadata { get; }

        public HttpEvent(string eventId, string eventType, string data, string metadata)
        {
            EventId = eventId;
            EventType = eventType;
            Data = data;
            Metadata = metadata;
        }
    }
}
