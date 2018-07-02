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

        //public async Task AppendToStreamAsync(long expectedVersion, dynamic evt, byte[] metadata)
        //{
        //    var url = Endpoint + "/streams/" + evt.Event.EventStreamId;
        //    using (var request = new HttpRequestMessage(HttpMethod.Post, url))
        //    {
        //        request.Content = new StringContent(JsonConvert.SerializeObject(new List<dynamic>
        //        {
        //            new
        //            {
        //                evt.Event.EventId,
        //                evt.Event.EventType,
        //                Data = Encoding.UTF8.GetString(evt.Event.Data),
        //                Metadata = Encoding.UTF8.GetString(metadata)
        //            }
        //        }), Encoding.UTF8, "application/vnd.eventstore.events+json");
        //        request.Headers.Add("ES-ExpectedVersion", expectedVersion.ToString());
        //        request.Headers.Add("ES-EventId", evt.Event.EventId.ToString());
        //        request.Headers.Add("ES-EventType", evt.Event.EventType);
        //        var result = await _httpClientProxy.SendAsync(_httpClient, request).ConfigureAwait(false);
        //        if (!result.IsSuccessStatusCode)
        //        {
        //            // TODO differentiate the WrongExpectedVersion and any other error
        //            throw new EventStoreHttpException(result.Content.ToString(), result.ReasonPhrase, result.StatusCode);
        //        }
        //    }
        //}

        public async Task AppendToStreamAsync(string stream, dynamic[] events)
        {
            throw new NotImplementedException();
            var url = Endpoint + "/streams/" + stream;
            var eventDatas = ToEventData(events);
            var body = ToHttpEvents(eventDatas);
            using (var client = new HttpClient())
            {
                try
                {
                    client.Timeout = new TimeSpan(0, 0, 5);
                    client.DefaultRequestHeaders.Accept.Clear();
                    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                    var json = JsonConvert.SerializeObject(body);

                    var responseMessage = await client.PostAsync(url, new StringContent(json, Encoding.UTF8, "application/vnd.eventstore.atom+json"));

                    var response = await responseMessage.Content.ReadAsStringAsync();

                    if (responseMessage.IsSuccessStatusCode)
                    {
                        return;
                    }
                    else
                    {
                        throw new Exception("Request failed");
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
                
            }
            //using (var webClient = new WebClient())
            //{
            //    webClient.Headers.Add("Content-Type", "application/vnd.eventstore.atom+json");
            //    webClient.Headers.Add("ES-ExpectedVersion", ExpectedVersion.Any.ToString());
            //    var body = ToHttpEvents(eventDatas);
            //    var bodyAsString = JsonConvert.SerializeObject(body);
            //    try
            //    {
            //        var result = webClient.UploadString(new Uri(url), bodyAsString);
            //    }
            //    catch (Exception e)
            //    {
            //        throw;
            //    }

            //    return Task.CompletedTask;
            //}
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
