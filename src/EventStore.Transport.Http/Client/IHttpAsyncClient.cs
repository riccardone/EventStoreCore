using System;
using System.Collections.Generic;

namespace EventStore.Transport.Http.Client
{
    public interface IHttpAsyncClient : IHttpClient
    {
        void Get(string url, IEnumerable<KeyValuePair<string, string>> headers, TimeSpan timeout,
            Action<HttpResponse> onSuccess, Action<Exception> onException);

        void Post(string url, string body, string contentType, IEnumerable<KeyValuePair<string, string>> headers,
            TimeSpan timeout,
            Action<HttpResponse> onSuccess, Action<Exception> onException);
        
    }
}
