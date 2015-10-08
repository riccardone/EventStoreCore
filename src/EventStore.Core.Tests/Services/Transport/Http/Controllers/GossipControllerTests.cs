using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using EventStore.ClientAPI.Common.Utils;
using EventStore.Common.Log;
using EventStore.Core.Cluster;
using EventStore.Core.Messages;
using EventStore.Core.Services.Transport.Http.Controllers;
using EventStore.Core.Tests.Fakes;
using EventStore.Transport.Http;
using EventStore.Transport.Http.Client;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Transport.Http.Controllers
{
    public class FakeHttpAsynchClient : IHttpAsyncClient
    {
        public void Get(string url, TimeSpan timeout, Action<HttpResponse> onSuccess, Action<Exception> onException)
        {
            throw new NotImplementedException();
        }

        public void Post(string url, string request, string contentType, TimeSpan timeout, Action<HttpResponse> onSuccess, Action<Exception> onException)
        {
            onSuccess(new HttpResponse(null));
        }

        public void Delete(string url, TimeSpan timeout, Action<HttpResponse> onSuccess, Action<Exception> onException)
        {
            throw new NotImplementedException();
        }

        public void Put(string url, string request, string contentType, TimeSpan timeout, Action<HttpResponse> onSuccess, Action<Exception> onException)
        {
            throw new NotImplementedException();
        }

        public void Get(string url, IEnumerable<KeyValuePair<string, string>> headers, TimeSpan timeout, Action<HttpResponse> onSuccess, Action<Exception> onException)
        {
            throw new NotImplementedException();
        }

        public void Post(string url, string body, string contentType, IEnumerable<KeyValuePair<string, string>> headers, TimeSpan timeout, Action<HttpResponse> onSuccess,
            Action<Exception> onException)
        {
            throw new NotImplementedException();
        }
    }

    [TestFixture]
    public class GossipControllerTests
    {
        private GossipController _controller;
        private FakeLogger _logger = new FakeLogger();

        [SetUp]
        public void SetUp()
        {
            _controller = new GossipController(new FakePublisher(), new FakePublisher(), new TimeSpan(), new FakeHttpAsynchClient(), _logger);
        }

        [Test]
        public void log_two_line_is_better_than_one()
        {
            // set up
            var serverEndPoint = new IPEndPoint(1, 1);
            var endPoint = new IPEndPoint(2, 2);
            var gossipMessageCausingLog = new GossipMessage.SendGossip(new ClusterInfo(), serverEndPoint) ;

            // act
            _controller.Send(gossipMessageCausingLog, endPoint);
            var res = _logger.ToJson();
            

            // verify
            Assert.IsTrue(res != null);
        }
    }
}
