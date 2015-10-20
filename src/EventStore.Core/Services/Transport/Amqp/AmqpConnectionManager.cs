using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Security.Principal;
using System.Threading;
using EventStore.Common.Log;
using EventStore.Common.Utils;
using EventStore.Core.Authentication;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.TimerService;
using EventStore.Core.Services.Transport.Tcp;
using EventStore.Transport.Tcp;
using EventStore.Transport.Tcp.Framing;

namespace EventStore.Core.Services.Transport.Amqp
{
    /// <summary>
    /// Manager for individual TCP connection. It handles connection lifecycle,
    /// heartbeats, message framing and dispatch to the memory bus.
    /// </summary>
    public class AmqpConnectionManager : IHandle<AmqpMessage.Heartbeat>, IHandle<AmqpMessage.HeartbeatTimeout>
    {
        public const int ConnectionQueueSizeThreshold = 50000;
        public static readonly TimeSpan ConnectionTimeout = TimeSpan.FromMilliseconds(1000);

        private static readonly ILogger Log = LogManager.GetLoggerFor<TcpConnectionManager>();

        public readonly Guid ConnectionId;
        public readonly string ConnectionName;
        public readonly IPEndPoint RemoteEndPoint;
        public IPEndPoint LocalEndPoint { get { return _connection.LocalEndPoint; } }
        public bool IsClosed { get { return _isClosed != 0; } }
        public int SendQueueSize { get { return _connection.SendQueueSize; } }

        private readonly ITcpConnection _connection;
        private readonly IEnvelope _tcpEnvelope;
        private readonly IPublisher _publisher;
        private readonly IAmqpDispatcher _dispatcher;
        private readonly IMessageFramer _framer;
        private int _messageNumber;
        private int _isClosed;

        private readonly Action<TcpConnectionManager, SocketError> _connectionClosed;
        private readonly Action<TcpConnectionManager> _connectionEstablished;

        private readonly SendToWeakThisEnvelope _weakThisEnvelope;
        private readonly TimeSpan _heartbeatInterval;
        private readonly TimeSpan _heartbeatTimeout;

        private readonly IAuthenticationProvider _authProvider;
        private UserCredentials _defaultUser;

        public AmqpConnectionManager(string connectionName, 
                                    IAmqpDispatcher dispatcher, 
                                    IPublisher publisher, 
                                    ITcpConnection openedConnection,
                                    IPublisher networkSendQueue,
                                    IAuthenticationProvider authProvider,
                                    TimeSpan heartbeatInterval,
                                    TimeSpan heartbeatTimeout,
                                    Action<TcpConnectionManager, SocketError> onConnectionClosed)
        {
            Ensure.NotNull(dispatcher, "dispatcher");
            Ensure.NotNull(publisher, "publisher");
            Ensure.NotNull(openedConnection, "openedConnnection");
            Ensure.NotNull(networkSendQueue, "networkSendQueue");
            Ensure.NotNull(authProvider, "authProvider");

            ConnectionId = openedConnection.ConnectionId;
            ConnectionName = connectionName;

            _tcpEnvelope = new SendOverTcpEnvelope(this, networkSendQueue);
            _publisher = publisher;
            _dispatcher = dispatcher;
            _authProvider = authProvider;

            _framer = new LengthPrefixMessageFramer();
            _framer.RegisterMessageArrivedCallback(OnMessageArrived);

            _weakThisEnvelope = new SendToWeakThisEnvelope(this);
            _heartbeatInterval = heartbeatInterval;
            _heartbeatTimeout = heartbeatTimeout;

            _connectionClosed = onConnectionClosed;

            RemoteEndPoint = openedConnection.RemoteEndPoint;
            _connection = openedConnection;
            _connection.ConnectionClosed += OnConnectionClosed;
            if (_connection.IsClosed)
            {
                OnConnectionClosed(_connection, SocketError.Success);
                return;
            }

            ScheduleHeartbeat(0);
        }

        public AmqpConnectionManager(string connectionName, 
                                    Guid connectionId,
                                    IAmqpDispatcher dispatcher,
                                    IPublisher publisher,
                                    IPEndPoint remoteEndPoint, 
                                    TcpClientConnector connector,
                                    bool useSsl,
                                    string sslTargetHost,
                                    bool sslValidateServer,
                                    IPublisher networkSendQueue,
                                    IAuthenticationProvider authProvider,
                                    TimeSpan heartbeatInterval,
                                    TimeSpan heartbeatTimeout,
                                    Action<TcpConnectionManager> onConnectionEstablished,
                                    Action<TcpConnectionManager, SocketError> onConnectionClosed)
        {
            Ensure.NotEmptyGuid(connectionId, "connectionId");
            Ensure.NotNull(dispatcher, "dispatcher");
            Ensure.NotNull(publisher, "publisher");
            Ensure.NotNull(authProvider, "authProvider");
            Ensure.NotNull(remoteEndPoint, "remoteEndPoint");
            Ensure.NotNull(connector, "connector");
            if (useSsl) Ensure.NotNull(sslTargetHost, "sslTargetHost");

            ConnectionId = connectionId;
            ConnectionName = connectionName;

            _tcpEnvelope = new SendOverTcpEnvelope(this, networkSendQueue);
            _publisher = publisher;
            _dispatcher = dispatcher;
            _authProvider = authProvider;

            _framer = new LengthPrefixMessageFramer();
            _framer.RegisterMessageArrivedCallback(OnMessageArrived);

            _weakThisEnvelope = new SendToWeakThisEnvelope(this);
            _heartbeatInterval = heartbeatInterval;
            _heartbeatTimeout = heartbeatTimeout;

            _connectionEstablished = onConnectionEstablished;
            _connectionClosed = onConnectionClosed;

            RemoteEndPoint = remoteEndPoint;
            _connection = useSsl 
                ? connector.ConnectSslTo(ConnectionId, remoteEndPoint, ConnectionTimeout,
                                         sslTargetHost, sslValidateServer, OnConnectionEstablished, OnConnectionFailed)
                : connector.ConnectTo(ConnectionId, remoteEndPoint, ConnectionTimeout, OnConnectionEstablished, OnConnectionFailed);
            _connection.ConnectionClosed += OnConnectionClosed;
            if (_connection.IsClosed)
                OnConnectionClosed(_connection, SocketError.Success);
        }

        private void OnConnectionEstablished(ITcpConnection connection)
        {
            Log.Info("Connection '{0}' ({1:B}) to [{2}] established.", ConnectionName, ConnectionId, connection.RemoteEndPoint);

            ScheduleHeartbeat(0);

            var handler = _connectionEstablished;
            if (handler != null)
                handler(this);
        }

        private void OnConnectionFailed(ITcpConnection connection, SocketError socketError)
        {
            if (Interlocked.CompareExchange(ref _isClosed, 1, 0) != 0) return;
            Log.Info("Connection '{0}' ({1:B}) to [{2}] failed: {3}.", ConnectionName, ConnectionId, connection.RemoteEndPoint, socketError);
            if (_connectionClosed != null)
                _connectionClosed(this, socketError);
        }

        private void OnConnectionClosed(ITcpConnection connection, SocketError socketError)
        {
            if (Interlocked.CompareExchange(ref _isClosed, 1, 0) != 0) return;
            Log.Info("Connection '{0}' [{1}, {2:B}] closed: {3}.", ConnectionName, connection.RemoteEndPoint, ConnectionId, socketError);
            if (_connectionClosed != null)
                _connectionClosed(this, socketError);
        }

        public void StartReceiving()
        {
            _connection.ReceiveAsync(OnRawDataReceived);
        }

        private void OnRawDataReceived(ITcpConnection connection, IEnumerable<ArraySegment<byte>> data)
        {
            Interlocked.Increment(ref _messageNumber);

            try
            {
                _framer.UnFrameData(data);
            }
            catch (PackageFramingException exc)
            {
                SendBadRequestAndClose(Guid.Empty, string.Format("Invalid TCP frame received. Error: {0}.", exc.Message));
                return;
            }
            _connection.ReceiveAsync(OnRawDataReceived);
        }

        private void OnMessageArrived(ArraySegment<byte> data)
        {
            AmqpPackage package;
            try
            {
                package = AmqpPackage.FromArraySegment(data);
            }
            catch (Exception e)
            {
                SendBadRequestAndClose(Guid.Empty, string.Format("Received bad network package. Error: {0}", e));
                return;
            }

            OnPackageReceived(package);
        }

        private void OnPackageReceived(AmqpPackage package)
        {
            try
            {
                ProcessPackage(package);
            }
            catch (Exception e)
            {
                SendBadRequestAndClose(package.CorrelationId, string.Format("Error while processing package. Error: {0}", e));
            }
        }

        private void ProcessPackage(AmqpPackage package)
        {
            switch (package.Command)
            {
                case AmqpCommand.HeartbeatResponseCommand:
                    break;
                case AmqpCommand.HeartbeatRequestCommand:
                    SendPackage(new AmqpPackage(AmqpCommand.HeartbeatResponseCommand, package.CorrelationId, null));
                    break;
                case AmqpCommand.BadRequest:
                {
                    var reason = string.Empty;
                    Helper.EatException(() => reason = Helper.UTF8NoBom.GetString(package.Data.Array, package.Data.Offset, package.Data.Count));
                    var exitMessage = 
                        string.Format("Bad request received from '{0}' [{1}, L{2}, {3:B}], will stop server. CorrelationId: {4:B}, Error: {5}.",
                                      ConnectionName, RemoteEndPoint, LocalEndPoint, ConnectionId, package.CorrelationId,
                                      reason.IsEmptyString() ? "<reason missing>" : reason);
                    Log.Error(exitMessage);
                    break;
                }
                case AmqpCommand.Authenticate:
                {
                    if ((package.Flags & AmqpFlags.Authenticated) == 0)
                        ReplyNotAuthenticated(package.CorrelationId, "No user credentials provided.");
                    else
                    {
                        var defaultUser = new UserCredentials(package.Login, package.Password, null);
                        Interlocked.Exchange(ref _defaultUser, defaultUser);
                        _authProvider.Authenticate(new AmqpDefaultAuthRequest(this, package.CorrelationId, defaultUser));
                    }
                    break;
                }
                default:
                {
                    var defaultUser = _defaultUser;
                    if ((package.Flags & AmqpFlags.Authenticated) != 0)
                    {
                        _authProvider.Authenticate(new AmqpAuthRequest(this, package, package.Login, package.Password));
                    }
                    else if (defaultUser != null)
                    {
                        if (defaultUser.User != null)
                            UnwrapAndPublishPackage(package, defaultUser.User, defaultUser.Login, defaultUser.Password);
                        else
                            _authProvider.Authenticate(new AmqpAuthRequest(this, package, defaultUser.Login, defaultUser.Password));
                    }
                    else
                    {
                        UnwrapAndPublishPackage(package, null, null, null);
                    }
                    break;
                }
            }
        }

        private void UnwrapAndPublishPackage(AmqpPackage package, IPrincipal user, string login, string password)
        {
            Message message = null;
            string error = "";
            try {
                message = _dispatcher.UnwrapPackage(package, _tcpEnvelope, user, login, password, this);
            }
            catch(Exception ex) {
                error = ex.Message;
            }
            if (message != null)
                _publisher.Publish(message);
            else
                SendBadRequest(package.CorrelationId,
                                       string.Format("Could not unwrap network package for command {0}.\n{1}", package.Command, error));
        }

        private void ReplyNotAuthenticated(Guid correlationId, string description)
        {
            _tcpEnvelope.ReplyWith(new AmqpMessage.NotAuthenticated(correlationId, description));
        }

        private void ReplyNotReady(Guid correlationId, string description)
        {
            _tcpEnvelope.ReplyWith(new ClientMessage.NotHandled(correlationId, TcpClientMessageDto.NotHandled.NotHandledReason.NotReady, description));
        }


        private void ReplyAuthenticated(Guid correlationId, UserCredentials userCredentials, IPrincipal user)
        {
            var authCredentials = new UserCredentials(userCredentials.Login, userCredentials.Password, user);
            Interlocked.CompareExchange(ref _defaultUser, authCredentials, userCredentials);
            _tcpEnvelope.ReplyWith(new AmqpMessage.Authenticated(correlationId));
        }

        public void SendBadRequestAndClose(Guid correlationId, string message)
        {
            Ensure.NotNull(message, "message");

            SendPackage(new AmqpPackage(AmqpCommand.BadRequest, correlationId, Helper.UTF8NoBom.GetBytes(message)), checkQueueSize: false);
            Log.Error("Closing connection '{0}' [{1}, L{2}, {3:B}] due to error. Reason: {4}",
                      ConnectionName, RemoteEndPoint, LocalEndPoint, ConnectionId, message);
            _connection.Close(message);
        }

        public void SendBadRequest(Guid correlationId, string message)
        {
            Ensure.NotNull(message, "message");

            SendPackage(new AmqpPackage(AmqpCommand.BadRequest, correlationId, Helper.UTF8NoBom.GetBytes(message)), checkQueueSize: false);
        }


        public void Stop(string reason = null)
        {
            Log.Trace("Closing connection '{0}' [{1}, L{2}, {3:B}] cleanly.{4}",
                      ConnectionName, RemoteEndPoint, LocalEndPoint, ConnectionId,
                      reason.IsEmpty() ? string.Empty : " Reason: " + reason);
            _connection.Close(reason);
        }

        public void SendMessage(Message message)
        {
            var package = _dispatcher.WrapMessage(message);
            if (package != null)
                SendPackage(package.Value);
        }

        private void SendPackage(AmqpPackage package, bool checkQueueSize = true)
        {
            if (IsClosed)
                return;

            int queueSize;
            if (checkQueueSize && (queueSize = _connection.SendQueueSize) > ConnectionQueueSizeThreshold)
            {
                SendBadRequestAndClose(Guid.Empty, string.Format("Connection queue size is too large: {0}.", queueSize));
                return;
            }

            var data = package.AsArraySegment();
            var framed = _framer.FrameData(data);
            _connection.EnqueueSend(framed);
        }

        public void Handle(AmqpMessage.Heartbeat message)
        {
            if (IsClosed) return;

            var msgNum = _messageNumber;
            if (message.MessageNumber != msgNum)
                ScheduleHeartbeat(msgNum);
            else
            {
                SendPackage(new AmqpPackage(AmqpCommand.HeartbeatRequestCommand, Guid.NewGuid(), null));

                _publisher.Publish(TimerMessage.Schedule.Create(_heartbeatTimeout, _weakThisEnvelope, new AmqpMessage.HeartbeatTimeout(msgNum)));
            }
        }

        public void Handle(AmqpMessage.HeartbeatTimeout message)
        {
            if (IsClosed) return;

            var msgNum = _messageNumber;
            if (message.MessageNumber != msgNum)
                ScheduleHeartbeat(msgNum);
            else
                Stop(string.Format("HEARTBEAT TIMEOUT at msgNum {0}", msgNum));
        }

        private void ScheduleHeartbeat(int msgNum)
        {
            _publisher.Publish(TimerMessage.Schedule.Create(_heartbeatInterval, _weakThisEnvelope, new TcpMessage.Heartbeat(msgNum)));
        }

        private class SendToWeakThisEnvelope : IEnvelope
        {
            private readonly WeakReference _receiver;

            public SendToWeakThisEnvelope(object receiver)
            {
                _receiver = new WeakReference(receiver);
            }

            public void ReplyWith<T>(T message) where T : Message
            {
                var x = _receiver.Target as IHandle<T>;
                if (x != null)
                    x.Handle(message);
            }
        }

        private class AmqpAuthRequest : AuthenticationRequest
        {
            private readonly AmqpConnectionManager _manager;
            private readonly AmqpPackage _package;

            public AmqpAuthRequest(AmqpConnectionManager manager, AmqpPackage package, string login, string password) 
                : base(login, password)
            {
                _manager = manager;
                _package = package;
            }

            public override void Unauthorized()
            {
                _manager.ReplyNotAuthenticated(_package.CorrelationId, "Not Authenticated");
            }

            public override void Authenticated(IPrincipal principal)
            {
                _manager.UnwrapAndPublishPackage(_package, principal, Name, SuppliedPassword);
            }

            public override void Error()
            {
                _manager.ReplyNotAuthenticated(_package.CorrelationId, "Internal Server Error");
            }

            public override void NotReady()
            {
                _manager.ReplyNotReady(_package.CorrelationId, "Server not ready");
            }
        }


        private class AmqpDefaultAuthRequest : AuthenticationRequest
        {
            private readonly AmqpConnectionManager _manager;
            private readonly Guid _correlationId;
            private readonly UserCredentials _userCredentials;

            public AmqpDefaultAuthRequest(AmqpConnectionManager manager, Guid correlationId, UserCredentials userCredentials)
                : base(userCredentials.Login, userCredentials.Password)
            {
                _manager = manager;
                _correlationId = correlationId;
                _userCredentials = userCredentials;
            }

            public override void Unauthorized()
            {
                _manager.ReplyNotAuthenticated(_correlationId, "Unauthorized");
            }

            public override void Authenticated(IPrincipal principal)
            {
                _manager.ReplyAuthenticated(_correlationId, _userCredentials, principal);
            }

            public override void Error()
            {
                _manager.ReplyNotAuthenticated(_correlationId, "Internal Server Error");
            }

            public override void NotReady()
            {
                _manager.ReplyNotAuthenticated(_correlationId, "Server not yet ready");
            }
        }

        private class UserCredentials
        {
            public readonly string Login;
            public readonly string Password;
            public readonly IPrincipal User;

            public UserCredentials(string login, string password, IPrincipal user)
            {
                Login = login;
                Password = password;
                User = user;
            }
        }
    }
}