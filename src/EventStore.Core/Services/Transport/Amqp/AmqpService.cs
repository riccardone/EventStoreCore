using System;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using EventStore.Common.Log;
using EventStore.Common.Utils;
using EventStore.Core.Authentication;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Services.Transport.Tcp;
using EventStore.Transport.Tcp;

namespace EventStore.Core.Services.Transport.Amqp
{
    public enum AmqpServiceType
    {
        Internal, 
        External
    }

    public enum AmqpSecurityType
    {
        Normal,
        Secure
    }

    public class AmqpService : IHandle<SystemMessage.SystemInit>,
                              IHandle<SystemMessage.SystemStart>,
                              IHandle<SystemMessage.BecomeShuttingDown>
    {
        private static readonly ILogger Log = LogManager.GetLoggerFor<AmqpService>();

        private readonly IPublisher _publisher;
        private readonly IPEndPoint _serverEndPoint;
        private readonly TcpServerListener _serverListener;
        private readonly IPublisher _networkSendQueue;
        private readonly AmqpServiceType _serviceType;
        private readonly AmqpSecurityType _securityType;
        private readonly Func<Guid, IPEndPoint, IAmqpDispatcher> _dispatcherFactory;
        private readonly TimeSpan _heartbeatInterval;
        private readonly TimeSpan _heartbeatTimeout;
        private readonly IAuthenticationProvider _authProvider;
        private readonly X509Certificate _certificate;

        public AmqpService(IPublisher publisher,
                          IPEndPoint serverEndPoint,
                          IPublisher networkSendQueue,
                          AmqpServiceType serviceType,
                          AmqpSecurityType securityType,
                          IAmqpDispatcher dispatcher,
                          TimeSpan heartbeatInterval,
                          TimeSpan heartbeatTimeout,
                          IAuthenticationProvider authProvider,
                          X509Certificate certificate)
            : this(publisher, serverEndPoint, networkSendQueue, serviceType, securityType, (_, __) => dispatcher, 
                   heartbeatInterval, heartbeatTimeout, authProvider, certificate)
        {
        }

        public AmqpService(IPublisher publisher, 
                          IPEndPoint serverEndPoint, 
                          IPublisher networkSendQueue,
                          AmqpServiceType serviceType,
                          AmqpSecurityType securityType,
                          Func<Guid, IPEndPoint, IAmqpDispatcher> dispatcherFactory,
                          TimeSpan heartbeatInterval,
                          TimeSpan heartbeatTimeout,
                          IAuthenticationProvider authProvider,
                          X509Certificate certificate)
        {
            Ensure.NotNull(publisher, "publisher");
            Ensure.NotNull(serverEndPoint, "serverEndPoint");
            Ensure.NotNull(networkSendQueue, "networkSendQueue");
            Ensure.NotNull(dispatcherFactory, "dispatcherFactory");
            Ensure.NotNull(authProvider, "authProvider");
            if (securityType == AmqpSecurityType.Secure)
                Ensure.NotNull(certificate, "certificate");

            _publisher = publisher;
            _serverEndPoint = serverEndPoint;
            _serverListener = new TcpServerListener(_serverEndPoint);
            _networkSendQueue = networkSendQueue;
            _serviceType = serviceType;
            _securityType = securityType;
            _dispatcherFactory = dispatcherFactory;
            _heartbeatInterval = heartbeatInterval;
            _heartbeatTimeout = heartbeatTimeout;
            _authProvider = authProvider;
            _certificate = certificate;
        }

        public void Handle(SystemMessage.SystemInit message)
        {
            try
            {
                //new EventStoreServer().Run();
                _serverListener.StartListening(OnConnectionAccepted, _securityType.ToString());
            }
            catch (Exception e)
            {
                Application.Exit(ExitCode.Error, e.Message);
            }
        }

        public void Handle(SystemMessage.SystemStart message)
        {
        }

        public void Handle(SystemMessage.BecomeShuttingDown message)
        {
            _serverListener.Stop();
        }

        private void OnConnectionAccepted(IPEndPoint endPoint, Socket socket)
        {
            var conn = _securityType == AmqpSecurityType.Secure 
                ? TcpConnectionSsl.CreateServerFromSocket(Guid.NewGuid(), endPoint, socket, _certificate, verbose: true)
                : AmqpConnection.CreateAcceptedAmqpConnection(Guid.NewGuid(), endPoint, socket, verbose: true);
            Log.Info("{0} Amqp connection accepted: [{1}, {2}, L{3}, {4:B}].", 
                     _serviceType, _securityType, conn.RemoteEndPoint, conn.LocalEndPoint, conn.ConnectionId);

            var dispatcher = _dispatcherFactory(conn.ConnectionId, _serverEndPoint);
            var manager = new AmqpConnectionManager(
                    string.Format("{0}-{1}", _serviceType.ToString().ToLower(), _securityType.ToString().ToLower()),
                    dispatcher,
                    _publisher,
                    conn,
                    _networkSendQueue,
                    _authProvider,
                    _heartbeatInterval,
                    _heartbeatTimeout,
                    (m, e) => _publisher.Publish(new AmqpMessage.ConnectionClosed(m, e))); // TODO AN: race condition
            _publisher.Publish(new AmqpMessage.ConnectionEstablished(manager));
            manager.StartReceiving();
        }
    }
}
