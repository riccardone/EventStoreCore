using System;
using System.Collections.Generic;
using System.Security.Principal;
using EventStore.Common.Log;
using EventStore.Core.Messaging;

namespace EventStore.Core.Services.Transport.Amqp
{
    public abstract class AmqpDispatcher: IAmqpDispatcher
    {
        private static readonly ILogger Log = LogManager.GetLoggerFor<AmqpDispatcher>();

        private readonly Func<AmqpPackage, IEnvelope, IPrincipal, string, string, AmqpConnectionManager, Message>[] _unwrappers;
        private readonly IDictionary<Type, Func<Message, AmqpPackage>> _wrappers;

        protected AmqpDispatcher()
        {
            _unwrappers = new Func<AmqpPackage, IEnvelope, IPrincipal, string, string, AmqpConnectionManager, Message>[255];
            _wrappers = new Dictionary<Type, Func<Message, AmqpPackage>>();
        }

        protected void AddWrapper<T>(Func<T, AmqpPackage> wrapper) where T : Message
        {
            _wrappers[typeof(T)] = x => wrapper((T)x);
        }

        protected void AddUnwrapper<T>(AmqpCommand command, Func<AmqpPackage, IEnvelope, T> unwrapper) where T : Message
        {
            _unwrappers[(byte)command] = (pkg, env, user, login, pass, conn) => unwrapper(pkg, env);
        }

        protected void AddUnwrapper<T>(AmqpCommand command, Func<AmqpPackage, IEnvelope, AmqpConnectionManager, T> unwrapper) where T : Message
        {
            _unwrappers[(byte)command] = (pkg, env, user, login, pass, conn) => unwrapper(pkg, env, conn);
        }

        protected void AddUnwrapper<T>(AmqpCommand command, Func<AmqpPackage, IEnvelope, IPrincipal, T> unwrapper) where T : Message
        {
            _unwrappers[(byte)command] = (pkg, env, user, login, pass, conn) => unwrapper(pkg, env, user);
        }

        protected void AddUnwrapper<T>(AmqpCommand command, Func<AmqpPackage, IEnvelope, IPrincipal, string, string, T> unwrapper) where T : Message
        {
            _unwrappers[(byte)command] = (pkg, env, user, login, pass, conn) => unwrapper(pkg, env, user, login, pass);
        }

        protected void AddUnwrapper<T>(AmqpCommand command, Func<AmqpPackage, IEnvelope, IPrincipal, string, string, AmqpConnectionManager, T> unwrapper) 
            where T : Message
        {
// ReSharper disable RedundantCast
            _unwrappers[(byte)command] = (Func<AmqpPackage, IEnvelope, IPrincipal, string, string, AmqpConnectionManager, Message>)unwrapper;
// ReSharper restore RedundantCast
        }

        public AmqpPackage? WrapMessage(Message message)
        {
            if (message == null)
                throw new ArgumentNullException("message");

            try
            {
                Func<Message, AmqpPackage> wrapper;
                if (_wrappers.TryGetValue(message.GetType(), out wrapper))
                    return wrapper(message);
            }
            catch (Exception exc)
            {
                Log.ErrorException(exc, "Error while wrapping message {0}.", message);
            }
            return null;
        }

        public Message UnwrapPackage(AmqpPackage package, IEnvelope envelope, IPrincipal user, string login, string pass, AmqpConnectionManager connection)
        {
            if (envelope == null)
                throw new ArgumentNullException("envelope");

            var unwrapper = _unwrappers[(byte)package.Command];
            if (unwrapper != null)
            {
                try
                {
                    return unwrapper(package, envelope, user, login, pass, connection);
                }
                catch (Exception exc)
                {
                    Log.ErrorException(exc, "Error while unwrapping TcpPackage with command {0}.", package.Command);
                }
            }
            return null;
        }
    }
}