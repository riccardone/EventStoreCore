using System;
using System.Linq;
using System.Security.Principal;
using Amqp;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;

namespace EventStore.Core.Services.Transport.Amqp
{
    public class ClientAmqpDispatcher : AmqpDispatcher
    {
        public ClientAmqpDispatcher()
        {
            AddUnwrapper(AmqpCommand.Ping, UnwrapPing);
            AddWrapper<AmqpMessage.PongMessage>(WrapPong);

            AddUnwrapper(AmqpCommand.WriteEvents, UnwrapWriteEvents);
            AddWrapper<ClientMessage.WriteEvents>(WrapWriteEvents);
            AddUnwrapper(AmqpCommand.WriteEventsCompleted, UnwrapWriteEventsCompleted);
            AddWrapper<ClientMessage.WriteEventsCompleted>(WrapWriteEventsCompleted);

            AddUnwrapper(AmqpCommand.TransactionStart, UnwrapTransactionStart);
            AddWrapper<ClientMessage.TransactionStart>(WrapTransactionStart);
            AddUnwrapper(AmqpCommand.TransactionStartCompleted, UnwrapTransactionStartCompleted);
            AddWrapper<ClientMessage.TransactionStartCompleted>(WrapTransactionStartCompleted);

            AddUnwrapper(AmqpCommand.TransactionWrite, UnwrapTransactionWrite);
            AddWrapper<ClientMessage.TransactionWrite>(WrapTransactionWrite);
            AddUnwrapper(AmqpCommand.TransactionWriteCompleted, UnwrapTransactionWriteCompleted);
            AddWrapper<ClientMessage.TransactionWriteCompleted>(WrapTransactionWriteCompleted);

            AddUnwrapper(AmqpCommand.TransactionCommit, UnwrapTransactionCommit);
            AddWrapper<ClientMessage.TransactionCommit>(WrapTransactionCommit);
            AddUnwrapper(AmqpCommand.TransactionCommitCompleted, UnwrapTransactionCommitCompleted);
            AddWrapper<ClientMessage.TransactionCommitCompleted>(WrapTransactionCommitCompleted);

            AddUnwrapper(AmqpCommand.DeleteStream, UnwrapDeleteStream);
            AddWrapper<ClientMessage.DeleteStream>(WrapDeleteStream);
            AddUnwrapper(AmqpCommand.DeleteStreamCompleted, UnwrapDeleteStreamCompleted);
            AddWrapper<ClientMessage.DeleteStreamCompleted>(WrapDeleteStreamCompleted);

            AddUnwrapper(AmqpCommand.ReadEvent, UnwrapReadEvent);
            AddWrapper<ClientMessage.ReadEventCompleted>(WrapReadEventCompleted);

            AddUnwrapper(AmqpCommand.ReadStreamEventsForward, UnwrapReadStreamEventsForward);
            AddWrapper<ClientMessage.ReadStreamEventsForwardCompleted>(WrapReadStreamEventsForwardCompleted);
            AddUnwrapper(AmqpCommand.ReadStreamEventsBackward, UnwrapReadStreamEventsBackward);
            AddWrapper<ClientMessage.ReadStreamEventsBackwardCompleted>(WrapReadStreamEventsBackwardCompleted);

            AddUnwrapper(AmqpCommand.ReadAllEventsForward, UnwrapReadAllEventsForward);
            AddWrapper<ClientMessage.ReadAllEventsForwardCompleted>(WrapReadAllEventsForwardCompleted);
            AddUnwrapper(AmqpCommand.ReadAllEventsBackward, UnwrapReadAllEventsBackward);
            AddWrapper<ClientMessage.ReadAllEventsBackwardCompleted>(WrapReadAllEventsBackwardCompleted);

            AddUnwrapper(AmqpCommand.SubscribeToStream, UnwrapSubscribeToStream);
            AddUnwrapper(AmqpCommand.UnsubscribeFromStream, UnwrapUnsubscribeFromStream);

            AddWrapper<ClientMessage.SubscriptionConfirmation>(WrapSubscribedToStream);
            AddWrapper<ClientMessage.StreamEventAppeared>(WrapStreamEventAppeared);
            AddWrapper<ClientMessage.SubscriptionDropped>(WrapSubscriptionDropped);
            AddUnwrapper(AmqpCommand.CreatePersistentSubscription, UnwrapCreatePersistentSubscription);
            AddUnwrapper(AmqpCommand.DeletePersistentSubscription, UnwrapDeletePersistentSubscription);
            AddWrapper<ClientMessage.CreatePersistentSubscriptionCompleted>(WrapCreatePersistentSubscriptionCompleted);
            AddWrapper<ClientMessage.DeletePersistentSubscriptionCompleted>(WrapDeletePersistentSubscriptionCompleted);
            AddUnwrapper(AmqpCommand.UpdatePersistentSubscription, UnwrapUpdatePersistentSubscription);
            AddWrapper<ClientMessage.UpdatePersistentSubscriptionCompleted>(WrapUpdatePersistentSubscriptionCompleted);


            AddUnwrapper(AmqpCommand.ConnectToPersistentSubscription, UnwrapConnectToPersistentSubscription);
            AddUnwrapper(AmqpCommand.PersistentSubscriptionAckEvents, UnwrapPersistentSubscriptionAckEvents);
            AddUnwrapper(AmqpCommand.PersistentSubscriptionNakEvents, UnwrapPersistentSubscriptionNackEvents);
            AddWrapper<ClientMessage.PersistentSubscriptionConfirmation>(WrapPersistentSubscriptionConfirmation);
            AddWrapper<ClientMessage.PersistentSubscriptionStreamEventAppeared>(WrapPersistentSubscriptionStreamEventAppeared);

            AddUnwrapper(AmqpCommand.ScavengeDatabase, UnwrapScavengeDatabase);
            AddWrapper<ClientMessage.ScavengeDatabaseCompleted>(WrapScavengeDatabaseResponse);

            AddWrapper<ClientMessage.NotHandled>(WrapNotHandled);
            AddUnwrapper(AmqpCommand.NotHandled, UnwrapNotHandled);

            AddWrapper<AmqpMessage.NotAuthenticated>(WrapNotAuthenticated);
            AddWrapper<AmqpMessage.Authenticated>(WrapAuthenticated);
        }

        private static Messaging.Message UnwrapPing(AmqpPackage package, IEnvelope envelope)
        {
            var data = new byte[package.Data.Count];
            Buffer.BlockCopy(package.Data.Array, package.Data.Offset, data, 0, package.Data.Count);
            var pongMessage = new AmqpMessage.PongMessage(package.CorrelationId, data);
            envelope.ReplyWith(pongMessage);
            return pongMessage;
        }

        private static AmqpPackage WrapPong(AmqpMessage.PongMessage message)
        {
            return new AmqpPackage(AmqpCommand.Pong, message.CorrelationId, message.Payload);
        }

        private static ClientMessage.WriteEvents UnwrapWriteEvents(AmqpPackage package, IEnvelope envelope,
                                                                   IPrincipal user, string login, string password)
        {
            //var bBuffer = new ByteBuffer(package.AsByteArray(), 0, package.Data.Count, package.Data.Array.Length);
            //var msg = AmqpLite.Message.Decode(bBuffer);

            var dto = package.Data.Deserialize<AmqpClientMessageDto.WriteEvents>();
            if (dto == null) return null;

            var events = new Event[dto.Events == null ? 0 : dto.Events.Length];
            for (int i = 0; i < events.Length; ++i)
            {
                // ReSharper disable PossibleNullReferenceException
                var e = dto.Events[i];
                // ReSharper restore PossibleNullReferenceException
                events[i] = new Event(new Guid(e.EventId), e.EventType, e.DataContentType == 1, e.Data, e.Metadata);
            }
            return new ClientMessage.WriteEvents(Guid.NewGuid(), package.CorrelationId, envelope, dto.RequireMaster,
                                                 dto.EventStreamId, dto.ExpectedVersion, events, user, login, password);
        }

        private static AmqpPackage WrapWriteEvents(ClientMessage.WriteEvents msg)
        {
            var events = new TcpClientMessageDto.NewEvent[msg.Events.Length];
            for (int i = 0; i < events.Length; ++i)
            {
                var e = msg.Events[i];
                events[i] = new TcpClientMessageDto.NewEvent(e.EventId.ToByteArray(),
                                                             e.EventType,
                                                             e.IsJson ? 1 : 0,
                                                             0, e.Data,
                                                             e.Metadata);
            }
            var dto = new TcpClientMessageDto.WriteEvents(msg.EventStreamId, msg.ExpectedVersion, events, msg.RequireMaster);
            return CreateWriteRequestPackage(AmqpCommand.WriteEvents, msg, dto);
        }

        private static AmqpPackage CreateWriteRequestPackage(AmqpCommand command, ClientMessage.WriteRequestMessage msg, object dto)
        {
            // we forwarding with InternalCorrId, not client's CorrelationId!!!
            return msg.Login != null && msg.Password != null
                ? new AmqpPackage(command, AmqpFlags.Authenticated, msg.InternalCorrId, msg.Login, msg.Password, dto.Serialize())
                : new AmqpPackage(command, AmqpFlags.None, msg.InternalCorrId, null, null, dto.Serialize());
        }

        private static ClientMessage.WriteEventsCompleted UnwrapWriteEventsCompleted(AmqpPackage package, IEnvelope envelope)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.WriteEventsCompleted>();
            if (dto == null) return null;
            if (dto.Result == TcpClientMessageDto.OperationResult.Success)
                return new ClientMessage.WriteEventsCompleted(package.CorrelationId,
                                                             dto.FirstEventNumber,
                                                             dto.LastEventNumber,
                                                             dto.PreparePosition ?? -1,
                                                             dto.CommitPosition ?? -1);
            return new ClientMessage.WriteEventsCompleted(package.CorrelationId,
                                                          (OperationResult)dto.Result,
                                                          dto.Message);
        }

        private static AmqpPackage WrapWriteEventsCompleted(ClientMessage.WriteEventsCompleted msg)
        {
            var dto = new TcpClientMessageDto.WriteEventsCompleted((TcpClientMessageDto.OperationResult)msg.Result,
                                                                   msg.Message,
                                                                   msg.FirstEventNumber,
                                                                   msg.LastEventNumber,
                                                                   msg.PreparePosition,
                                                                   msg.CommitPosition);
            return new AmqpPackage(AmqpCommand.WriteEventsCompleted, msg.CorrelationId, dto.Serialize());
        }

        private static ClientMessage.TransactionStart UnwrapTransactionStart(AmqpPackage package, IEnvelope envelope,
                                                                             IPrincipal user, string login, string password)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.TransactionStart>();
            if (dto == null) return null;
            return new ClientMessage.TransactionStart(Guid.NewGuid(), package.CorrelationId, envelope, dto.RequireMaster,
                                                      dto.EventStreamId, dto.ExpectedVersion, user, login, password);
        }

        private static AmqpPackage WrapTransactionStart(ClientMessage.TransactionStart msg)
        {
            var dto = new TcpClientMessageDto.TransactionStart(msg.EventStreamId, msg.ExpectedVersion, msg.RequireMaster);
            return CreateWriteRequestPackage(AmqpCommand.TransactionStart, msg, dto);
        }

        private static ClientMessage.TransactionStartCompleted UnwrapTransactionStartCompleted(AmqpPackage package, IEnvelope envelope)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.TransactionStartCompleted>();
            if (dto == null) return null;
            return new ClientMessage.TransactionStartCompleted(package.CorrelationId, dto.TransactionId, (OperationResult)dto.Result, dto.Message);
        }

        private static AmqpPackage WrapTransactionStartCompleted(ClientMessage.TransactionStartCompleted msg)
        {
            var dto = new TcpClientMessageDto.TransactionStartCompleted(msg.TransactionId, (TcpClientMessageDto.OperationResult)msg.Result, msg.Message);
            return new AmqpPackage(AmqpCommand.TransactionStartCompleted, msg.CorrelationId, dto.Serialize());
        }

        private static ClientMessage.TransactionWrite UnwrapTransactionWrite(AmqpPackage package, IEnvelope envelope,
                                                                             IPrincipal user, string login, string password)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.TransactionWrite>();
            if (dto == null) return null;

            var events = new Event[dto.Events == null ? 0 : dto.Events.Length];
            for (int i = 0; i < events.Length; ++i)
            {
                // ReSharper disable PossibleNullReferenceException
                var e = dto.Events[i];
                // ReSharper restore PossibleNullReferenceException
                events[i] = new Event(new Guid(e.EventId), e.EventType, e.DataContentType == 1, e.Data, e.Metadata);
            }
            return new ClientMessage.TransactionWrite(Guid.NewGuid(), package.CorrelationId, envelope, dto.RequireMaster,
                                                      dto.TransactionId, events, user, login, password);
        }

        private static AmqpPackage WrapTransactionWrite(ClientMessage.TransactionWrite msg)
        {
            var events = new TcpClientMessageDto.NewEvent[msg.Events.Length];
            for (int i = 0; i < events.Length; ++i)
            {
                var e = msg.Events[i];
                events[i] = new TcpClientMessageDto.NewEvent(e.EventId.ToByteArray(), e.EventType, e.IsJson ? 1 : 0, 0, e.Data, e.Metadata);
            }
            var dto = new TcpClientMessageDto.TransactionWrite(msg.TransactionId, events, msg.RequireMaster);
            return CreateWriteRequestPackage(AmqpCommand.TransactionWrite, msg, dto);
        }

        private static ClientMessage.TransactionWriteCompleted UnwrapTransactionWriteCompleted(AmqpPackage package, IEnvelope envelope)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.TransactionWriteCompleted>();
            if (dto == null) return null;
            return new ClientMessage.TransactionWriteCompleted(package.CorrelationId, dto.TransactionId, (OperationResult)dto.Result, dto.Message);
        }

        private static AmqpPackage WrapTransactionWriteCompleted(ClientMessage.TransactionWriteCompleted msg)
        {
            var dto = new TcpClientMessageDto.TransactionWriteCompleted(msg.TransactionId, (TcpClientMessageDto.OperationResult)msg.Result, msg.Message);
            return new AmqpPackage(AmqpCommand.TransactionWriteCompleted, msg.CorrelationId, dto.Serialize());
        }

        private static ClientMessage.TransactionCommit UnwrapTransactionCommit(AmqpPackage package, IEnvelope envelope,
                                                                               IPrincipal user, string login, string password)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.TransactionCommit>();
            if (dto == null) return null;
            return new ClientMessage.TransactionCommit(Guid.NewGuid(), package.CorrelationId, envelope, dto.RequireMaster,
                                                       dto.TransactionId, user, login, password);
        }

        private static AmqpPackage WrapTransactionCommit(ClientMessage.TransactionCommit msg)
        {
            var dto = new TcpClientMessageDto.TransactionCommit(msg.TransactionId, msg.RequireMaster);
            return CreateWriteRequestPackage(AmqpCommand.TransactionCommit, msg, dto);
        }

        private static ClientMessage.TransactionCommitCompleted UnwrapTransactionCommitCompleted(AmqpPackage package, IEnvelope envelope)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.TransactionCommitCompleted>();
            if (dto == null) return null;
            if (dto.Result == TcpClientMessageDto.OperationResult.Success)
                return new ClientMessage.TransactionCommitCompleted(package.CorrelationId, dto.TransactionId, dto.FirstEventNumber, dto.LastEventNumber, dto.PreparePosition ?? -1, dto.CommitPosition ?? -1);
            return new ClientMessage.TransactionCommitCompleted(package.CorrelationId, dto.TransactionId, (OperationResult)dto.Result, dto.Message);
        }

        private static AmqpPackage WrapTransactionCommitCompleted(ClientMessage.TransactionCommitCompleted msg)
        {
            var dto = new TcpClientMessageDto.TransactionCommitCompleted(msg.TransactionId, (TcpClientMessageDto.OperationResult)msg.Result,
                                                                         msg.Message, msg.FirstEventNumber, msg.LastEventNumber, msg.PreparePosition, msg.CommitPosition);
            return new AmqpPackage(AmqpCommand.TransactionCommitCompleted, msg.CorrelationId, dto.Serialize());
        }

        private static ClientMessage.DeleteStream UnwrapDeleteStream(AmqpPackage package, IEnvelope envelope,
                                                                     IPrincipal user, string login, string password)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.DeleteStream>();
            if (dto == null) return null;
            return new ClientMessage.DeleteStream(Guid.NewGuid(), package.CorrelationId, envelope, dto.RequireMaster,
                                                  dto.EventStreamId, dto.ExpectedVersion, dto.HardDelete ?? false, user, login, password);
        }

        private static AmqpPackage WrapDeleteStream(ClientMessage.DeleteStream msg)
        {
            var dto = new TcpClientMessageDto.DeleteStream(msg.EventStreamId, msg.ExpectedVersion, msg.RequireMaster, msg.HardDelete);
            return CreateWriteRequestPackage(AmqpCommand.DeleteStream, msg, dto);
        }

        private static ClientMessage.DeleteStreamCompleted UnwrapDeleteStreamCompleted(AmqpPackage package, IEnvelope envelope)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.DeleteStreamCompleted>();
            if (dto == null) return null;
            return new ClientMessage.DeleteStreamCompleted(package.CorrelationId, (OperationResult)dto.Result,
                                                           dto.Message,
                                                           dto.PreparePosition ?? -1,
                                                           dto.CommitPosition ?? -1);
        }

        private static AmqpPackage WrapDeleteStreamCompleted(ClientMessage.DeleteStreamCompleted msg)
        {
            var dto = new TcpClientMessageDto.DeleteStreamCompleted((TcpClientMessageDto.OperationResult)msg.Result,
                                                                    msg.Message,
                                                                    msg.PreparePosition,
                                                                    msg.CommitPosition);
            return new AmqpPackage(AmqpCommand.DeleteStreamCompleted, msg.CorrelationId, dto.Serialize());
        }

        private static ClientMessage.ReadEvent UnwrapReadEvent(AmqpPackage package, IEnvelope envelope, IPrincipal user)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.ReadEvent>();
            if (dto == null) return null;
            return new ClientMessage.ReadEvent(Guid.NewGuid(), package.CorrelationId, envelope, dto.EventStreamId,
                                               dto.EventNumber, dto.ResolveLinkTos, dto.RequireMaster, user);
        }

        private static AmqpPackage WrapReadEventCompleted(ClientMessage.ReadEventCompleted msg)
        {
            var dto = new TcpClientMessageDto.ReadEventCompleted(
                (TcpClientMessageDto.ReadEventCompleted.ReadEventResult)msg.Result,
                new TcpClientMessageDto.ResolvedIndexedEvent(msg.Record.Event, msg.Record.Link), msg.Error);
            return new AmqpPackage(AmqpCommand.ReadEventCompleted, msg.CorrelationId, dto.Serialize());
        }

        private static ClientMessage.ReadStreamEventsForward UnwrapReadStreamEventsForward(AmqpPackage package, IEnvelope envelope, IPrincipal user)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.ReadStreamEvents>();
            if (dto == null) return null;
            return new ClientMessage.ReadStreamEventsForward(Guid.NewGuid(), package.CorrelationId, envelope,
                                                             dto.EventStreamId, dto.FromEventNumber, dto.MaxCount,
                                                             dto.ResolveLinkTos, dto.RequireMaster, null, user);
        }

        private static AmqpPackage WrapReadStreamEventsForwardCompleted(ClientMessage.ReadStreamEventsForwardCompleted msg)
        {
            var dto = new TcpClientMessageDto.ReadStreamEventsCompleted(
                ConvertToResolvedIndexedEvents(msg.Events), (TcpClientMessageDto.ReadStreamEventsCompleted.ReadStreamResult)msg.Result,
                msg.NextEventNumber, msg.LastEventNumber, msg.IsEndOfStream, msg.TfLastCommitPosition, msg.Error);
            return new AmqpPackage(AmqpCommand.ReadStreamEventsForwardCompleted, msg.CorrelationId, dto.Serialize());
        }

        private static ClientMessage.ReadStreamEventsBackward UnwrapReadStreamEventsBackward(AmqpPackage package, IEnvelope envelope, IPrincipal user)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.ReadStreamEvents>();
            if (dto == null) return null;
            return new ClientMessage.ReadStreamEventsBackward(Guid.NewGuid(), package.CorrelationId, envelope,
                                                              dto.EventStreamId, dto.FromEventNumber, dto.MaxCount,
                                                              dto.ResolveLinkTos, dto.RequireMaster, null, user);
        }

        private static AmqpPackage WrapReadStreamEventsBackwardCompleted(ClientMessage.ReadStreamEventsBackwardCompleted msg)
        {
            var dto = new TcpClientMessageDto.ReadStreamEventsCompleted(
                ConvertToResolvedIndexedEvents(msg.Events), (TcpClientMessageDto.ReadStreamEventsCompleted.ReadStreamResult)msg.Result,
                msg.NextEventNumber, msg.LastEventNumber, msg.IsEndOfStream, msg.TfLastCommitPosition, msg.Error);
            return new AmqpPackage(AmqpCommand.ReadStreamEventsBackwardCompleted, msg.CorrelationId, dto.Serialize());
        }

        private static TcpClientMessageDto.ResolvedIndexedEvent[] ConvertToResolvedIndexedEvents(ResolvedEvent[] events)
        {
            var result = new TcpClientMessageDto.ResolvedIndexedEvent[events.Length];
            for (int i = 0; i < events.Length; ++i)
            {
                result[i] = new TcpClientMessageDto.ResolvedIndexedEvent(events[i].Event, events[i].Link);
            }
            return result;
        }

        private static ClientMessage.ReadAllEventsForward UnwrapReadAllEventsForward(AmqpPackage package, IEnvelope envelope, IPrincipal user)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.ReadAllEvents>();
            if (dto == null) return null;
            return new ClientMessage.ReadAllEventsForward(Guid.NewGuid(), package.CorrelationId, envelope,
                                                          dto.CommitPosition, dto.PreparePosition, dto.MaxCount,
                                                          dto.ResolveLinkTos, dto.RequireMaster, null, user);
        }

        private static AmqpPackage WrapReadAllEventsForwardCompleted(ClientMessage.ReadAllEventsForwardCompleted msg)
        {
            var dto = new TcpClientMessageDto.ReadAllEventsCompleted(
                msg.CurrentPos.CommitPosition, msg.CurrentPos.PreparePosition, ConvertToResolvedEvents(msg.Events),
                msg.NextPos.CommitPosition, msg.NextPos.PreparePosition,
                (TcpClientMessageDto.ReadAllEventsCompleted.ReadAllResult)msg.Result, msg.Error);
            return new AmqpPackage(AmqpCommand.ReadAllEventsForwardCompleted, msg.CorrelationId, dto.Serialize());
        }

        private static ClientMessage.ReadAllEventsBackward UnwrapReadAllEventsBackward(AmqpPackage package, IEnvelope envelope, IPrincipal user)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.ReadAllEvents>();
            if (dto == null) return null;
            return new ClientMessage.ReadAllEventsBackward(Guid.NewGuid(), package.CorrelationId, envelope,
                                                           dto.CommitPosition, dto.PreparePosition, dto.MaxCount,
                                                           dto.ResolveLinkTos, dto.RequireMaster, null, user);
        }

        private static AmqpPackage WrapReadAllEventsBackwardCompleted(ClientMessage.ReadAllEventsBackwardCompleted msg)
        {
            var dto = new TcpClientMessageDto.ReadAllEventsCompleted(
                msg.CurrentPos.CommitPosition, msg.CurrentPos.PreparePosition, ConvertToResolvedEvents(msg.Events),
                msg.NextPos.CommitPosition, msg.NextPos.PreparePosition,
                (TcpClientMessageDto.ReadAllEventsCompleted.ReadAllResult)msg.Result, msg.Error);
            return new AmqpPackage(AmqpCommand.ReadAllEventsBackwardCompleted, msg.CorrelationId, dto.Serialize());
        }

        private static TcpClientMessageDto.ResolvedEvent[] ConvertToResolvedEvents(ResolvedEvent[] events)
        {
            var result = new TcpClientMessageDto.ResolvedEvent[events.Length];
            for (int i = 0; i < events.Length; ++i)
            {
                result[i] = new TcpClientMessageDto.ResolvedEvent(events[i]);
            }
            return result;
        }

        private ClientMessage.SubscribeToStream UnwrapSubscribeToStream(AmqpPackage package,
                                                                        IEnvelope envelope,
                                                                        IPrincipal user,
                                                                        string login,
                                                                        string pass,
                                                                        AmqpConnectionManager connection)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.SubscribeToStream>();
            if (dto == null) return null;
            return new ClientMessage.SubscribeToStream(Guid.NewGuid(), package.CorrelationId, envelope,
                                                       connection.ConnectionId, dto.EventStreamId, dto.ResolveLinkTos, user);
        }

        private ClientMessage.UnsubscribeFromStream UnwrapUnsubscribeFromStream(AmqpPackage package, IEnvelope envelope, IPrincipal user)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.UnsubscribeFromStream>();
            if (dto == null) return null;
            return new ClientMessage.UnsubscribeFromStream(Guid.NewGuid(), package.CorrelationId, envelope, user);
        }

        private AmqpPackage WrapSubscribedToStream(ClientMessage.SubscriptionConfirmation msg)
        {
            var dto = new TcpClientMessageDto.SubscriptionConfirmation(msg.LastCommitPosition, msg.LastEventNumber);
            return new AmqpPackage(AmqpCommand.SubscriptionConfirmation, msg.CorrelationId, dto.Serialize());
        }

        private ClientMessage.CreatePersistentSubscription UnwrapCreatePersistentSubscription(
            AmqpPackage package, IEnvelope envelope, IPrincipal user, string username, string password,
            AmqpConnectionManager connection)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.CreatePersistentSubscription>();
            if (dto == null) return null;

            var namedConsumerStrategy = dto.NamedConsumerStrategy;
            if (string.IsNullOrEmpty(namedConsumerStrategy))
            {
                namedConsumerStrategy = dto.PreferRoundRobin
                  ? SystemConsumerStrategies.RoundRobin
                  : SystemConsumerStrategies.DispatchToSingle;
            }

            return new ClientMessage.CreatePersistentSubscription(Guid.NewGuid(), package.CorrelationId, envelope,
                            dto.EventStreamId, dto.SubscriptionGroupName, dto.ResolveLinkTos, dto.StartFrom, dto.MessageTimeoutMilliseconds,
                            dto.RecordStatistics, dto.MaxRetryCount, dto.BufferSize, dto.LiveBufferSize,
                            dto.ReadBatchSize, dto.CheckpointAfterTime, dto.CheckpointMinCount,
                            dto.CheckpointMaxCount, dto.SubscriberMaxCount, namedConsumerStrategy,
                            user, username, password);
        }

        private ClientMessage.UpdatePersistentSubscription UnwrapUpdatePersistentSubscription(
            AmqpPackage package, IEnvelope envelope, IPrincipal user, string username, string password,
            AmqpConnectionManager connection)
        {

            var dto = package.Data.Deserialize<TcpClientMessageDto.UpdatePersistentSubscription>();
            if (dto == null) return null;

            var namedConsumerStrategy = dto.NamedConsumerStrategy;
            if (string.IsNullOrEmpty(namedConsumerStrategy))
            {
                namedConsumerStrategy = dto.PreferRoundRobin
                  ? SystemConsumerStrategies.RoundRobin
                  : SystemConsumerStrategies.DispatchToSingle;
            }

            return new ClientMessage.UpdatePersistentSubscription(Guid.NewGuid(), package.CorrelationId, envelope,
                dto.EventStreamId, dto.SubscriptionGroupName, dto.ResolveLinkTos, dto.StartFrom,
                dto.MessageTimeoutMilliseconds,
                dto.RecordStatistics, dto.MaxRetryCount, dto.BufferSize, dto.LiveBufferSize,
                dto.ReadBatchSize, dto.CheckpointAfterTime, dto.CheckpointMinCount,
                dto.CheckpointMaxCount, dto.SubscriberMaxCount, namedConsumerStrategy,
                user, username, password);
        }

        private ClientMessage.DeletePersistentSubscription UnwrapDeletePersistentSubscription(
            AmqpPackage package, IEnvelope envelope, IPrincipal user, string username, string password,
            AmqpConnectionManager connection)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.CreatePersistentSubscription>();
            if (dto == null) return null;
            return new ClientMessage.DeletePersistentSubscription(Guid.NewGuid(), package.CorrelationId, envelope,
                            dto.EventStreamId, dto.SubscriptionGroupName, user);
        }

        private AmqpPackage WrapDeletePersistentSubscriptionCompleted(ClientMessage.DeletePersistentSubscriptionCompleted msg)
        {
            var dto = new TcpClientMessageDto.DeletePersistentSubscriptionCompleted((TcpClientMessageDto.DeletePersistentSubscriptionCompleted.DeletePersistentSubscriptionResult)msg.Result, msg.Reason);
            return new AmqpPackage(AmqpCommand.DeletePersistentSubscriptionCompleted, msg.CorrelationId, dto.Serialize());
        }

        private AmqpPackage WrapCreatePersistentSubscriptionCompleted(ClientMessage.CreatePersistentSubscriptionCompleted msg)
        {
            var dto = new TcpClientMessageDto.CreatePersistentSubscriptionCompleted((TcpClientMessageDto.CreatePersistentSubscriptionCompleted.CreatePersistentSubscriptionResult)msg.Result, msg.Reason);
            return new AmqpPackage(AmqpCommand.CreatePersistentSubscriptionCompleted, msg.CorrelationId, dto.Serialize());
        }

        private AmqpPackage WrapUpdatePersistentSubscriptionCompleted(ClientMessage.UpdatePersistentSubscriptionCompleted msg)
        {
            var dto = new TcpClientMessageDto.UpdatePersistentSubscriptionCompleted((TcpClientMessageDto.UpdatePersistentSubscriptionCompleted.UpdatePersistentSubscriptionResult)msg.Result, msg.Reason);
            return new AmqpPackage(AmqpCommand.UpdatePersistentSubscriptionCompleted, msg.CorrelationId, dto.Serialize());
        }


        private ClientMessage.ConnectToPersistentSubscription UnwrapConnectToPersistentSubscription(
            AmqpPackage package, IEnvelope envelope, IPrincipal user, string login, string pass,
            AmqpConnectionManager connection)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.ConnectToPersistentSubscription>();
            if (dto == null) return null;
            return new ClientMessage.ConnectToPersistentSubscription(Guid.NewGuid(), package.CorrelationId, envelope,
                connection.ConnectionId, dto.SubscriptionId, dto.EventStreamId, dto.AllowedInFlightMessages, connection.RemoteEndPoint.ToString(), user);
        }

        private ClientMessage.PersistentSubscriptionAckEvents UnwrapPersistentSubscriptionAckEvents(
            AmqpPackage package, IEnvelope envelope, IPrincipal user, string login, string pass,
            AmqpConnectionManager connection)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.PersistentSubscriptionAckEvents>();
            if (dto == null) return null;
            return new ClientMessage.PersistentSubscriptionAckEvents(
                Guid.NewGuid(), package.CorrelationId, envelope, dto.SubscriptionId,
                dto.ProcessedEventIds.Select(x => new Guid(x)).ToArray(), user);
        }

        private ClientMessage.PersistentSubscriptionNackEvents UnwrapPersistentSubscriptionNackEvents(
            AmqpPackage package, IEnvelope envelope, IPrincipal user, string login, string pass,
            AmqpConnectionManager connection)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.PersistentSubscriptionNakEvents>();
            if (dto == null) return null;
            return new ClientMessage.PersistentSubscriptionNackEvents(
                Guid.NewGuid(), package.CorrelationId, envelope, dto.SubscriptionId,
                dto.Message, (ClientMessage.PersistentSubscriptionNackEvents.NakAction)dto.Action,
                dto.ProcessedEventIds.Select(x => new Guid(x)).ToArray(), user);
        }

        private AmqpPackage WrapPersistentSubscriptionConfirmation(ClientMessage.PersistentSubscriptionConfirmation msg)
        {
            var dto = new TcpClientMessageDto.PersistentSubscriptionConfirmation(msg.LastCommitPosition, msg.SubscriptionId, msg.LastEventNumber);
            return new AmqpPackage(AmqpCommand.PersistentSubscriptionConfirmation, msg.CorrelationId, dto.Serialize());
        }

        private AmqpPackage WrapPersistentSubscriptionStreamEventAppeared(ClientMessage.PersistentSubscriptionStreamEventAppeared msg)
        {
            var dto = new TcpClientMessageDto.PersistentSubscriptionStreamEventAppeared(new TcpClientMessageDto.ResolvedIndexedEvent(msg.Event.Event, msg.Event.Link));
            return new AmqpPackage(AmqpCommand.PersistentSubscriptionStreamEventAppeared, msg.CorrelationId, dto.Serialize());
        }

        private AmqpPackage WrapStreamEventAppeared(ClientMessage.StreamEventAppeared msg)
        {
            var dto = new TcpClientMessageDto.StreamEventAppeared(new TcpClientMessageDto.ResolvedEvent(msg.Event));
            return new AmqpPackage(AmqpCommand.StreamEventAppeared, msg.CorrelationId, dto.Serialize());
        }

        private AmqpPackage WrapSubscriptionDropped(ClientMessage.SubscriptionDropped msg)
        {
            var dto = new TcpClientMessageDto.SubscriptionDropped((TcpClientMessageDto.SubscriptionDropped.SubscriptionDropReason)msg.Reason);
            return new AmqpPackage(AmqpCommand.SubscriptionDropped, msg.CorrelationId, dto.Serialize());
        }

        private ClientMessage.ScavengeDatabase UnwrapScavengeDatabase(AmqpPackage package, IEnvelope envelope, IPrincipal user)
        {
            return new ClientMessage.ScavengeDatabase(envelope, package.CorrelationId, user);
        }

        private AmqpPackage WrapScavengeDatabaseResponse(ClientMessage.ScavengeDatabaseCompleted msg)
        {
            var dto = new TcpClientMessageDto.ScavengeDatabaseCompleted(
                (TcpClientMessageDto.ScavengeDatabaseCompleted.ScavengeResult)msg.Result, msg.Error,
                (int)msg.TotalTime.TotalMilliseconds, msg.TotalSpaceSaved);
            return new AmqpPackage(AmqpCommand.ScavengeDatabaseCompleted, msg.CorrelationId, dto.Serialize());
        }

        private ClientMessage.NotHandled UnwrapNotHandled(AmqpPackage package, IEnvelope envelope)
        {
            var dto = package.Data.Deserialize<TcpClientMessageDto.NotHandled>();
            if (dto == null) return null;
            return new ClientMessage.NotHandled(package.CorrelationId, dto.Reason, dto.AdditionalInfo);
        }

        private AmqpPackage WrapNotHandled(ClientMessage.NotHandled msg)
        {
            var dto = new TcpClientMessageDto.NotHandled(msg.Reason, msg.AdditionalInfo == null ? null : msg.AdditionalInfo.SerializeToArray());
            return new AmqpPackage(AmqpCommand.NotHandled, msg.CorrelationId, dto.Serialize());
        }

        private AmqpPackage WrapNotAuthenticated(AmqpMessage.NotAuthenticated msg)
        {
            return new AmqpPackage(AmqpCommand.NotAuthenticated, msg.CorrelationId, Helper.UTF8NoBom.GetBytes(msg.Reason ?? string.Empty));
        }

        private AmqpPackage WrapAuthenticated(AmqpMessage.Authenticated msg)
        {
            return new AmqpPackage(AmqpCommand.Authenticated, msg.CorrelationId, Empty.ByteArray);
        }
    }
}