using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.VNode;
using EventStore.Core.TransactionLog.Chunks;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace EventStore.Core.Tests.AllReaderTests
{
    internal class SimulatedFSM : IHandle<Message>
    {
        private readonly IBus _outputBus;
        private QueuedHandler _queue;
        private VNodeState _state = VNodeState.Initializing;
        private int _numberOfServicesExpectedToStart = 3;
        private int _serviceShutdownsToExpect = 3;
        private Guid _stateCorrelationId = Guid.NewGuid();
        private Guid _subscriptionId = Guid.NewGuid();
        private VNodeFSM _fsm;
        private VNodeInfo _master;
        private TFChunkDb _db;
        private MultiQueuedHandler _workersHandler;

        public SimulatedFSM(IBus outputBus, TFChunkDb db, MultiQueuedHandler workersHandler)
        {
            _outputBus = outputBus;
            _fsm = BuildFSM();
            _db = db;
            _workersHandler = workersHandler;
        }
        public void SetQueue(QueuedHandler queue)
        {
            _queue = queue;
        }
        void IHandle<Message>.Handle(Message message)
        {
            _fsm.Handle(message);
        }

        public void Handle(SystemMessage.SystemInit message)
        {
            _outputBus.Publish(message);
        }

        public void Handle(SystemMessage.ServiceInitialized message)
        {
            _numberOfServicesExpectedToStart--;
            _outputBus.Publish(message);
            if (_numberOfServicesExpectedToStart == 0)
            {
                _queue.Handle(new SystemMessage.SystemStart());
            }
        }
        private void Handle(SystemMessage.SystemStart message)
        {
            _outputBus.Publish(message);
            _fsm.Handle(new SystemMessage.BecomeUnknown(_stateCorrelationId));
        }

        private void Handle(SystemMessage.BecomeUnknown message)
        {
            _state = VNodeState.Unknown;
            _master = null;
            _outputBus.Publish(message);
            _queue.Publish(new SystemMessage.BecomePreMaster(_stateCorrelationId));
        }

        private void Handle(SystemMessage.BecomePreReplica message)
        {
            if (_master == null) throw new Exception("_master == null");
            if (_stateCorrelationId != message.CorrelationId)
                return;

            _state = VNodeState.PreReplica;
            _outputBus.Publish(message);
            _queue.Publish(new SystemMessage.WaitForChaserToCatchUp(_stateCorrelationId, TimeSpan.Zero));
        }

        private void Handle(SystemMessage.BecomeCatchingUp message)
        {
            if (_master == null) throw new Exception("_master == null");
            if (_stateCorrelationId != message.CorrelationId)
                return;

            _state = VNodeState.CatchingUp;
            _outputBus.Publish(message);
        }

        private void Handle(SystemMessage.BecomeClone message)
        {
            if (_master == null) throw new Exception("_master == null");
            if (_stateCorrelationId != message.CorrelationId)
                return;

            _state = VNodeState.Clone;
            _outputBus.Publish(message);
        }

        private void Handle(SystemMessage.BecomeSlave message)
        {
            if (_master == null) throw new Exception("_master == null");
            if (_stateCorrelationId != message.CorrelationId)
                return;

            _state = VNodeState.Slave;
            _outputBus.Publish(message);
        }

        private void Handle(SystemMessage.BecomePreMaster message)
        {
            if (_stateCorrelationId != message.CorrelationId)
                return;

            _state = VNodeState.PreMaster;
            _outputBus.Publish(message);
            _queue.Publish(new SystemMessage.WaitForChaserToCatchUp(_stateCorrelationId, TimeSpan.Zero));
        }

        private void Handle(SystemMessage.BecomeMaster message)
        {
            if (_state == VNodeState.Master) throw new Exception("We should not BecomeMaster twice in a row.");
            if (_stateCorrelationId != message.CorrelationId)
                return;

            _state = VNodeState.Master;
            _outputBus.Publish(message);
        }

        private void HandleAsNonMaster(ClientMessage.ReadEvent message)
        {
            if (message.RequireMaster)
            {
                //if (_master == null)
                //    DenyRequestBecauseNotReady(message.Envelope, message.CorrelationId);
                //else
                //    DenyRequestBecauseNotMaster(message.CorrelationId, message.Envelope);
            }
            else
            {
                _outputBus.Publish(message);
            }
        }

        private void HandleAsNonMaster(ClientMessage.ReadStreamEventsForward message)
        {
            if (message.RequireMaster)
            {
                //if (_master == null)
                //    DenyRequestBecauseNotReady(message.Envelope, message.CorrelationId);
                //else
                //    DenyRequestBecauseNotMaster(message.CorrelationId, message.Envelope);
            }
            else
            {
                _outputBus.Publish(message);
            }
        }

        private void HandleAsNonMaster(ClientMessage.ReadStreamEventsBackward message)
        {
            if (message.RequireMaster)
            {
                //if (_master == null)
                //    DenyRequestBecauseNotReady(message.Envelope, message.CorrelationId);
                //else
                //    DenyRequestBecauseNotMaster(message.CorrelationId, message.Envelope);
            }
            else
            {
                _outputBus.Publish(message);
            }
        }

        private void HandleAsNonMaster(ClientMessage.ReadAllEventsForward message)
        {
            if (message.RequireMaster)
            {
                //if (_master == null)
                //    DenyRequestBecauseNotReady(message.Envelope, message.CorrelationId);
                //else
                //    DenyRequestBecauseNotMaster(message.CorrelationId, message.Envelope);
            }
            else
            {
                _outputBus.Publish(message);
            }
        }

        private void HandleAsNonMaster(ClientMessage.ReadAllEventsBackward message)
        {
            if (message.RequireMaster)
            {
                //if (_master == null)
                //    DenyRequestBecauseNotReady(message.Envelope, message.CorrelationId);
                //else
                //    //DenyRequestBecauseNotMaster(message.CorrelationId, message.Envelope);
            }
            else
            {
                _outputBus.Publish(message);
            }
        }

        private void HandleAsNonMaster(ClientMessage.WriteEvents message)
        {
            if (message.RequireMaster)
            {
                //DenyRequestBecauseNotMaster(message.CorrelationId, message.Envelope);
                return;
            }
            var timeoutMessage = new ClientMessage.WriteEventsCompleted(
                    message.CorrelationId, OperationResult.ForwardTimeout, "Forwarding timeout");
            ForwardRequest(message, timeoutMessage);
        }

        private void HandleAsNonMaster(ClientMessage.TransactionStart message)
        {
            if (message.RequireMaster)
            {
                //DenyRequestBecauseNotMaster(message.CorrelationId, message.Envelope);
                return;
            }
            var timeoutMessage = new ClientMessage.TransactionStartCompleted(
                message.CorrelationId, -1, OperationResult.ForwardTimeout, "Forwarding timeout");
            ForwardRequest(message, timeoutMessage);
        }

        private void HandleAsNonMaster(ClientMessage.TransactionWrite message)
        {
            if (message.RequireMaster)
            {
                //DenyRequestBecauseNotMaster(message.CorrelationId, message.Envelope);
                return;
            }
            var timeoutMessage = new ClientMessage.TransactionWriteCompleted(
                message.CorrelationId, message.TransactionId, OperationResult.ForwardTimeout, "Forwarding timeout");
            ForwardRequest(message, timeoutMessage);
        }

        private void HandleAsNonMaster(ClientMessage.TransactionCommit message)
        {
            if (message.RequireMaster)
            {
                //DenyRequestBecauseNotMaster(message.CorrelationId, message.Envelope);
                return;
            }
            var timeoutMessage = new ClientMessage.TransactionCommitCompleted(
                message.CorrelationId, message.TransactionId, OperationResult.ForwardTimeout, "Forwarding timeout");
            ForwardRequest(message, timeoutMessage);
        }

        private void HandleAsNonMaster(ClientMessage.DeleteStream message)
        {
            if (message.RequireMaster)
            {
                //DenyRequestBecauseNotMaster(message.CorrelationId, message.Envelope);
                return;
            }
            var timeoutMessage = new ClientMessage.DeleteStreamCompleted(
                message.CorrelationId, OperationResult.ForwardTimeout, "Forwarding timeout", -1, -1);
            ForwardRequest(message, timeoutMessage);
        }

        private void ForwardRequest(ClientMessage.WriteRequestMessage msg, Message timeoutMessage)
        {
            _outputBus.Publish(new ClientMessage.TcpForwardMessage(msg));
        }

        private void DenyRequestBecauseNotReady(IEnvelope envelope, Guid correlationId)
        {
            envelope.ReplyWith(new ClientMessage.NotHandled(correlationId, TcpClientMessageDto.NotHandled.NotHandledReason.NotReady, null));
        }

        private void HandleAsMaster(GossipMessage.GossipUpdated message)
        {
            if (_master == null) throw new Exception("_master == null");
            if (message.ClusterInfo.Members.Count(x => x.IsAlive && x.State == VNodeState.Master) > 1)
            {
                _queue.Publish(new ElectionMessage.StartElections());
            }

            _outputBus.Publish(message);
        }

        private void HandleAsNonMaster(GossipMessage.GossipUpdated message)
        {
            if (_master == null) throw new Exception("_master == null");
            var master = message.ClusterInfo.Members.FirstOrDefault(x => x.InstanceId == _master.InstanceId);
            if (master == null || !master.IsAlive)
            {
                _queue.Publish(new ElectionMessage.StartElections());
            }

            _outputBus.Publish(message);
        }

        private void Handle(SystemMessage.NoQuorumMessage message)
        {
            _fsm.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));
        }

        private void Handle(SystemMessage.WaitForChaserToCatchUp message)
        {
            if (message.CorrelationId != _stateCorrelationId)
                return;
            _outputBus.Publish(message);
        }

        private void HandleAsPreMaster(SystemMessage.ChaserCaughtUp message)
        {
            if (_stateCorrelationId != message.CorrelationId)
                return;

            _outputBus.Publish(message);
            _fsm.Handle(new SystemMessage.BecomeMaster(_stateCorrelationId));
        }

        private void HandleAsPreReplica(SystemMessage.ChaserCaughtUp message)
        {
            if (_master == null) throw new Exception("_master == null");
            if (_stateCorrelationId != message.CorrelationId)
                return;
            _outputBus.Publish(message);
            _fsm.Handle(new ReplicationMessage.SubscribeToMaster(_stateCorrelationId, _master.InstanceId, Guid.NewGuid()));
        }

        private void Handle(ReplicationMessage.ReconnectToMaster message)
        {
            if (_master.InstanceId != message.Master.InstanceId || _stateCorrelationId != message.StateCorrelationId)
                return;
            _outputBus.Publish(message);
        }

        private void Handle(ReplicationMessage.ReplicaSubscribed message)
        {
            if (IsLegitimateReplicationMessage(message))
            {
                _outputBus.Publish(message);
                _fsm.Handle(new SystemMessage.BecomeCatchingUp(_stateCorrelationId, _master));
            }
        }

        private void ForwardReplicationMessage<T>(T message) where T : Message, ReplicationMessage.IReplicationMessage
        {
            if (IsLegitimateReplicationMessage(message))
                _outputBus.Publish(message);
        }

        private void Handle(ReplicationMessage.SlaveAssignment message)
        {
            if (IsLegitimateReplicationMessage(message))
            {
                _outputBus.Publish(message);
                _fsm.Handle(new SystemMessage.BecomeSlave(_stateCorrelationId, _master));
            }
        }

        private void Handle(ReplicationMessage.CloneAssignment message)
        {
            if (IsLegitimateReplicationMessage(message))
            {
                _outputBus.Publish(message);
                _fsm.Handle(new SystemMessage.BecomeClone(_stateCorrelationId, _master));
            }
        }

        private bool IsLegitimateReplicationMessage(ReplicationMessage.IReplicationMessage message)
        {
            if (message.SubscriptionId == Guid.Empty) throw new Exception("IReplicationMessage with empty SubscriptionId provided.");
            if (message.SubscriptionId != _subscriptionId)
            {
                return false;
            }
            if (_master == null || _master.InstanceId != message.MasterId)
            {
                return false;
            }
            return true;
        }

        private void Handle(ClientMessage.RequestShutdown message)
        {
            _outputBus.Publish(message);
            _fsm.Handle(new SystemMessage.BecomeShuttingDown(Guid.NewGuid(), message.ExitProcess));
        }

        private void Handle(SystemMessage.BecomeShuttingDown message)
        {
            if (_state == VNodeState.ShuttingDown || _state == VNodeState.Shutdown)
                return;

            _master = null;
            _stateCorrelationId = message.CorrelationId;
            _state = VNodeState.ShuttingDown;
            _outputBus.Publish(message);
        }

        private void Handle(SystemMessage.BecomeShutdown message)
        {
            _state = VNodeState.Shutdown;
            try
            {
                _outputBus.Publish(message);
            }
            catch (Exception)
            {
                //ignore
            }
            try
            {
                _workersHandler.Stop();
                _queue.RequestStop();
            }
            catch (Exception)
            {
                //ignore
            }
        }

        private void Handle(SystemMessage.ServiceShutdown message)
        {
            _serviceShutdownsToExpect -= 1;
            if (_serviceShutdownsToExpect == 0)
            {
                Shutdown();
            }
            _outputBus.Publish(message);
        }

        private void Shutdown()
        {
            Debug.Assert(_state == VNodeState.ShuttingDown);

            _db.Close();
            _fsm.Handle(new SystemMessage.BecomeShutdown(_stateCorrelationId));
        }

        private void Handle(SystemMessage.ShutdownTimeout message)
        {
            _outputBus.Publish(message);
        }
        public VNodeFSM BuildFSM()
        {
            var stm = new VNodeFSMBuilder(() => _state)
            .InAnyState()
                .When<SystemMessage.StateChangeMessage>()
                    .Do(m => Application.Exit(ExitCode.Error, string.Format("{0} message was unhandled in {1}.", m.GetType().Name, GetType().Name)))

            .InState(VNodeState.Initializing)
                .When<SystemMessage.SystemInit>().Do(Handle)
                .When<SystemMessage.SystemStart>().Do(Handle)
                .When<SystemMessage.ServiceInitialized>().Do(Handle)
                //.When<ClientMessage.ScavengeDatabase>().Ignore()
                .WhenOther().ForwardTo(_outputBus)

            .InState(VNodeState.Unknown)
                .WhenOther().ForwardTo(_outputBus)

            .InStates(VNodeState.Initializing, VNodeState.Master, VNodeState.PreMaster,
                      VNodeState.PreReplica, VNodeState.CatchingUp, VNodeState.Clone, VNodeState.Slave)
                .When<SystemMessage.BecomeUnknown>().Do(Handle)

            .InAllStatesExcept(VNodeState.Unknown,
                               VNodeState.PreReplica, VNodeState.CatchingUp, VNodeState.Clone, VNodeState.Slave,
                               VNodeState.Master)
                //.When<ClientMessage.ReadRequestMessage>().Do(msg => DenyRequestBecauseNotReady(msg.Envelope, msg.CorrelationId))

            .InAllStatesExcept(VNodeState.Master,
                               VNodeState.PreReplica, VNodeState.CatchingUp, VNodeState.Clone, VNodeState.Slave)
                //.When<ClientMessage.WriteRequestMessage>().Do(msg => DenyRequestBecauseNotReady(msg.Envelope, msg.CorrelationId))

            .InState(VNodeState.Master)
                .When<ClientMessage.ReadEvent>().ForwardTo(_outputBus)
                .When<ClientMessage.ReadStreamEventsForward>().ForwardTo(_outputBus)
                .When<ClientMessage.ReadStreamEventsBackward>().ForwardTo(_outputBus)
                .When<ClientMessage.ReadAllEventsForward>().ForwardTo(_outputBus)
                .When<ClientMessage.ReadAllEventsBackward>().ForwardTo(_outputBus)
                .When<ClientMessage.WriteEvents>().ForwardTo(_outputBus)
                .When<ClientMessage.TransactionStart>().ForwardTo(_outputBus)
                .When<ClientMessage.TransactionWrite>().ForwardTo(_outputBus)
                .When<ClientMessage.TransactionCommit>().ForwardTo(_outputBus)
                .When<ClientMessage.DeleteStream>().ForwardTo(_outputBus)

            .InStates(VNodeState.PreReplica, VNodeState.CatchingUp, VNodeState.Clone, VNodeState.Slave,
                      VNodeState.Unknown)
                //.When<ClientMessage.ReadEvent>().Do(HandleAsNonMaster)
                //.When<ClientMessage.ReadStreamEventsForward>().Do(HandleAsNonMaster)
                //.When<ClientMessage.ReadStreamEventsBackward>().Do(HandleAsNonMaster)
                //.When<ClientMessage.ReadAllEventsForward>().Do(HandleAsNonMaster)
                //.When<ClientMessage.ReadAllEventsBackward>().Do(HandleAsNonMaster)

            .InStates(VNodeState.PreReplica, VNodeState.CatchingUp, VNodeState.Clone, VNodeState.Slave)
                //.When<ClientMessage.WriteEvents>().Do(HandleAsNonMaster)
                //.When<ClientMessage.TransactionStart>().Do(HandleAsNonMaster)
                //.When<ClientMessage.TransactionWrite>().Do(HandleAsNonMaster)
                //.When<ClientMessage.TransactionCommit>().Do(HandleAsNonMaster)
                //.When<ClientMessage.DeleteStream>().Do(HandleAsNonMaster)

            .InAnyState()
                .When<ClientMessage.NotHandled>().ForwardTo(_outputBus)
                .When<ClientMessage.ReadEventCompleted>().ForwardTo(_outputBus)
                .When<ClientMessage.ReadStreamEventsForwardCompleted>().ForwardTo(_outputBus)
                .When<ClientMessage.ReadStreamEventsBackwardCompleted>().ForwardTo(_outputBus)
                .When<ClientMessage.ReadAllEventsForwardCompleted>().ForwardTo(_outputBus)
                .When<ClientMessage.ReadAllEventsBackwardCompleted>().ForwardTo(_outputBus)
                .When<ClientMessage.WriteEventsCompleted>().ForwardTo(_outputBus)
                .When<ClientMessage.TransactionStartCompleted>().ForwardTo(_outputBus)
                .When<ClientMessage.TransactionWriteCompleted>().ForwardTo(_outputBus)
                .When<ClientMessage.TransactionCommitCompleted>().ForwardTo(_outputBus)
                .When<ClientMessage.DeleteStreamCompleted>().ForwardTo(_outputBus)

            .InAllStatesExcept(VNodeState.Initializing, VNodeState.ShuttingDown, VNodeState.Shutdown)
                //.When<ElectionMessage.ElectionsDone>().Do(Handle)

            .InStates(VNodeState.Unknown,
                      VNodeState.PreReplica, VNodeState.CatchingUp, VNodeState.Clone, VNodeState.Slave,
                      VNodeState.PreMaster, VNodeState.Master)
                .When<SystemMessage.BecomePreReplica>().Do(Handle)
                .When<SystemMessage.BecomePreMaster>().Do(Handle)

            .InStates(VNodeState.PreReplica, VNodeState.CatchingUp, VNodeState.Clone, VNodeState.Slave)
                //.When<GossipMessage.GossipUpdated>().Do(HandleAsNonMaster)
                //.When<SystemMessage.VNodeConnectionLost>().Do(Handle)

            .InAllStatesExcept(VNodeState.PreReplica, VNodeState.PreMaster)
                .When<SystemMessage.WaitForChaserToCatchUp>().Ignore()
                .When<SystemMessage.ChaserCaughtUp>().Ignore()

            .InState(VNodeState.PreReplica)
                //.When<SystemMessage.BecomeCatchingUp>().Do(Handle)
                //.When<SystemMessage.WaitForChaserToCatchUp>().Do(Handle)
                //.When<SystemMessage.ChaserCaughtUp>().Do(HandleAsPreReplica)
                //.When<ReplicationMessage.ReconnectToMaster>().Do(Handle)
                //.When<ReplicationMessage.SubscribeToMaster>().Do(Handle)
                //.When<ReplicationMessage.ReplicaSubscriptionRetry>().Do(Handle)
                //.When<ReplicationMessage.ReplicaSubscribed>().Do(Handle)
                .WhenOther().ForwardTo(_outputBus)
            .InAllStatesExcept(VNodeState.PreReplica)
                .When<ReplicationMessage.ReconnectToMaster>().Ignore()
                .When<ReplicationMessage.SubscribeToMaster>().Ignore()
                .When<ReplicationMessage.ReplicaSubscriptionRetry>().Ignore()
                .When<ReplicationMessage.ReplicaSubscribed>().Ignore()

            .InStates(VNodeState.CatchingUp, VNodeState.Clone, VNodeState.Slave)
                //.When<ReplicationMessage.CreateChunk>().Do(ForwardReplicationMessage)
                //.When<ReplicationMessage.RawChunkBulk>().Do(ForwardReplicationMessage)
                //.When<ReplicationMessage.DataChunkBulk>().Do(ForwardReplicationMessage)
                .When<ReplicationMessage.AckLogPosition>().ForwardTo(_outputBus)
                .WhenOther().ForwardTo(_outputBus)
            .InAllStatesExcept(VNodeState.CatchingUp, VNodeState.Clone, VNodeState.Slave)
                .When<ReplicationMessage.CreateChunk>().Ignore()
                .When<ReplicationMessage.RawChunkBulk>().Ignore()
                .When<ReplicationMessage.DataChunkBulk>().Ignore()
                .When<ReplicationMessage.AckLogPosition>().Ignore()

            .InState(VNodeState.CatchingUp)
                //.When<ReplicationMessage.CloneAssignment>().Do(Handle)
                //.When<ReplicationMessage.SlaveAssignment>().Do(Handle)
                //.When<SystemMessage.BecomeClone>().Do(Handle)
                //.When<SystemMessage.BecomeSlave>().Do(Handle)
            .InState(VNodeState.Clone)
                //.When<ReplicationMessage.SlaveAssignment>().Do(Handle)
                //.When<SystemMessage.BecomeSlave>().Do(Handle)
            .InState(VNodeState.Slave)
                //.When<ReplicationMessage.CloneAssignment>().Do(Handle)
                //.When<SystemMessage.BecomeClone>().Do(Handle)

            .InStates(VNodeState.PreMaster, VNodeState.Master)
                //.When<GossipMessage.GossipUpdated>().Do(HandleAsMaster)
                .When<ReplicationMessage.ReplicaSubscriptionRequest>().ForwardTo(_outputBus)
                .When<ReplicationMessage.ReplicaLogPositionAck>().ForwardTo(_outputBus)
            .InAllStatesExcept(VNodeState.PreMaster, VNodeState.Master)
                .When<ReplicationMessage.ReplicaSubscriptionRequest>().Ignore()

            .InState(VNodeState.PreMaster)
                .When<SystemMessage.BecomeMaster>().Do(Handle)
                .When<SystemMessage.WaitForChaserToCatchUp>().Do(Handle)
                .When<SystemMessage.ChaserCaughtUp>().Do(HandleAsPreMaster)
                .WhenOther().ForwardTo(_outputBus)

            .InState(VNodeState.Master)
                //.When<SystemMessage.NoQuorumMessage>().Do(Handle)
                .When<StorageMessage.WritePrepares>().ForwardTo(_outputBus)
                .When<StorageMessage.WriteDelete>().ForwardTo(_outputBus)
                .When<StorageMessage.WriteTransactionStart>().ForwardTo(_outputBus)
                .When<StorageMessage.WriteTransactionData>().ForwardTo(_outputBus)
                .When<StorageMessage.WriteTransactionPrepare>().ForwardTo(_outputBus)
                .When<StorageMessage.WriteCommit>().ForwardTo(_outputBus)
                .WhenOther().ForwardTo(_outputBus)

            .InAllStatesExcept(VNodeState.Master)
                .When<SystemMessage.NoQuorumMessage>().Ignore()
                .When<StorageMessage.WritePrepares>().Ignore()
                .When<StorageMessage.WriteDelete>().Ignore()
                .When<StorageMessage.WriteTransactionStart>().Ignore()
                .When<StorageMessage.WriteTransactionData>().Ignore()
                .When<StorageMessage.WriteTransactionPrepare>().Ignore()
                .When<StorageMessage.WriteCommit>().Ignore()

            .InAllStatesExcept(VNodeState.ShuttingDown, VNodeState.Shutdown)
                .When<ClientMessage.RequestShutdown>().Do(Handle)
                .When<SystemMessage.BecomeShuttingDown>().Do(Handle)

            .InState(VNodeState.ShuttingDown)
                .When<SystemMessage.BecomeShutdown>().Do(Handle)
                .When<SystemMessage.ShutdownTimeout>().Do(Handle)

            .InStates(VNodeState.ShuttingDown, VNodeState.Shutdown)
                .When<SystemMessage.ServiceShutdown>().Do(Handle)
                .WhenOther().ForwardTo(_outputBus)

            .Build();
            return stm;
        }
    }
}
