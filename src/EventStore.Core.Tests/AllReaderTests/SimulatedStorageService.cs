using System;
using System.IO;
using System.Linq;
using EventStore.Common.Utils;
using EventStore.Core.Authentication;
using EventStore.Core.Bus;
using EventStore.Core.DataStructures;
using EventStore.Core.Index;
using EventStore.Core.Index.Hashes;
using EventStore.Core.Messages;
using EventStore.Core.Services;
using EventStore.Core.Services.RequestManager;
using EventStore.Core.Services.Storage;
using EventStore.Core.Services.Storage.EpochManager;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Services.VNode;
using EventStore.Core.Settings;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using EventStore.Core.Messaging;
using System.Threading;

namespace EventStore.Core.Tests.AllReaderTests
{
    internal class SimulatedStorageService
    {
        public MockBus Bus;
        public QueuedHandler Queue;

        public SimulatedStorageService()
        {
            Bus = new MockBus();

            var forwardingProxy = new MessageForwardingProxy();

            var db = new TFChunkDb(GetMemDbConfig(Path.GetTempPath() + "tempDb", true));
            db.Open(verifyHash: false);

            var workerBusses = Enumerable.Range(0, 1).Select(queueNum =>
                new InMemoryBus(string.Format("Worker #{0} Bus", queueNum + 1),
                                watchSlowMsg: true,
                                slowMsgThreshold: TimeSpan.FromMilliseconds(200))).ToArray();
            var workersHandler = new MultiQueuedHandler(
                    queueCount: 1, 
                    queueFactory: queueNum => new QueuedHandlerThreadPool(workerBusses[queueNum],
                                                            string.Format("Worker #{0}", queueNum + 1),
                                                            groupName: "Workers",
                                                            watchSlowMsg: true,
                                                            slowMsgThreshold: TimeSpan.FromMilliseconds(200)));
            var simulatedFSM = new SimulatedFSM(Bus, db, workersHandler);
            Queue = new QueuedHandler(simulatedFSM, "QueuedHandler");
            simulatedFSM.SetQueue(Queue);

            // REQUEST FORWARDING
            var forwardingService = new RequestForwardingService(Queue, forwardingProxy, TimeSpan.FromSeconds(1));
            Bus.Subscribe<SystemMessage.SystemStart>(forwardingService);
            Bus.Subscribe<SystemMessage.RequestForwardingTimerTick>(forwardingService);
            Bus.Subscribe<ClientMessage.NotHandled>(forwardingService);
            Bus.Subscribe<ClientMessage.WriteEventsCompleted>(forwardingService);
            Bus.Subscribe<ClientMessage.TransactionStartCompleted>(forwardingService);
            Bus.Subscribe<ClientMessage.TransactionWriteCompleted>(forwardingService);
            Bus.Subscribe<ClientMessage.TransactionCommitCompleted>(forwardingService);
            Bus.Subscribe<ClientMessage.DeleteStreamCompleted>(forwardingService);


            var indexPath = Path.Combine(db.Config.Path, "index");
            var readerPool = new ObjectPool<ITransactionFileReader>(
                "ReadIndex readers pool", ESConsts.PTableInitialReaderCount, ESConsts.PTableMaxReaderCount,
                () => new TFChunkReader(db, db.Config.WriterCheckpoint));
            var tableIndex = new TableIndex(indexPath,
                                            () => new HashListMemTable(maxSize: 10),
                                            () => new TFReaderLease(readerPool),
                                            maxSizeForMemory: 100,
                                            maxTablesPerLevel: 2,
                                            inMem: db.Config.InMemDb);
            var hash = new XXHashUnsafe();
            var readIndex = new ReadIndex(Queue,
                                          readerPool,
                                          tableIndex,
                                          hash,
                                          ESConsts.StreamInfoCacheCapacity,
                                          Application.IsDefined(Application.AdditionalCommitChecks),
                                          Application.IsDefined(Application.InfiniteMetastreams) ? int.MaxValue : 1);
            var writer = new TFChunkWriter(db);
            var epochManager = new EpochManager(ESConsts.CachedEpochCount,
                                                db.Config.EpochCheckpoint,
                                                writer,
                                                initialReaderCount: 1,
                                                maxReaderCount: 5,
                                                readerFactory: () => new TFChunkReader(db, db.Config.WriterCheckpoint));
            epochManager.Init();

            new ClusterStorageWriterService(Queue, Bus, TFConsts.MinFlushDelayMs,
                                                                db, writer, readIndex.IndexWriter, epochManager,
                                                                () => readIndex.LastCommitPosition);

            var storageReader = new StorageReaderService(Queue, Bus, readIndex, ESConsts.StorageReaderThreadCount, db.Config.WriterCheckpoint);

            Bus.Subscribe<SystemMessage.SystemInit>(storageReader);
            Bus.Subscribe<SystemMessage.BecomeShuttingDown>(storageReader);
            Bus.Subscribe<SystemMessage.BecomeShutdown>(storageReader);

            var chaser = new TFChunkChaser(db, db.Config.WriterCheckpoint, db.Config.ChaserCheckpoint);
            var storageChaser = new StorageChaser(Queue, db.Config.WriterCheckpoint, chaser, readIndex.IndexCommitter, epochManager);

            Bus.Subscribe<SystemMessage.SystemInit>(storageChaser);
            Bus.Subscribe<SystemMessage.SystemStart>(storageChaser);
            Bus.Subscribe<SystemMessage.BecomeShuttingDown>(storageChaser);

            var storageScavenger = new StorageScavenger(db, tableIndex, hash, readIndex,
                                                        Application.IsDefined(Application.AlwaysKeepScavenged),
                                                        mergeChunks: true);

            Bus.Subscribe<ClientMessage.ScavengeDatabase>(storageScavenger);

            // REQUEST MANAGEMENT
            var requestManagement = new RequestManagementService(Queue,
                                                                 prepareCount: 1,
                                                                 commitCount: 1,
                                                                 prepareTimeout: TimeSpan.FromSeconds(5),
                                                                 commitTimeout: TimeSpan.FromSeconds(5));
            Bus.Subscribe<SystemMessage.SystemInit>(requestManagement);
            Bus.Subscribe<ClientMessage.WriteEvents>(requestManagement);
            Bus.Subscribe<ClientMessage.TransactionStart>(requestManagement);
            Bus.Subscribe<ClientMessage.TransactionWrite>(requestManagement);
            Bus.Subscribe<ClientMessage.TransactionCommit>(requestManagement);
            Bus.Subscribe<ClientMessage.DeleteStream>(requestManagement);
            Bus.Subscribe<StorageMessage.RequestCompleted>(requestManagement);
            Bus.Subscribe<StorageMessage.CheckStreamAccessCompleted>(requestManagement);
            Bus.Subscribe<StorageMessage.AlreadyCommitted>(requestManagement);
            Bus.Subscribe<StorageMessage.CommitAck>(requestManagement);
            Bus.Subscribe<StorageMessage.PrepareAck>(requestManagement);
            Bus.Subscribe<StorageMessage.WrongExpectedVersion>(requestManagement);
            Bus.Subscribe<StorageMessage.InvalidTransaction>(requestManagement);
            Bus.Subscribe<StorageMessage.StreamDeleted>(requestManagement);
            Bus.Subscribe<StorageMessage.RequestManagerTimerTick>(requestManagement);


            var internalAuthFactory = new InternalAuthenticationProviderFactory();
            internalAuthFactory.BuildAuthenticationProvider(Queue, Bus, workersHandler, workerBusses);
            Queue.Start();
        }

        public void Start()
        {
            Queue.Publish(new SystemMessage.SystemInit());
        }

        public void Stop()
        {
            var shutdownEvent = new ManualResetEventSlim(false);
            Bus.Subscribe(new AdHocHandler<SystemMessage.BecomeShutdown>(m => shutdownEvent.Set()));
            Queue.Publish(new ClientMessage.RequestShutdown(exitProcess: false));
            if (!shutdownEvent.Wait(20000))
                throw new TimeoutException("Has not shut down");
        }

        protected static TFChunkDbConfig GetMemDbConfig(string dbPath, bool inMemDb)
        {
            ICheckpoint writerChk = new InMemoryCheckpoint(Checkpoint.Writer);
            ICheckpoint chaserChk = new InMemoryCheckpoint(Checkpoint.Chaser);
            ICheckpoint epochChk = new InMemoryCheckpoint(Checkpoint.Epoch, initValue: -1);
            ICheckpoint truncateChk = new InMemoryCheckpoint(Checkpoint.Truncate, initValue: -1);
            var nodeConfig = new TFChunkDbConfig(dbPath,
                                                 new VersionedPatternFileNamingStrategy(dbPath, "chunk-"),
                                                 TFConsts.ChunkSize,
                                                 TFConsts.ChunksCacheSize,
                                                 writerChk,
                                                 chaserChk,
                                                 epochChk,
                                                 truncateChk,
                                                 inMemDb);
            return nodeConfig;
        }
    }
}
