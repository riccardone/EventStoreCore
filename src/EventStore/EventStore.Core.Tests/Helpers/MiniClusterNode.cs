﻿// Copyright (c) 2012, Event Store LLP
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
// 
// Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
// Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
// Neither the name of the Event Store LLP nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// 

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using EventStore.Common.Log;
using EventStore.Common.Utils;
using EventStore.Core.Authentication;
using EventStore.Core.Bus;
using EventStore.Core.Cluster.Settings;
using EventStore.Core.Messages;
using EventStore.Core.Services.Gossip;
using EventStore.Core.Services.Monitoring;
using EventStore.Core.Settings;
using EventStore.Core.Tests.Http;
using EventStore.Core.Tests.Services.Transport.Tcp;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;

namespace EventStore.Core.Tests.Helpers
{
    public class MiniClusterNode
    {
        private static bool _running;

        public static int RunCount = 0;
        public static readonly Stopwatch RunningTime = new Stopwatch();
        public static readonly Stopwatch StartingTime = new Stopwatch();
        public static readonly Stopwatch StoppingTime = new Stopwatch();

        public const int ChunkSize = 1024*1024;
        public const int CachedChunkSize = ChunkSize + ChunkHeader.Size + ChunkFooter.Size;

        private static readonly ILogger Log = LogManager.GetLoggerFor<MiniClusterNode>();

        public IPEndPoint InternalTcpEndPoint { get; private set; }
        public IPEndPoint InternalTcpSecEndPoint { get; private set; }
        public IPEndPoint InternalHttpEndPoint { get; private set; }
        public IPEndPoint ExternalTcpEndPoint { get; private set; }
        public IPEndPoint ExternalTcpSecEndPoint { get; private set; }
        public IPEndPoint ExternalHttpEndPoint { get; private set; }

        public readonly ClusterVNode Node;
        public readonly TFChunkDb Db;
        private readonly string _dbPath;

        public MiniClusterNode(
            string pathname, IPEndPoint internalTcpPort, IPEndPoint internalTcpSecPort, IPEndPoint internalHttpPort, IPEndPoint externalTcpPort,
            IPEndPoint externalTcpSecPort, IPEndPoint externalHttpPort, IPEndPoint[] gossipSeeds, ISubsystem[] subsystems = null,
            int? chunkSize = null, int? cachedChunkSize = null, bool enableTrustedAuth = false,
            bool skipInitializeStandardUsersCheck = true, int memTableSize = 1000, bool inMemDb = true)
        {
            if (_running) throw new Exception("Previous MiniNode is still running!!!");
            _running = true;

            RunningTime.Start();
            RunCount += 1;

            IPAddress ip = IPAddress.Loopback; //GetLocalIp();

            _dbPath = Path.Combine(
                pathname,
                string.Format("mini-cluster-node-db-{0}-{1}-{2}", externalTcpPort, externalTcpSecPort, externalHttpPort));

            Directory.CreateDirectory(_dbPath);
            Db =
                new TFChunkDb(
                    CreateDbConfig(chunkSize ?? ChunkSize, _dbPath, cachedChunkSize ?? CachedChunkSize, inMemDb));

            InternalTcpEndPoint = internalTcpPort;
            InternalTcpSecEndPoint = internalTcpSecPort;
            InternalHttpEndPoint = internalHttpPort;

            ExternalTcpEndPoint = externalTcpPort;
            ExternalTcpSecEndPoint = externalTcpSecPort;
            ExternalHttpEndPoint = externalHttpPort;

            var singleVNodeSettings = new ClusterVNodeSettings(
                Guid.NewGuid(), InternalTcpEndPoint, InternalTcpSecEndPoint, InternalHttpEndPoint, ExternalTcpEndPoint,
                ExternalTcpSecEndPoint, ExternalHttpEndPoint, new[] {ExternalHttpEndPoint.ToHttpUrl()},
                enableTrustedAuth, ssl_connections.GetCertificate(), 1, false, "", gossipSeeds, TFConsts.MinFlushDelayMs,
                3, 2, 2, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2), false, "", false, TimeSpan.FromHours(1),
                StatsStorage.None, 0, new InternalAuthenticationProviderFactory(), true);

            Log.Info(
                "\n{0,-25} {1} ({2}/{3}, {4})\n" + "{5,-25} {6} ({7})\n" + "{8,-25} {9} ({10}-bit)\n"
                + "{11,-25} {12}\n" + "{13,-25} {14}\n" + "{15,-25} {16}\n" + "{17,-25} {18}\n" + "{19,-25} {20}\n\n",
                "ES VERSION:", VersionInfo.Version, VersionInfo.Branch, VersionInfo.Hashtag, VersionInfo.Timestamp,
                "OS:", OS.OsFlavor, Environment.OSVersion, "RUNTIME:", OS.GetRuntimeVersion(),
                Marshal.SizeOf(typeof (IntPtr))*8, "GC:",
                GC.MaxGeneration == 0
                    ? "NON-GENERATION (PROBABLY BOEHM)"
                    : string.Format("{0} GENERATIONS", GC.MaxGeneration + 1), "DBPATH:", _dbPath, "ExTCP ENDPOINT:",
                ExternalTcpEndPoint, "ExTCP SECURE ENDPOINT:", ExternalTcpSecEndPoint, "ExHTTP ENDPOINT:",
                ExternalHttpEndPoint);

            Node = new ClusterVNode(
                Db, singleVNodeSettings, dbVerifyHashes: true, memTableEntryCount: memTableSize, subsystems: subsystems,
                gossipSeedSource: new KnownEndpointGossipSeedSource(gossipSeeds));
            Node.ExternalHttpService.SetupController(new TestController(Node.MainQueue, Node.NetworkSendService));
        }

        public void Start()
        {
            StartingTime.Start();

            var startedEvent = new ManualResetEventSlim(false);
            Node.MainBus.Subscribe(
                new AdHocHandler<UserManagementMessage.UserManagementServiceInitialized>(m => startedEvent.Set()));

            Node.Start();

            if (!startedEvent.Wait(60000))
                throw new TimeoutException("MiniNode haven't started in 60 seconds.");

            StartingTime.Stop();
        }

        public void Shutdown(bool keepDb = false, bool keepPorts = false)
        {
            StoppingTime.Start();

            var shutdownEvent = new ManualResetEventSlim(false);
            Node.MainBus.Subscribe(new AdHocHandler<SystemMessage.BecomeShutdown>(m => shutdownEvent.Set()));

            Node.Stop();

            if (!shutdownEvent.Wait(20000))
                throw new TimeoutException("MiniNode haven't shut down in 20 seconds.");

            if (!keepPorts)
            {
                PortsHelper.ReturnPort(InternalTcpEndPoint.Port);
                PortsHelper.ReturnPort(InternalTcpSecEndPoint.Port);
                PortsHelper.ReturnPort(InternalHttpEndPoint.Port);
                PortsHelper.ReturnPort(ExternalTcpEndPoint.Port);
                PortsHelper.ReturnPort(ExternalTcpSecEndPoint.Port);
                PortsHelper.ReturnPort(ExternalHttpEndPoint.Port);
            }

            if (!keepDb)
                TryDeleteDirectory(_dbPath);

            StoppingTime.Stop();
            RunningTime.Stop();

            _running = false;
        }

        private void TryDeleteDirectory(string directory)
        {
            try
            {
                Directory.Delete(directory, true);
            }
            catch (Exception e)
            {
                Debug.WriteLine("Failed to remove directory {0}", directory);
                Debug.WriteLine(e);
            }
        }

        private IPAddress GetLocalIp()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            return host.AddressList.FirstOrDefault(ip => ip.AddressFamily == AddressFamily.InterNetwork);
        }

        private TFChunkDbConfig CreateDbConfig(int chunkSize, string dbPath, long chunksCacheSize, bool inMemDb)
        {
            ICheckpoint writerChk;
            ICheckpoint chaserChk;
            ICheckpoint epochChk;
            ICheckpoint truncateChk;
            if (inMemDb)
            {
                writerChk = new InMemoryCheckpoint(Checkpoint.Writer);
                chaserChk = new InMemoryCheckpoint(Checkpoint.Chaser);
                epochChk = new InMemoryCheckpoint(Checkpoint.Epoch, initValue: -1);
                truncateChk = new InMemoryCheckpoint(Checkpoint.Truncate, initValue: -1);
            }
            else
            {
                var writerCheckFilename = Path.Combine(dbPath, Checkpoint.Writer + ".chk");
                var chaserCheckFilename = Path.Combine(dbPath, Checkpoint.Chaser + ".chk");
                var epochCheckFilename = Path.Combine(dbPath, Checkpoint.Epoch + ".chk");
                var truncateCheckFilename = Path.Combine(dbPath, Checkpoint.Truncate + ".chk");
                if (Runtime.IsMono)
                {
                    writerChk = new FileCheckpoint(writerCheckFilename, Checkpoint.Writer, cached: true);
                    chaserChk = new FileCheckpoint(chaserCheckFilename, Checkpoint.Chaser, cached: true);
                    epochChk = new FileCheckpoint(epochCheckFilename, Checkpoint.Epoch, cached: true, initValue: -1);
                    truncateChk = new FileCheckpoint(
                        truncateCheckFilename, Checkpoint.Truncate, cached: true, initValue: -1);
                }
                else
                {
                    writerChk = new MemoryMappedFileCheckpoint(writerCheckFilename, Checkpoint.Writer, cached: true);
                    chaserChk = new MemoryMappedFileCheckpoint(chaserCheckFilename, Checkpoint.Chaser, cached: true);
                    epochChk = new MemoryMappedFileCheckpoint(
                        epochCheckFilename, Checkpoint.Epoch, cached: true, initValue: -1);
                    truncateChk = new MemoryMappedFileCheckpoint(
                        truncateCheckFilename, Checkpoint.Truncate, cached: true, initValue: -1);
                }
            }
            var nodeConfig = new TFChunkDbConfig(
                dbPath, new VersionedPatternFileNamingStrategy(dbPath, "chunk-"), chunkSize, chunksCacheSize, writerChk,
                chaserChk, epochChk, truncateChk, inMemDb);
            return nodeConfig;
        }
    }
}