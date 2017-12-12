using System;
using System.Collections.Generic;
using EventStore.Core.Index;
using NUnit.Framework;
using EventStore.Core.Index.Hashes;

namespace EventStore.Core.Tests.Index.IndexV1
{
    [TestFixture(PTableVersions.IndexV1,false)]
    [TestFixture(PTableVersions.IndexV1,true)]
    [TestFixture(PTableVersions.IndexV2,false)]
    [TestFixture(PTableVersions.IndexV2,true)]
    [TestFixture(PTableVersions.IndexV3,false)]
    [TestFixture(PTableVersions.IndexV3,true)]
    [TestFixture(PTableVersions.IndexV4,false)]
    [TestFixture(PTableVersions.IndexV4,true)]
    public class when_merging_four_ptables: SpecificationWithDirectoryPerTestFixture
    {
        private readonly List<string> _files = new List<string>();
        private readonly List<PTable> _tables = new List<PTable>();
        private PTable _newtable;
        protected byte _ptableVersion = PTableVersions.IndexV1;
        private IHasher hasher;

        private bool _skipIndexVerify;

        public when_merging_four_ptables(byte version, bool skipIndexVerify){
            _ptableVersion = version;
            _skipIndexVerify = skipIndexVerify;
        }

        [OneTimeSetUp]
        public override void TestFixtureSetUp()
        {
            hasher = new Murmur3AUnsafe();
            base.TestFixtureSetUp();

            for (int i = 0; i < 4; i++)
            {
                _files.Add(GetTempFilePath());

                var table = new HashListMemTable(_ptableVersion, maxSize: 20);
                for (int j=0;j<10;j++)
                {
                    table.Add((ulong)(0x010100000000 << (j + 1)), i + 1, i*j);
                }
                _tables.Add(PTable.FromMemtable(table, _files[i],skipIndexVerify: _skipIndexVerify));
            }
            _files.Add(GetTempFilePath());
            _newtable = PTable.MergeTo(_tables, _files[4], (streamId, hash) => hash << 32 | hasher.Hash(streamId), _ => true, _ => new System.Tuple<string, bool>("", true), _ptableVersion,skipIndexVerify: _skipIndexVerify);
        }

        [OneTimeTearDown]
        public override void TestFixtureTearDown()
        {
            _newtable.Dispose();
            foreach (var ssTable in _tables)
            {
                ssTable.Dispose();
            }
            base.TestFixtureTearDown();
        }

        [Test]
        public void there_are_forty_records_in_merged_index()
        {
            Assert.AreEqual(40, _newtable.Count);
        }

        [Test]
        public void the_items_are_sorted()
        {
            var last = new IndexEntry(ulong.MaxValue, 0, long.MaxValue);
            foreach(var item in _newtable.IterateAllInOrder())
            {
                Assert.IsTrue((last.Stream == item.Stream ? last.Version > item.Version : last.Stream > item.Stream) || 
                             ((last.Stream == item.Stream && last.Version == item.Version) && last.Position > item.Position));
                last = item;
            }
        }
    }
}