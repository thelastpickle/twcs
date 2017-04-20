package com.jeffjirsa.cassandra.db.compaction;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import java.util.Set;

/**
 * Created by jhaddad on 4/19/17.
 */

// added by Jon.
class TimeWindowCompactionTask extends CompactionTask
{

    public TimeWindowCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, boolean offline)
    {
        super(cfs, txn, gcBefore, offline);
    }
    @Override
    public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables)
    {
        // should return a
        return new TTLAwareWriter(cfs, txn, nonExpiredSSTables, false);
    }

}
