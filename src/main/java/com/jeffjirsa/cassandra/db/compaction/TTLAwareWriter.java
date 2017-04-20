package com.jeffjirsa.cassandra.db.compaction;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;

import java.util.HashMap;
import java.util.Set;

/**
 * Created by jhaddad on 4/19/17.
 */

class TTLAwareWriter extends CompactionAwareWriter
{

    private HashMap<Integer, SSTableWriter> writers = new HashMap<>();
    public TTLAwareWriter(ColumnFamilyStore cfs, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, boolean offline) {
        super(cfs, txn, nonExpiredSSTables, offline);
        writers = new HashMap<>();
    }

    @Override
    public boolean append(AbstractCompactedRow abstractCompactedRow)
    {

        RowIndexEntry rie = this.sstableWriter.append(abstractCompactedRow);
        return rie != null;
    }
}