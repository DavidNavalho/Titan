package swift.client;

import java.util.HashMap;
import java.util.Map;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDT;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.ObjectUpdatesListener;
import swift.crdt.interfaces.TxnHandle;
import swift.crdt.interfaces.TxnLocalCRDT;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

/**
 * Implementation of {@link swift.crdt.interfaces.IsolationLevel#SNAPSHOT_ISOLATION} transaction,
 * which always read from a consistent snapshot.
 * <p>
 * <b>Implementation notes<b>. Each transaction defines a snapshot point which
 * is a set of update transactions visible to this transaction at the beginning
 * of the transaction. This set includes globally committed transaction visible
 * at the beginning of the transaction. {@link TxnManager} includes also all
 * previously locally committed transactions.
 * 
 * @author mzawirski
 */
class SnapshotIsolationTxnHandle extends AbstractTxnHandle implements TxnHandle {
    final CausalityClock visibleTransactionsClock;
    final Map<CRDTIdentifier, TxnLocalCRDT<?>> objectViewsCache;

    /**
     * @param manager
     *            manager maintaining this transaction
     * @param cachePolicy
     *            cache policy used by this transaction
     * @param localTimestamp
     *            local timestamp used for local operations of this transaction
     * @param globalVisibleTransactionsClock
     *            clock representing globally committed update transactions
     *            visible to this transaction; left unmodified
     */
    SnapshotIsolationTxnHandle(final TxnManager manager, final CachePolicy cachePolicy, final Timestamp localTimestamp,
            final CausalityClock globalVisibleTransactionsClock) {
        super(manager, cachePolicy, localTimestamp);
        this.visibleTransactionsClock = globalVisibleTransactionsClock.clone();
        this.objectViewsCache = new HashMap<CRDTIdentifier, TxnLocalCRDT<?>>();
        updateUpdatesDependencyClock(visibleTransactionsClock);
    }

    @Override
    protected <V extends CRDT<V>, T extends TxnLocalCRDT<V>> T getImpl(CRDTIdentifier id, boolean create,
            Class<V> classOfV, ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
            VersionNotFoundException, NetworkException {
        TxnLocalCRDT<V> localView = (TxnLocalCRDT<V>) objectViewsCache.get(id);
        if (localView == null) {
            localView = manager.getObjectVersionTxnView(this, id, visibleTransactionsClock, create, classOfV,
                    updatesListener);
            objectViewsCache.put(id, localView);
        }
        return (T) localView;
    }
}
