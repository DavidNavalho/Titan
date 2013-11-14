package swift.crdt.operations;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.DirectoryVersioned;
import swift.crdt.interfaces.CRDTUpdate;

public class DirectoryRemoveUpdate extends BaseUpdate<DirectoryVersioned> {
    private Set<TripleTimestamp> toBeRemoved;
    private CRDTIdentifier key;

    public DirectoryRemoveUpdate() {
        // Method stub for kryo
    }

    public DirectoryRemoveUpdate(CRDTIdentifier key, Set<TripleTimestamp> toBeRemoved, TripleTimestamp ts) {
        super(ts);
        this.key = key;
        this.toBeRemoved = toBeRemoved;
    }

    @Override
    public CRDTUpdate<DirectoryVersioned> withBaseTimestamp(Timestamp ts) {
        // TODO Check if deep copy of removed ids is necessary
        return new DirectoryRemoveUpdate(this.key, this.toBeRemoved, getTimestamp().withBaseTimestamp(ts));
    }

    @Override
    public void replaceDependeeOperationTimestamp(Timestamp oldTs, Timestamp newTs) {
        final List<TripleTimestamp> newIds = new LinkedList<TripleTimestamp>();
        final Iterator<TripleTimestamp> iter = toBeRemoved.iterator();
        while (iter.hasNext()) {
            final TripleTimestamp id = iter.next();
            if (oldTs.includes(id)) {
                newIds.add(id.withBaseTimestamp(newTs));
                iter.remove();
            }
        }
        toBeRemoved.addAll(newIds);

    }

    @Override
    public void applyTo(DirectoryVersioned crdt) {
        crdt.applyRemove(key, toBeRemoved, getTimestamp());
    }

}
