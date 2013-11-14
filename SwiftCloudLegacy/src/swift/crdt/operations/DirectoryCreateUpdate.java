package swift.crdt.operations;

import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.CRDTIdentifier;
import swift.crdt.DirectoryVersioned;
import swift.crdt.interfaces.CRDTUpdate;

public class DirectoryCreateUpdate extends BaseUpdate<DirectoryVersioned> {
    CRDTIdentifier entry;

    public DirectoryCreateUpdate() {
        // method stub for kryo...
    }

    public DirectoryCreateUpdate(CRDTIdentifier val, TripleTimestamp ts) {
        // TODO Check that key and type of CRDT are consistent
        super(ts);
        this.entry = val;
    }

    @Override
    public CRDTUpdate<DirectoryVersioned> withBaseTimestamp(Timestamp ts) {
        return new DirectoryCreateUpdate(this.entry, getTimestamp().withBaseTimestamp(ts));
    }

    @Override
    public void replaceDependeeOperationTimestamp(Timestamp oldTs, Timestamp newTs) {
        // does not rely on any timestamp dependency.
    }

    @Override
    public void applyTo(DirectoryVersioned crdt) {
        crdt.applyPut(entry, getTimestamp());
    }

}
