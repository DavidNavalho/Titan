package vrs0.crdts;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import vrs0.clocks.CausalityClock;
import vrs0.clocks.EventClock;
import vrs0.crdts.interfaces.CvRDT;
import vrs0.crdts.interfaces.CvRDTReferenceOverBucket;
import vrs0.crdts.interfaces.ICRDTMap;
import vrs0.crdts.runtimes.CRDTRuntime;
import vrs0.exceptions.IncompatibleTypeException;
import vrs0.utils.Pair;

/**
 * CRDT OR-Map with tombstones.
 * 
 * @author vb
 * 
 * @param <V>
 * 
 *            TODO: Should cache value when getValue()?
 * 
 */
public class ORMap<K extends Serializable, V extends Serializable> implements
        CvRDT, ICRDTMap<K, V>, CvRDTReferenceable {

    private static final long serialVersionUID = 1L;
    private Map<K, Set<Pair<V, EventClock>>> elems;
    private Set<EventClock> tomb; // tombstones

    public ORMap() {
        elems = new HashMap<K, Set<Pair<V, EventClock>>>();
        tomb = new HashSet<EventClock>();
    }

    /**
     * Returns true if e is in the set
     * 
     * @param e
     * @return
     */
    public synchronized boolean lookup(K k) {
        return elems.containsKey(k);
    }

    /**
     * Returns the set of elements in the OR-set
     * 
     * @return
     */
    public synchronized Map<K, Set<V>> getValue() {
        Map<K, Set<V>> retValues = new HashMap<K, Set<V>>();
        for (Entry<K, Set<Pair<V, EventClock>>> entry : elems.entrySet()) {
            Set<V> keyValues = new HashSet<V>();
            for (Pair<V, EventClock> ve : entry.getValue()) {
                keyValues.add(ve.getFirst());
            }
            retValues.put(entry.getKey(), keyValues);
        }
        return retValues;
    }

    /**
     * Insert element V in the set. Use CRDTRuntime to geenrate new Timestamp.
     * 
     * @param e
     */
    public void insert(K k, V e) {
        insert(k, e, CRDTRuntime.getInstance().nextEventClock());
    }

    /**
     * Insert element V in the set, using the given unique identifier.
     * 
     * @param e
     */
    public synchronized void insert(K k, V e, EventClock clk) {
        Set<Pair<V, EventClock>> s = elems.get(k);
        if (s == null) {
            s = new HashSet<Pair<V, EventClock>>();
            elems.put(k, s);
        } else {
            for (Pair<V, EventClock> p : s) {
                tomb.add(p.getSecond());
            }
        }
        s.clear();
        s.add(new Pair<V, EventClock>(e, clk));
    }

    /**
     * Delete element e from the set.
     * 
     * @param e
     */
    public void delete(K k) {
        delete(k, CRDTRuntime.getInstance().nextEventClock());
    }

    /**
     * Delete element e from the set, using the given Timestamp.
     * 
     * @param e
     */
    public synchronized void delete(K k, EventClock clk) {
        Set<Pair<V, EventClock>> s = elems.get(k);
        if (s == null) {
            return;
        }
        for (Pair<V, EventClock> entry : s) {
            tomb.add(entry.getSecond());
        }
        elems.remove(k);
    }

    @Override
    public synchronized void merge(CvRDT oo, CausalityClock thisClock,
            CausalityClock ooClock) throws IncompatibleTypeException {
        if (!(oo instanceof ORMap)) {
            throw new IncompatibleTypeException();
        }
        ORMap<K, V> o = (ORMap<K, V>) oo;
        tomb.addAll(o.tomb);
        Iterator<Entry<K, Set<Pair<V, EventClock>>>> it = o.elems.entrySet()
                .iterator();
        while (it.hasNext()) {
            Entry<K, Set<Pair<V, EventClock>>> e = it.next();
            Set<Pair<V, EventClock>> s = elems.get(e.getKey());
            if (s == null) {
                elems.put(e.getKey(),
                        new HashSet<Pair<V, EventClock>>(e.getValue()));
            } else {
                // TODO: Qual é o eventClock que fica associado ao valor que
                // foi merged?
                boolean crdts = false;
                // if (s.size() == 1 && e.getValue().size() == 1) {
                // Pair<V, EventClock> thisPair = s.iterator().next();
                // Pair<V, EventClock> otherPair = e.getValue().iterator()
                // .next();
                // if (thisPair.getFirst() instanceof CvRDT
                // && otherPair.getFirst() instanceof CvRDT) {
                // ((CvRDT) thisPair.getFirst()).merge(
                // (CvRDT) otherPair.getFirst(), thisClock,
                // ooClock);
                // crdts = true;
                // }
                // }
                if (!crdts) {
                    s.addAll(e.getValue());
                }
            }
        }

        it = elems.entrySet().iterator();
        while (it.hasNext()) {
            Entry<K, Set<Pair<V, EventClock>>> e = it.next();
            Set<Pair<V, EventClock>> s = e.getValue();
            Iterator<Pair<V, EventClock>> itS = s.iterator();
            while (itS.hasNext()) {
                Pair<V, EventClock> p = itS.next();
                if (tomb.contains(p.getSecond())) {
                    itS.remove();
                }
            }

            if (s.size() == 0) {
                it.remove();
            }
        }
    }

    @Override
    public boolean equals(CvRDT o) {
        if (!(o instanceof ORMap)) {
            return false;
        }
        ORMap oi = (ORMap) o;
        return oi.elems.equals(elems) && oi.tomb.equals(tomb);
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("{");
        Iterator<Entry<K, Set<Pair<V, EventClock>>>> it = elems.entrySet()
                .iterator();
        while (it.hasNext()) {
            Entry<K, Set<Pair<V, EventClock>>> e = it.next();
            buf.append(e.getKey());
            buf.append("->[");
            Iterator<Pair<V, EventClock>> itE = e.getValue().iterator();
            while (itE.hasNext()) {
                Pair<V, EventClock> ev = itE.next();
                buf.append(ev);
                if (itE.hasNext()) {
                    buf.append(",");
                }
            }
            buf.append("]");
            if (it.hasNext()) {
                buf.append(",");
            }
        }
        /*
         * Iterator<V> it = elems.keySet().iterator(); while( it.hasNext()) { V
         * e = it.next(); buf.append(e); if( it.hasNext()) buf.append(","); }
         */buf.append("}");
        return buf.toString();
    }

    public CvRDTReferenceOverBucket processReferenceValues() {
        return new CvRDTReferenceOverBucket() {

            @Override
            public void storeReferences(final String bucket, final String key) {
                for (Entry<K, Set<Pair<V, EventClock>>> e : elems.entrySet()) {
                    for (Pair<V, EventClock> pair : e.getValue()) {
                        if (pair.getFirst() instanceof CvRDTReferenceable) {
                            CvRDTReferenceable ref = ((CvRDTReferenceable) pair
                                    .getFirst());
                            CvRDTReferenceOverBucket handler = ref.processReferenceValues();
                            if (handler != null) {
                                //TODO + "_" + counter++ antes tinhamos este prefixo na chave, agora nao se mudam referencias
                                handler.storeReferences(bucket,
                                        key + e.getKey() );
                            }
                        } else {
                            break;
                        }
                    }
                }
            }

        };
    }

    public Set<V> get(K key) {
        Set<Pair<V, EventClock>> values = elems.get(key);
        Set<V> ret = new HashSet<V>();
        if (values != null) {
            for (Pair<V, EventClock> p : values) {
                ret.add(p.getFirst());
            }
            return ret;
        } else
            return null;

    }

}
