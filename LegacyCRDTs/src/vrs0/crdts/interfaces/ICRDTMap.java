package vrs0.crdts.interfaces;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import vrs0.clocks.EventClock;

public interface ICRDTMap<K extends Serializable, V extends Serializable>
        extends Serializable, CvRDT {
    /**
     * Returns true if e is in the set
     * 
     * @param e
     * @return
     */
    boolean lookup(K e);

    /**
     * Returns the set of elements in the OR-set
     * 
     * @return
     */
    Map<K, Set<V>> getValue();

    /**
     * Insert element V in the set. Use CRDTRuntime to geenrate new timestamp.
     * 
     * @param e
     */
    void insert(K k, V e);

    /**
     * Insert element V in the set, using the given unique identifier.
     * 
     * @param e
     */
    void insert(K k, V e, EventClock clk);

    /**
     * Delete element e from the set.
     * 
     * @param e
     */
    void delete(K k);

    /**
     * Delete element e from the set, using the given timestamp.
     * 
     * @param e
     */
    void delete(K k, EventClock clk);

}
