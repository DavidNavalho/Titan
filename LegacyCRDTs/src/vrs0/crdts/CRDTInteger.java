package vrs0.crdts;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import vrs0.clocks.CausalityClock;
import vrs0.clocks.EventClock;
import vrs0.crdts.interfaces.CvRDT;
import vrs0.crdts.runtimes.CRDTRuntime;
import vrs0.exceptions.IncompatibleTypeException;

public class CRDTInteger implements CvRDT {
    private static final long serialVersionUID = 1L;
    private Map<String, Integer> adds;
    private Map<String, Integer> rems;
    private int val;

    public CRDTInteger(int initial) {
        this.val = initial;
        this.adds = new HashMap<String, Integer>();
        this.rems = new HashMap<String, Integer>();
    }

    public CRDTInteger() {
        this(0);
    }

    public int value() {
        return this.val;
    }

    public int add(int n) {
        String siteId = CRDTRuntime.getInstance().siteId();
        return add(n, siteId);
    }

    public int add(int n, EventClock ec) {
        String siteId = ec.getIdentifier();
        return add(n, siteId);
    }

    private synchronized int add(int n, String siteId) {
        if (n < 0) {
            return sub(-n, siteId);
        }

        int v;
        if (this.adds.containsKey(siteId)) {
            v = this.adds.get(siteId) + n;
        } else {
            v = n;
        }

        this.adds.put(siteId, v);
        this.val += n;
        return this.val;
    }

    public int sub(int n) {
        return sub(n, CRDTRuntime.getInstance().siteId());
    }

    public int sub(int n, EventClock ec) {
        return sub(n, ec.getIdentifier());
    }

    private int sub(int n, String siteId) {
        if (n < 0) {
            return add(-n, siteId);
        }
        int v;
        if (this.rems.containsKey(siteId)) {
            v = this.rems.get(siteId) + n;
        } else {
            v = n;
        }

        this.rems.put(siteId, v);
        this.val -= n;
        return this.val;
    }

    @Override
    public synchronized void merge(CvRDT other, CausalityClock thisClock,
            CausalityClock thatClock) throws IncompatibleTypeException {
        if (!(other instanceof CRDTInteger)) {
            throw new IncompatibleTypeException();
        }

        CRDTInteger that = (CRDTInteger) other;
        for (Entry<String, Integer> e : that.adds.entrySet()) {
            if (!this.adds.containsKey(e.getKey())) {
                int v = e.getValue();
                this.val += v;
                this.adds.put(e.getKey(), v);
            } else {
                int v = this.adds.get(e.getKey());
                if (v < e.getValue()) {
                    this.val = this.val - v + e.getValue();
                    this.adds.put(e.getKey(), e.getValue());
                }
            }
        }

        for (Entry<String, Integer> e : that.rems.entrySet()) {
            if (!this.rems.containsKey(e.getKey())) {
                int v = e.getValue();
                this.val -= v;
                this.rems.put(e.getKey(), v);
            } else {
                int v = this.rems.get(e.getKey());
                if (v < e.getValue()) {
                    this.val = this.val + v - e.getValue();
                    this.rems.put(e.getKey(), e.getValue());
                }
            }
        }
    }

    @Override
    public boolean equals(CvRDT other) {
        if (!(other instanceof CRDTInteger)) {
            return false;
        }
        CRDTInteger that = (CRDTInteger) other;
        return that.val == this.val && that.adds.equals(this.adds)
                && that.rems.equals(this.rems);
    }

    // TODO Reimplement the hashCode() method!!!

}
