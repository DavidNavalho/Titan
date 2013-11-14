package vrs0.crdts.more;

import vrs0.clocks.CausalityClock;
import vrs0.clocks.EventClock;
import vrs0.crdts.interfaces.CvRDT;
import vrs0.crdts.runtimes.CRDTRuntime;
import vrs0.exceptions.IncompatibleTypeException;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CRDTDouble implements CvRDT {
    private static final long serialVersionUID = 1L;
    private Map<String, Double> adds;
    private Map<String, Double> rems;
    private double val;

    public CRDTDouble(double initial) {
        this.val = initial;
        this.adds = new HashMap<String, Double>();
        this.rems = new HashMap<String, Double>();
    }

    public CRDTDouble() {
        this(0);
    }

    public double value() {
        return this.val;
    }

    public double add(double n) {
        String siteId = CRDTRuntime.getInstance().siteId();
        return add(n, siteId);
    }

    public double add(double n, EventClock ec) {
        String siteId = ec.getIdentifier();
        return add(n, siteId);
    }

    private synchronized double add(double n, String siteId) {
        if (n < 0) {
            return sub(-n, siteId);
        }

        double v;
        if (this.adds.containsKey(siteId)) {
            v = this.adds.get(siteId) + n;
        } else {
            v = n;
        }

        this.adds.put(siteId, v);
        this.val += n;
        return this.val;
    }

    public double sub(double n) {
        return sub(n, CRDTRuntime.getInstance().siteId());
    }

    public double sub(double n, EventClock ec) {
        return sub(n, ec.getIdentifier());
    }

    private double sub(double n, String siteId) {
        if (n < 0) {
            return add(-n, siteId);
        }
        double v;
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
        if (!(other instanceof CRDTDouble)) {
            throw new IncompatibleTypeException();
        }

        CRDTDouble that = (CRDTDouble) other;
        for (Entry<String, Double> e : that.adds.entrySet()) {
            if (!this.adds.containsKey(e.getKey())) {
                double v = e.getValue();
                this.val += v;
                this.adds.put(e.getKey(), v);
            } else {
                double v = this.adds.get(e.getKey());
                if (v < e.getValue()) {
                    this.val = this.val - v + e.getValue();
                    this.adds.put(e.getKey(), e.getValue());
                }
            }
        }

        for (Entry<String, Double> e : that.rems.entrySet()) {
            if (!this.rems.containsKey(e.getKey())) {
                double v = e.getValue();
                this.val -= v;
                this.rems.put(e.getKey(), v);
            } else {
                double v = this.rems.get(e.getKey());
                if (v < e.getValue()) {
                    this.val = this.val + v - e.getValue();
                    this.rems.put(e.getKey(), e.getValue());
                }
            }
        }
    }

    @Override
    public boolean equals(CvRDT other) {
        if (!(other instanceof CRDTDouble)) {
            return false;
        }
        CRDTDouble that = (CRDTDouble) other;
        return that.val == this.val && that.adds.equals(this.adds)
                && that.rems.equals(this.rems);
    }

    // TODO Reimplement the hashCode() method!!!

}
