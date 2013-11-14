package vrs0.crdts.runtimes;

import java.io.Serializable;

import vrs0.clocks.CausalityClock;
import vrs0.clocks.EventClock;
import vrs0.clocks.VersionVector;

import com.google.gson.Gson;

public class CRDTRuntime implements Serializable
{
	private static CRDTRuntime _instance;
	public static CRDTRuntime getInstance() {
		if( _instance == null)
			_instance = new CRDTRuntime();
		return _instance;
	}
	
	protected String siteId = "0";
	protected Gson gson;
	protected VersionVector vv;
	
	public CRDTRuntime() {
		gson = new Gson();
		vv = new VersionVector();
	}
	
	public CRDTRuntime(String siteId) {
		gson = new Gson();
		vv = new VersionVector();
		this.siteId = siteId;
	}
	
	public Gson gson() {
		gson = new Gson();
		return gson;
	}
	
	public EventClock nextEventClock() {
		return vv.recordNext(siteId);
	}

	public CausalityClock getCausalityClock() {
		return vv;
	}

	public String siteId() {
		return siteId;
	}
	public void setSiteId(String siteId) {
		this.siteId = siteId;
	}
}
