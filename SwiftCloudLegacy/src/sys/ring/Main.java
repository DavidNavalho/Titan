package sys.ring;

import static sys.Sys.Sys;

import java.util.Random;

import sys.dht.catadupa.Catadupa;
import sys.dht.catadupa.Catadupa.Scope;

public class Main {

	public static void main(String[] args) throws Exception {

		sys.Sys.init();

		Sys.setDatacenter("datacenter-" + new Random().nextInt(3));
		System.err.println(Sys.getDatacenter());

		Catadupa.setScopeAndDomain(Scope.UNIVERSAL, "SwiftSequencerRing");
		new AbstractSequencerNode().init();

		// DHT stub = Sys.getDHT_ClientStub();
		//
		// while (stub != null) {
		// String key = "" + Sys.rg.nextInt(1000);
		// stub.send(new StringKey(key), new StoreData(Sys.rg.nextDouble()), new
		// KVS.ReplyHandler() {
		// @Override
		// public void onReceive(StoreDataReply reply) {
		// System.out.println(reply.msg);
		// }
		// });
		// Threading.sleep(1000);
		// }

	}
}
