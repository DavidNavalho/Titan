package swift.dc;

import swift.dc.proto.DHTExecCRDT;
import swift.dc.proto.DHTExecCRDTReply;
import swift.dc.proto.DHTGetCRDT;
import swift.dc.proto.DHTGetCRDTReply;
import swift.dc.proto.DHTSendNotification;
import sys.dht.api.DHT;

/**
 * 
 * The KVS interface for DHT of DataNodes
 *  
 * @author preguica
 */
public interface DHTDataNode {

	/**
	 * Denotes collection of requests/messages that the DHT DataNode
	 * processes/expects
	 * 
	 * @author preguica
	 * 
	 */
	public abstract class RequestHandler extends DHT.AbstractMessageHandler {
        abstract public void onReceive(DHT.Handle con, DHT.Key key, DHTGetCRDT request);
        abstract public void onReceive(DHT.Handle con, DHT.Key key, DHTExecCRDT<?> request);
	}

	/**
	 * Denotes collection of reply/messages that the DHT DataNode client processes
	 * 
	 * @author preguica
	 * 
	 */
	public abstract class ReplyHandler extends DHT.AbstractReplyHandler {
        abstract public void onReceive(DHTGetCRDTReply reply);
        abstract public void onReceive(DHTExecCRDTReply reply);
        abstract public void onReceive(DHTSendNotification notification);
	}
}
