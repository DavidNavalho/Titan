package sys.net.impl.rpc;

import static sys.Sys.Sys;
import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractMessage;

public class RpcPong extends AbstractMessage  {
	
	public double timestamp;

	public RpcPong() {		
	}
	
	public RpcPong( RpcPing other) {
		this.timestamp = other.timestamp;
	}
		
	public double rtt() {
		return Sys.currentTime() - timestamp;
	}
	
	@Override
	public void deliverTo(TransportConnection conn, MessageHandler handler) {
		((RpcFactoryImpl) handler).onReceive(conn, this);
	}	
}
