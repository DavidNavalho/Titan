package sys.net.impl.providers;

import static sys.Sys.Sys;
import static sys.stats.TcpStats.TcpStats;
import sys.net.api.MessageHandler;
import sys.net.api.TransportConnection;
import sys.net.impl.AbstractMessage;

public class TcpPong extends AbstractMessage  {

	double timestamp;
	
	public TcpPong(){}
	
	public TcpPong( TcpPing ping) {
		this.timestamp = ping.timestamp;
	}

	public double rtt() {
		return Sys.currentTime() - timestamp;
	}

	public void deliverTo( TransportConnection conn, MessageHandler handler) {
		TcpStats.logRpcRTT(conn.remoteEndpoint(), this.rtt() ) ;
	}
}