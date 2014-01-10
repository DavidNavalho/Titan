package main.java;

import main.titan.data.messaging.catadupa.CatadupaReply;
import main.titan.data.messaging.catadupa.CatadupaRequest;
import sys.dht.api.DHT;

/**
 * Created by davidnavalho on 04/12/13.
 */
public interface TitanHandler {

    abstract class TitanRequestHandler extends DHT.AbstractMessageHandler{
        abstract public void onReceive(DHT.Handle con, DHT.Key key, CatadupaRequest msg);
    }

    abstract class TitanReplyHandler extends DHT.AbstractReplyHandler{
        abstract public void onReceive(CatadupaReply msg);
    }
}
