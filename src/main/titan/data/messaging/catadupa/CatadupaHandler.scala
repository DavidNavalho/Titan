package main.titan.data.messaging.catadupa

import sys.dht.api.DHT
import sys.dht.api.DHT.{MessageHandler, Key, Handle}
import main.java.TitanHandler
import main.titan.data.messaging.Messaging.TitanMessage

/**
 * Created by davidnavalho on 04/12/13.
 */
@deprecated
class CatadupaHandler{

	/*abstract class TitanRequestHandler extends DHT.AbstractMessageHandler{
		abstract onReceive(con: DHT.Handle, key: DHT.Key, msg: CatadupaRequest)
	}*/
/*
	def deliverTo(conn: Handle, key: Key, handler: MessageHandler) {
		handler.asInstanceOf[TitanHandler.TitanRequestHandler].onReceive(conn, key, this)
	}*/
}
