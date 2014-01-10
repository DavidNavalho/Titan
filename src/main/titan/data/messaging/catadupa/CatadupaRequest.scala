package main.titan.data.messaging.catadupa

import sys.dht.api.DHT
import main.java.TitanHandler
import sys.dht.api.DHT.{MessageHandler, Key}
import akka.actor.ActorRef
import main.titan.data.messaging.Messaging.TitanMessage

/**
 * Created by davidnavalho on 09/12/13.
 */
class CatadupaRequest extends DHT.Message{

	def deliverTo(conn: DHT.Handle, key: Key, handler: MessageHandler) {
		handler.asInstanceOf[TitanHandler.TitanRequestHandler].onReceive(conn, key, this);
	}

}
