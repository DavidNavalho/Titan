package main.titan.data.messaging.catadupa

import sys.dht.api.DHT
import akka.actor._
import sys.dht.api.DHT.Handle
import main.java.TitanHandler
import akka.serialization.{SerializationExtension, Serializer, Serialization}
import com.typesafe.config.ConfigFactory
import akka.remote.WireFormats.ActorRefData
import com.google.protobuf.Message
import com.esotericsoftware.kryo.Serializer


/**
 * Created by davidnavalho on 09/12/13.
 */
class CatadupaReply extends DHT.Reply{

	var str: String = ""

//		var actor: ActorRef = null
//	var bytes: Array[Byte] = null


	def setData(actorRef: String){
		this.str = actorRef
		/*var serialization: Serialization = null
		var serializer: Serializer = null
		serialization = SerializationExtension.get(system)
		serializer = serialization.findSerializerFor(actorRef)*/
//		str = Serialization.serializedActorPath(actorRef)//toBinary(actorRef)

	}

//	val actor: ActorRef = actorRef
//	val identifier: String = Serialization.serializedActorPath(actorRef)
//	println("first: "+identifier)





	def getActorRef: String = {
		this.str
	}

	@Override
	def deliverTo(conn: DHT.Handle, handler: DHT.ReplyHandler) {
		if(conn.expectingReply())
			handler.asInstanceOf[TitanHandler.TitanReplyHandler].onReceive(conn, this);
		handler.asInstanceOf[TitanHandler.TitanReplyHandler].onReceive(this);
	}

}
