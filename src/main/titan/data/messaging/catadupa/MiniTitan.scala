package main.titan.data.messaging.catadupa

import akka.actor.{ActorSelection, Actor, ActorRef}
import java.util.concurrent.{LinkedBlockingDeque, BlockingQueue}
import sys.Sys.Sys
import sys.dht.api.DHT
import main.java.TitanHandler
import sys.dht.catadupa.SeedDB
import sys.net.api.Networking

/**
 * Created by davidnavalho on 09/12/13.
 */
class MiniTitan extends Actor{
	val waiters: BlockingQueue[String] = new LinkedBlockingDeque[String]()
	var stub: DHT = null
	var handler: TitanHandler.TitanReplyHandler = null

//	def TitanOnline{

		sys.Sys.init()
		this.handler = new LocalReplyHandler()
//	  SeedDB.addSeedNode(Networking.Networking.resolve("localhost",))
		this.stub = Sys.getDHT_ClientStub
		println("miniTitan Online")
//	}

	def getReference(key: CatadupaKey): ActorSelection = {
		println("Sending request for TitanNode Ref")
		this.stub.send(key, new CatadupaRequest, this.handler)

		println("Waiting for reply")
		val stringActor: String = this.waiters.take()
		println("Got a reply...: "+stringActor)
		context.actorSelection(stringActor)
	}


	private class LocalReplyHandler extends TitanHandler.TitanReplyHandler{

		@Override
		def onReceive(msg: CatadupaReply){
			println("Received CatadupaReply with TitanNode Ref")
//			println(msg.str)
//			println(msg.getActorRef.toString())
			waiters.put(msg.str)
		}
	}

	def receive: Actor.Receive = {
		case key: CatadupaKey => {
			sender ! this.getReference(key)
		}
		case _ => println("miniTitan: unkown message received")
	}
}
