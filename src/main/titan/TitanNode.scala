package main.titan

import sys.dht.DHT_Node
import main.java.TitanHandler
import sys.dht.api.DHT.{Key, Handle}
import main.titan.data.messaging.Messaging.{CRDTCreationTitanMessage, TitanMessage}
import akka.actor._
import scala.util.Random
import sys.dht.api.DHT
import sys.Sys.Sys
import main.titan.data.messaging.catadupa.{CatadupaRequest, CatadupaReply, CatadupaHandler}
import main.java.TitanHandler.TitanRequestHandler
import akka.cluster.ClusterEvent.ClusterDomainEvent
import akka.cluster.Cluster

import akka.serialization._
import java.net.InetAddress
import main.titan.data.ccrdt.ComputationalCRDT
import main.hacks.data.ccrdts.ORSetCCRDT

/**
 * Created by davidnavalho on 04/12/13.
 */
class TitanNode extends DHT_Node{

	import com.typesafe.config.ConfigFactory
	val cf = ConfigFactory.load("TitanNode")
	val actorSystem = ActorSystem("TitanNode", cf)
	val titan: ActorRef = actorSystem.actorOf(Props[TitanActor](new TitanActor))//(new TitanActor()))
	/*val reader: ComputationalCRDT = new ORSetCCRDT("reader",1);
	titan ! new CRDTCreationTitanMessage(reader);*/

//	Cluster(actorSystem).subscribe(titan, classOf[ClusterDomainEvent])
	//	private val actorSystem: ActorSystem = ActorSystem("TitanNode"+Random.nextInt())
//	private val titan: ActorRef = actorSystem.actorOf(Props[TitanActor](new TitanActor))
	/*println("path: "+titan.path)
	println("path_address: "+titan.path.address)
	println("path_serialization: "+titan.path.toSerializationFormat)
	println("path_serialization_with_address: "+titan.path.toSerializationFormatWithAddress(titan.path.address))
	println("path_string_with_address: "+titan.path.toStringWithAddress(titan.path.address))
	println("host: "+titan.path.address.host.isDefined)
	println("port: "+titan.path.address.port.isDefined)
	println("protocol: "+titan.path.address.protocol)
	println("system: "+titan.path.address.system)
	println(InetAddress.getLocalHost.getHostAddress)
	println(ConfigFactory.load("TitanNode").getNumber("akka.remote.netty.tcp.port"))*/
//				println(titan.toString())
	val systemName: String = titan.path.address.system
	val path: String = titan.path.toStringWithAddress(titan.path.address)
	val split: Array[String] = path.split("akka://"+systemName)
	val splitPath: String = split(1)
	val address: String = InetAddress.getLocalHost.getHostAddress
	val port: String = cf.getNumber("akka.remote.netty.tcp.port")+""

	var actorRefString: String = "akka.tcp://"+systemName+"@"+address+":"+port+splitPath
	println(actorRefString)
//	sys.Sys.init()
	val requestHandler: TitanRequestHandler = new SysNodeRequestHandler
	DHT_Node.setHandler(this.requestHandler)
	var stub: DHT = Sys.getDHT_ClientStub
	println("TitanNode initialized")

	/*private class SysNodeReplyHandler extends TitanHandler.TitanReplyHandler{

		def onReceive(con: Handle, key: Key, msg: CatadupaReply) {
//			titan ! msg.msg
		}

	}*/

	private class SysNodeRequestHandler extends TitanRequestHandler{

		//this is only to get the actorRef...which this one already contains, so I can simply answer immediately with it!
		@Override
		def onReceive(con: Handle, key: Key, msg: CatadupaRequest){
//			println("#>TitanNode received an ActorRef request")
//			titan ! msg.message
			val reply = new CatadupaReply



//			reply.setActor(titan)
			reply.setData(actorRefString)

			con.reply(reply)
			println("#>TitanNode sent actorRef back: "+titan.toString()+" to: "+con.toString)
		}
	}

}
