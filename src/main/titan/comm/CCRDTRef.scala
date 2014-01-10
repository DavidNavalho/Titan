package main.titan.comm

import akka.actor.ActorRef
import sys.dht.api.DHT
import sys.Sys.Sys
import main.titan.data.messaging.Messaging.TitanMessage


/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 03/12/13
 * Time: 13:57
 * To change this template use File | Settings | File Templates.
 */
class CCRDTRef(actor: ActorRef) {
	var rpc: Boolean = false;
	var stub: DHT = null

	def this(rpc: Boolean) = {
		this(null)
		this.rpc = rpc
		this.stub = Sys.getDHT_ClientStub
	}

	def message(msg: TitanMessage){
		if(rpc){
//			this.stub.send(msg, msg)
		}
		else
			this.actor ! msg
	}

}
