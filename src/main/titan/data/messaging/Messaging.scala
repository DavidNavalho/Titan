package main.titan.data.messaging

import main.titan.data.ccrdt.ComputationalCRDT
import main.titan.computation.Trigger
import scala.collection.mutable.ListBuffer
import sys.dht.api.DHT.{Handle, Message, Key}
import sys.dht.api.DHT

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 19/11/13
 * Time: 15:01
 * To change this template use File | Settings | File Templates.
 */
object Messaging {

	/*abstract class RequestHandler extends DHT.AbstractMessageHandler{
		def onReceive(conn: DHT.Handle, key: DHT.Key, handler: DHT.MessageHandler)
	} */

	//I can split my messages in groups, in case I want to somehow differentiate them
	sealed abstract trait TitanMessage extends Message with Key{
		def deliverTo(conn: DHT.Handle, key: DHT.Key, handler: DHT.MessageHandler) {
			(handler).onReceive(conn, key, this);
		}
	}
	case class TriggerTitanMessage(longHashValue: Long, trigger:Trigger, ccrdt: ComputationalCRDT) extends TitanMessage
	case class DataTitanMessage(longHashValue: Long, titanData: TitanData) extends TitanMessage
	case class TargetedDataTitanMessage(longHashValue: Long, target: String, titanData: TitanData) extends TitanMessage
	case class TargetedDataTitanMessageWithReply(longHashValue: Long, target: String, titanData: TitanData) extends TitanMessage
	case class CRDTSyncTitanMessage(longHashValue: Long, data: ComputationalCRDT, partitionKey: Long) extends TitanMessage
	case class CRDTTitanMessage(longHashValue: Long, data: ListBuffer[TitanData]) extends TitanMessage

	case class EOE(longHashValue: Long, ccrdtName: String, data: EOEData) extends TitanMessage
//	case class EOE(ccrdtName: String)

	case class CRDTCreationTitanMessage(longHashValue: Long, ccrdt: ComputationalCRDT) extends TitanMessage
	case class EpochSync(longHashValue: Long, source: String, epoch: Int) extends TitanMessage
	case class IterationSync(longHashValue: Long, source: String, epoch: Int, iteration: Int) extends TitanMessage

	case class ManualCRDTSyncTitanMessage(longHashValue: Long, data: ComputationalCRDT, partitionKey: Long, myPartition: Int, myPartitioningSize: Int) extends TitanMessage

	case class CCRDTDataRequest(longHashValue: Long, target: String, source: String, iterationStep: Int) extends TitanMessage
	case class CCRDTDataRequestReply(longHashValue: Long, data: ComputationalCRDT, target: String, iterationStep: Int, expectedReplies: Int) extends TitanMessage
	case class IterationCheckSyncTitanMessage(longHashValue: Long, target: String, iterationStep: Int, stop: Boolean) extends TitanMessage

	//TODO: not using this right now, need to use a local reader
	class EOEData(start: Int, end: Int){
		val startValue: Int = start
		val endValue: Int = end
	}

	class IterationMetadata(){

	}


}
