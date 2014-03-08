package main.titan.data.messaging

import main.titan.data.ccrdt.{CCRDTSkeleton, ComputationalCRDT}
import main.titan.computation.Trigger
import scala.collection.mutable.ListBuffer
import sys.dht.api.DHT.{Message, Key}
import akka.actor.ActorRef
import main.titan.comm.CCRDTRef

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 19/11/13
 * Time: 15:01
 * To change this template use File | Settings | File Templates.
 */
object Messaging {

	//I can split my messages in groups, in case I want to somehow differentiate them
	sealed abstract trait TitanMessage
	case class TriggerTitanMessage(trigger:Trigger, ccrdt: ComputationalCRDT) extends TitanMessage
	case class DataTitanMessage(titanData: TitanData) extends TitanMessage
	case class TargetedDataTitanMessage(target: String, titanData: TitanData) extends TitanMessage
	case class TargetedDataTitanMessageWithReply(target: String, titanData: TitanData) extends TitanMessage
	case class CRDTSyncTitanMessage(data: ComputationalCRDT, partitionKey: Long) extends TitanMessage
	case class CRDTTitanMessage(data: ListBuffer[TitanData]) extends TitanMessage

	case class EOE(ccrdtName: String, data: EOEData) extends TitanMessage
//	case class EOE(ccrdtName: String)

	case class CRDTCreationTitanMessage(ccrdt: ComputationalCRDT) extends TitanMessage
  case class RemoteCreateCRDT(ccrdt: ComputationalCRDT, partition: Int, size: Int, partitionKey: Long, refs: CCRDTRef) extends TitanMessage
  case class RemoteCreateTrigger(trigger: Trigger, targetHollowReplica: ComputationalCRDT, key: Long) extends TitanMessage
  case class RemoteAddData(data: TitanData, key: Long) extends TitanMessage
	case class EpochSync(source: String, epoch: Int) extends TitanMessage
	case class IterationSync(source: String, epoch: Int, iteration: Int) extends TitanMessage

	case class ManualCRDTSyncTitanMessage(data: ComputationalCRDT, partitionKey: Long, myPartition: Int, myPartitioningSize: Int) extends TitanMessage

	case class CCRDTDataRequest(target: String, source: String, iterationStep: Int) extends TitanMessage
	case class CCRDTDataRequestReply(data: ComputationalCRDT, target: String, iterationStep: Int, expectedReplies: Int) extends TitanMessage
	case class IterationCheckSyncTitanMessage(target: String, iterationStep: Int, stop: Boolean) extends TitanMessage

	//TODO: not using this right now, need to use a local reader
	class EOEData(start: Int, end: Int){
		val startValue: Int = start
		val endValue: Int = end
	}
}
