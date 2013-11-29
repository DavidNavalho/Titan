package main.titan.data.messaging

import main.titan.data.ccrdt.ComputationalCRDT
import main.titan.computation.Trigger
import scala.collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 19/11/13
 * Time: 15:01
 * To change this template use File | Settings | File Templates.
 */
object Messaging {
	case class TriggerMessage(trigger:Trigger, ccrdt: ComputationalCRDT);
	case class DataMessage(titanData: TitanData);
	case class TargetedDataMessage(target: String, titanData: TitanData);
	case class TargetedDataMessageWithReply(target: String, titanData: TitanData);
	case class CRDTSyncMessage(data: ComputationalCRDT, partitionKey: Long)
	case class CRDTMessage(data: ListBuffer[TitanData])

	case class EOE(ccrdtName: String, data: EOEData)
//	case class EOE(ccrdtName: String)

	case class CRDTCreationMessage(ccrdt: ComputationalCRDT)
	case class EpochSync(source: String, epoch: Int)
	case class IterationSync(source: String, epoch: Int, iteration: Int)

	case class ManualCRDTSyncMessage(data: ComputationalCRDT, partitionKey: Long, myPartition: Int, myPartitioningSize: Int)

	case class CCRDTDataRequest(target: String, source: String, iterationStep: Int)
	case class CCRDTDataRequestReply(data: ComputationalCRDT, target: String, iterationStep: Int, expectedReplies: Int)
	case class IterationCheckSyncMessage(target: String, iterationStep: Int, stop: Boolean)

	//TODO: not using this right now, need to use a local reader
	class EOEData(start: Int, end: Int){
		val startValue: Int = start
		val endValue: Int = end
	}

	class IterationMetadata(){

	}

}
