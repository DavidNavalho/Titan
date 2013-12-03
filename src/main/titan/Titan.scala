package main.titan

import main.titan.data.ccrdt.{CCRDTSkeleton, ComputationalCRDT}
import akka.actor.{Actor, Props, ActorSystem, ActorRef}
import scala.collection.mutable.{ListBuffer, HashMap}
import main.titan.data.messaging.Messaging._
import main.titan.data.messaging.TitanData
import main.titan.computation.Trigger
import main.titan.data.messaging.Messaging.DataTitanMessage
import main.titan.data.messaging.Messaging.TriggerTitanMessage
import main.titan.data.messaging.Messaging.CRDTCreationTitanMessage
import main.titan.data.messaging.Messaging.TargetedDataTitanMessage

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 14/11/13
 * Time: 15:43
 * To change this template use File | Settings | File Templates.
 */
//TODO: when a Trigger is added, check for existing data: if there is any, it should be retrieved and processed (depending on configuration/keywords...)
//TODO: do not return until a partition/Trigger is actually fully installed (e.g, use '?' in actors)
class Titan extends Actor{
	val partitions: HashMap[Long, ActorRef] = new HashMap[Long, ActorRef]();
	val titanActorSystem: ActorSystem = ActorSystem("TitanMessaging")
	val namedPartitions: HashMap[String, CCRDTSkeleton] = new HashMap[String, CCRDTSkeleton]();


	//TODO: Communication protocol

	//TODO: check for repeated keys
	def addCCRDT(ccrdt: ComputationalCRDT){
		val skel: CCRDTSkeleton = ccrdt.skeleton
		this.namedPartitions.put(skel.reference, skel);
		for(i <- 1 to skel.partitioningSize){
			val partitionKey: Long = skel.getPartitionKey(i);
			val partitionActor: ActorRef = titanActorSystem.actorOf(Props[TitanPartition](new TitanPartition(ccrdt.hollowReplica, self, i, skel.partitioningSize)))
			if(this.partitions.contains(partitionKey))
				println("Titan: partitionKey already exists!!!")
			/*val systemActors = this.partitions.get(partitionKey);
			var list: ListBuffer[ActorRef] = null;
			if(!systemActors.isDefined){
				list = new ListBuffer[ActorRef]();
				this.partitions.put(partitionKey, list)
			}else
				list = systemActors.get;*/
			this.partitions.put(partitionKey, partitionActor);
		}
	}

	//manual method of adding data...definitely not optimal!
	def addData(target: String, data: TitanData){
		//find the correct CCRDT
		val skel: CCRDTSkeleton = this.namedPartitions.get(target).get;
		val key: Long = skel.hashingFunction(data.key);
		val actor: ActorRef = this.partitions.get(key).get;
		actor ! new DataTitanMessage(data);
	}

	//TODO: I should instead reuse the message Object, instead of creating new ones...
	def mergeCRDT(ccrdt: ComputationalCRDT, partitionKey: Long){
		this.partitions.get(partitionKey).get ! new CRDTSyncTitanMessage(ccrdt, partitionKey);
		/*val reference: String = ccrdt.reference;
		val skeleton: CCRDTSkeleton = ccrdt.skeleton;
		for(i <- 1 to skeleton.partitioningSize){
			this.partitions.get(skeleton.getPartitionKey(i)).get ! new CRDTSyncMessage(ccrdt)
		} */
	}

	def manuallymergeCCRDT(ccrdt: ComputationalCRDT, partitionKey: Long, partitionPlace: Int, partitioningSize: Int){
		this.partitions.get(partitionKey).get ! new ManualCRDTSyncTitanMessage(ccrdt, partitionKey, partitionPlace, partitioningSize)
	}

	//TODO: problem: can create a trigger before the partitions have all been instantiated -> that's a problem! (not now)
	//TODO: this is actually the method after having passed the Trigger to the main (target) CCRDT
	def addTrigger(trigger: Trigger, targetHollowReplica: ComputationalCRDT){
		val source: String = trigger.source;
		val sourceSkeleton: CCRDTSkeleton = this.namedPartitions.get(source).get;
		for(i <- 1 to sourceSkeleton.partitioningSize){
			this.partitions.get(sourceSkeleton.getPartitionKey(i)).get ! new TriggerTitanMessage(trigger, targetHollowReplica)
		}
	}

	def receive = {
		case TriggerTitanMessage(trigger:Trigger, ccrdt: ComputationalCRDT) =>
			this.addTrigger(trigger, ccrdt)
		case TargetedDataTitanMessage(target: String, titanData: TitanData) =>
			this.addData(target, titanData)
		case TargetedDataTitanMessageWithReply(target: String, titanData: TitanData) =>
			this.addData(target, titanData)
		case CRDTCreationTitanMessage(ccrdt: ComputationalCRDT) =>
			this.addCCRDT(ccrdt)
		case CRDTSyncTitanMessage(ccrdt: ComputationalCRDT, partitionKey: Long) =>
			this.mergeCRDT(ccrdt, partitionKey)
		case ManualCRDTSyncTitanMessage(ccrdt: ComputationalCRDT, partitionKey: Long, myPartition: Int, myPartitioningSize: Int) =>
			this.manuallymergeCCRDT(ccrdt, partitionKey, myPartition, myPartitioningSize)
		case EpochSync(str: String, epoch: Int) => {
			val skel: CCRDTSkeleton = this.namedPartitions.get(str).get;
			for(i <- 1 to skel.partitioningSize){
				val partition: Long = skel.getPartitionKey(i)
//				partitions.get(partition).get ! "manualsync"
				partitions.get(partition).get ! new EpochSync(str, epoch)
			}
		}
		case CCRDTDataRequest(target:String, source: String, iteration: Int) => {
			val skel: CCRDTSkeleton = this.namedPartitions.get(target).get;
			for(i <- 1 to skel.partitioningSize){
				val partition: Long = skel.getPartitionKey(i)
				partitions.get(partition).get ! new CCRDTDataRequest(target: String, source: String, iteration)
			}
		}
		case CCRDTDataRequestReply(data: ComputationalCRDT, target: String, iteration: Int, expectedReplies: Int) => {
			val skel: CCRDTSkeleton = this.namedPartitions.get(target).get;
			for(i <- 1 to skel.partitioningSize){
				val partition: Long = skel.getPartitionKey(i)
				partitions.get(partition).get ! new CCRDTDataRequestReply(data, target, iteration, expectedReplies)
			}
		}
		case IterationCheckSyncTitanMessage(target: String, iterationStep: Int, stop: Boolean) => {
			val skel: CCRDTSkeleton = this.namedPartitions.get(target).get;
			val partition: Long = skel.getPartitionKey(1)
			partitions.get(partition).get ! new IterationCheckSyncTitanMessage(target, iterationStep, stop)
		}
		case _ => println("Titan: unkown message received")
	}
}
