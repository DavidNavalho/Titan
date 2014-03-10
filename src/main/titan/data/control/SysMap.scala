package main.titan.data.control

import scala.collection.mutable.{ListBuffer, HashMap}
import akka.actor.{ActorRef, Actor}
import main.titan.data.ccrdt.{CCRDTSkeleton, ComputationalCRDT}
import main.titan.data.messaging.Messaging._
import main.titan.data.messaging.TitanData
import main.titan.computation.Trigger
import main.hacks.data.ccrdts.{MapCCRDT, ORSetCCRDT}
import main.titan.data.messaging.Messaging.DataTitanMessage
import main.titan.data.messaging.Messaging.ManualCRDTSyncTitanMessage
import main.titan.data.messaging.Messaging.CRDTTitanMessage
import main.titan.data.messaging.Messaging.CRDTSyncTitanMessage
import main.titan.comm.CCRDTRef


/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 18/11/13
 * Time: 15:02
 * To change this template use File | Settings | File Templates.
 */
//TODO: interface for increasing parallelism (Actor model processes 1 message at a time) -> Executors, thread pool
class SysMap(ccrdt: ComputationalCRDT, titanRefs: CCRDTRef, father: Int, maxSize: Int) extends Actor{

	import context.dispatcher
	import scala.concurrent.duration._

//	var triggers: HashMap[String, Trigger] = new HashMap[String, Trigger];
	var trigger: Trigger = null;
	var partitions: HashMap[Long, ComputationalCRDT] = new HashMap[Long, ComputationalCRDT]();
	val reference: String = ccrdt.reference;
	val skeleton: CCRDTSkeleton = ccrdt.skeleton;
	val titan: CCRDTRef = titanRefs
	val myFather: Int = father;
	val myFathersMaxSize: Int = maxSize;

	//TODO: no check for repeated keys....
	for(i <- 1 to this.skeleton.partitioningSize)
		this.partitions.put(this.skeleton.getPartitionKey(i), ccrdt.hollowReplica);
	//
	def addData(data: TitanData) {
		//first, process the data using the trigger compute function
		val pd: ListBuffer[TitanData] = this.trigger.compute(data);
		pd.foreach{processedData: TitanData =>
			val key: Long = this.skeleton.hashingFunction(processedData.key)
			val titanData: TitanData = this.partitions.get(key).get.addData(processedData);
		}
//		println(this.partitions.get(key).get.size())
//		println("Added data: "+titanData)
	}


	def merge(data: ListBuffer[TitanData]){
		println("merging on hollow replicas: "+data.size)
		data.foreach{ td: TitanData =>
			this.addData(td)
		}
	}

	//TODO: would be more efficient if the skeleton had the method to create a hollow CCRDT
	def sync{
		for(i <- 1 to this.skeleton.partitioningSize){
			val key: Long = this.skeleton.getPartitionKey(i)
			val ccrdt: ComputationalCRDT = this.partitions.get(key).get
			this.partitions.put(key,ccrdt.hollowReplica);
			titan.message(new CRDTSyncTitanMessage(ccrdt, key), key)
//			println("Sent: "+ccrdt.size()+"| Remained: "+this.partitions.get(key).get.size())
			//send the ccrdt to titan, so it can be synced - let's make it local for now
		}
	}

	//TODO: lots of messages going on here...too much?!? -> problem here: basically, i have a sysmap connected to the local TitanNode - check how i create sysmaps, and provide the map on construction instead
	def manualSync(epochData: EOEData){
		for(i <- 1 to this.skeleton.partitioningSize){
			val key: Long = this.skeleton.getPartitionKey(i)
			val ccrdt: ComputationalCRDT = this.partitions.get(key).get
//			println(ccrdt.size())
			this.partitions.put(key,ccrdt.hollowReplica);
			titan.message(new ManualCRDTSyncTitanMessage(ccrdt, key, this.myFather, this.myFathersMaxSize), key)
//						println("Sent: "+ccrdt.size()+"| Remained: "+this.partitions.get(key).get.size())
			//send the ccrdt to titan, so it can be synced - let's make it local for now
		}
	}

	var timing: Long = System.nanoTime();
	var test: Int = 0;
	def receive = {
		//TODO: scheduler only after running??
		//TODO: only using one Trigger for now
		case t: Trigger => {
			println("Sysmap "+this.reference+" received a Trigger: "+t.source+"->"+t.target);
			if(this.trigger!=null)
				println("Sysmap received multiple Triggers!")
//			this.triggers.put(trigger.getID(), trigger);
			this.trigger = t;
			if(this.trigger.triggerType.equalsIgnoreCase("periodic"))
				context.system.scheduler.schedule(new FiniteDuration(trigger.time, MILLISECONDS), new FiniteDuration(trigger.time, MILLISECONDS), self, "sync");
			else if(this.trigger.triggerType.equalsIgnoreCase("activated")){
				println("Trigger one-time activation received")
//				this.manualSync
			}
			if(this.trigger.triggerType.equalsIgnoreCase("fullIteration")){
				println("##Sysmap for "+this.skeleton.reference+" received iteration trigger")
			}
			/*val tick = context.system.scheduler.scheduleOnce(new FiniteDuration(trigger.time, MILLISECONDS), self, "foo")
			val newTime: Long = System.nanoTime();
			println("Time: "+(newTime-timing)/1000)
			timing = newTime;*/
		}
		case DataTitanMessage(data: TitanData) => {
			this.addData(data);
		}
		case CRDTTitanMessage(data: ListBuffer[TitanData]) => {
			this.merge(data);
		}
		case "sync" => {
//			localReplicas.foreach{actor: ActorRef =>
			this.synccon
			/*val sizeTest: Int = this.partitions.get(this.skeleton.getPartitionKey(1)).get.asInstanceOf[MapCCRDT].orMap.size()
			if(sizeTest>100){
				for(i <- 1 to skeleton.partitioningSize){
					val ccrdt: ComputationalCRDT = this.partitions.get(this.skeleton.getPartitionKey(i)).get
					val size: Int = ccrdt.size()
					println(i+":"+size)
				}
			}*/
			/*val newTime: Long = System.nanoTime();
			println("foo: "+(newTime-timing)/1000)
			timing = newTime*/
			//context.system.scheduler.scheduleOnce(new FiniteDuration(200, MILLISECONDS), self, "foo")
		}
		//TODO: deprecating...
		case "manualSync" => {
			println("#DEPRECATE THIS")
//			this.manualSync
		}
		case EpochSync(source: String, epoch: Int) => {
			println("Sysmap# epoch sync request!")
			this.manualSync(new EOEData(0,epoch))
		}
		case _ => println(this.toString+": Unkown message received")
	}
}
