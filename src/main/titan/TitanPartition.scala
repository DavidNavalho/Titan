package main.titan

import akka.actor.{Props, ActorRef, ActorSystem, Actor}
import main.titan.data.ccrdt.{CCRDTSkeleton, ComputationalCRDT}
import scala.collection.mutable.{ListBuffer, HashMap}
import scala.util.Random
import main.titan.data.control.{SysMap}
import main.titan.data.messaging.Messaging._
import main.titan.data.messaging.TitanData
import main.titan.computation.Trigger
import main.titan.data.messaging.Messaging.DataTitanMessage
import main.titan.data.messaging.Messaging.CRDTTitanMessage
import main.titan.data.messaging.Messaging.CRDTSyncTitanMessage
import main.titan.data.messaging.Messaging.TriggerTitanMessage
import main.hacks.data.triggers.CheckTrigger
import main.hacks.data.ccrdts.{Links, ScratchpadRanks}


/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 18/11/13
 * Time: 14:47
 * To change this template use File | Settings | File Templates.
 */
//Each hollowReplica actually contains it's own Trigger
//Since this is created by the trigger, it makes sense it runs inside
//TODO: receive network messages from nodes in the system (instead of going through Titan?)
//TODO: I may be overusing the actor system...?
class TitanPartition(ccrdt: ComputationalCRDT, titanRef: ActorRef, partitionPlace: Int, partitionsSize: Int) extends Actor{

	val partition: ComputationalCRDT = ccrdt;
	//TODO: shouldn't really be random...
	val partitionActorSystem: ActorSystem = ActorSystem("P-"+this.partition.skeleton.reference+Random.nextInt());
	val localReplicas: ListBuffer[ActorRef] =new ListBuffer[ActorRef]();
	println("New CCRDT Partition created: "+ccrdt.reference)
	val titan: ActorRef = titanRef;
	val place: Int = partitionPlace;
	val placeMax: Int = partitionsSize;

	var localTrigger: Trigger = null

	var hack: Array[Int] = null;

	//TODO: equivalent of the add Data method
	def merge(computationalCRDT: ComputationalCRDT){
		val returned: ListBuffer[TitanData] = this.ccrdt.merge(computationalCRDT);
		if(computationalCRDT!=null)
			println(computationalCRDT.size()+"->"+this.ccrdt.size())
//		println("Partition "+this.partition.reference+" merged. ")
		if(this.localTrigger!= null && this.localTrigger.triggerType.equalsIgnoreCase("fullIteration")){
//			println(this.ccrdt.reference+"## ASBJKBDSKKJADS")     //TODO ???
		}else
			this.localReplicas.foreach{actor: ActorRef =>
				actor ! new CRDTTitanMessage(returned)
	//			actor ! "manualSync"//TODO: removed sync here
			}
	}

	var lastIteration: Int = 0


	//TODO: make it abstract, based on trigger, etc
	//send a message to every(?) links partition, requesting their data so that it can be used with the join
	def messagePartitions(target: String){
		this.lastIteration+=1
		println("Starting iteration: "+this.lastIteration)
		titan ! new CCRDTDataRequest(target, this.ccrdt.reference, this.lastIteration)
	}

	//this one runs on all partitions
	//TODO: assuming a join for now...
	//: 9. Ranks runs the computation expressed by the Trigger: a join, based on the Key, of Links (remote ccrdt) and the last (complete) local version of ranks
	def computeIteration{
//		println("computing iteration: "+this.it_Counter)
		this.localTrigger.asInstanceOf[CheckTrigger].leftJoin(this.ccrdt.asInstanceOf[ScratchpadRanks], this.remote_CCRDT.asInstanceOf[Links])
//		println("Iteration step "+this.it_Counter+" complete")
		compute
	}

	def compute{
		if(this.localTrigger==null)
			return
		//else, it has a local Trigger, and should act on it
		//TODO: must check that it can indeed run!
		//assuming I am on ranks, after the init links -> ranks, I should get here by means of a ManualCRDTSyncMessage (right???)
		//take into account I have two partitions: if one ends, it should start the checkup part of the code
		//which means I need to sync on a wait for every partition, on the main partition
		//TODO: require: knowledge of #partitions -> I can get this from the skeleton data
		//I just completed the first ranks and got their value -
		//Now I need to check conditions: activate the Trigger and it's computation
		//TODO: 1. check the trigger - ignoring for now

		//TODO: 1.1 if it's the first iteration (0), assume it will be false anyway, and simply assume false [skip to _5_]
		if(lastIteration==0){//assume false
			println("First iteration, starting join")//TODO skip forward to [_5_]
			if(this.partitionPlace==1)//the first partition makes the decisions
				this.messagePartitions("links")
		}
		//2. run it's intended computation (returns tuples with true/false)
		else{//not the first iteration, run check Condition
			val result = this.localTrigger.asInstanceOf[CheckTrigger].check(this.ccrdt.asInstanceOf[ScratchpadRanks])
//			println(">Checking iteration condition: Iteration #"+this.lastIteration+", returned: "+result)
			if(result){
				println("Iteration verification suggesting to stop on partition: "+this.partitionPlace+", iteration: "+this.lastIteration)
				//I should now sync this to the first, which is the one that actually decides what to do
				titan ! new IterationCheckSyncTitanMessage(this.ccrdt.reference, this.it_Counter, true)
			}else
				titan ! new IterationCheckSyncTitanMessage(this.ccrdt.reference, this.it_Counter, false)
			return
		}
		//TODO: 3. the results from the compute function should be gathered onto a local true/false gatherer
		//TODO 3.0 OPTIMIZATION FOR TESTING: as soon as a false comes, stop compute, and start new computation!
		//TODO: 3.1 send the final result (true or false) to the first partition of this partition set
		//TODO: 4. the first partition will wait until every partition (itself included) have messaged with the result (and it's current iteration step)
		//TODO: 4.1 IF the result is negative (don't stop), then it should run the main_compute function of the IterationTrigger
		//TODO: 4.2 which, in this case, will be a join between the local data and a retrieval of external (links) data:
		// 5. send a message to every(?) links partition, so that it can be used with the join

		//TODO 5.0.1 OPTIMIZATION: if it's using the same hashing function, I can request only for the partitions I need
		//TODO 5.0.1.1 optimization: OR, I can request for the keys I require - this seems more complex, and more to the future...
		//TODO 5.0.2 OPTIMIZATION: [keyword?] since links never change, I should only really ask for the links once, and reuse them
		//6. links get the request message, and send their CompleteCCRDT to Ranks, with the iteration they belong to
		//7. Ranks receives the RequestedCCRDTMessage with the iteration number
		//8. Ranks checks if it has received all the expected data (in this case, all the links ccrdts partitions messages)
		//9. Ranks runs the computation expressed by the Trigger: a join, based on the Key, of Links (remote ccrdt) and the last (complete) local version of ranks
		//10. Ranks runs the main_iteration_compute function available, that takes: [link, [[links], rank]] as input (from the join function)
		//10.1 the compute function gives back a list of (TitanData)[link, Double] which is added to the next version of Ranks
		//TODO: 11. After every tuple has been computed (added to ranks), run the check operations again -> step 2
		//TODO 11.1 have to be careful with the iteration increases, and making sure the scratchpad is correctly updated


	}

	var it_Counter: Int = 0
	var it_maxSize: Int = 0
	var it_size: Int = 0
	var remote_CCRDT: ComputationalCRDT = null

	//Receive [internal] messages from Titan Node
 	def receive = {
		//TODO: multiple SysMap constructors?
		case TriggerTitanMessage(trigger: Trigger, ccrdt: ComputationalCRDT) => {
				println("["+this.partition.reference+"] New Trigger: "+trigger.toString())

			//if it's an iteration Trigger, the trigger actually runs in the partition, and not on the sysmaps (they remain empty)
			if(trigger.triggerType.equalsIgnoreCase("fullIteration")){
//				println("#Partition "+this.ccrdt.reference+"# received an iteration Trigger")
				this.localTrigger = trigger
			}
			else{
				val newActor: ActorRef = partitionActorSystem.actorOf(Props[SysMap](new SysMap(ccrdt.hollowReplica, this.titan, this.place, this.placeMax)))
				this.localReplicas+=newActor;
				newActor ! trigger;
			}
//			if(trigger.triggerType.equalsIgnoreCase("activated"))
//				this.merge(null)
		}
		case DataTitanMessage(titanData: TitanData) => {
			val data: TitanData = this.partition.addData(titanData)
//			println(this.partition.size())
//			println("blah: "+localReplicas.size)
			//add it to the localReplica(s) -> this is based on the Trigger, the sysmap handles it
			//TODO: but now triggers can be on the actual partition too, so I need to handle it here
			if(this.localTrigger!= null && this.localTrigger.triggerType.equalsIgnoreCase("fullIteration")){

			}else
				this.localReplicas.foreach{actor: ActorRef =>
					actor ! new DataTitanMessage(data)
				}
		}
		//TODO: activate triggers
		/*case CRDTDataMessage(ccrdt: ComputationalCRDT) => {
			println("Received CCRDT for merge procedure!")
		}*/
		case CRDTSyncTitanMessage(ccrdt: ComputationalCRDT, key: Long) => this.merge(ccrdt)//TODO: check: I should be able to ignore the key here - double-check??
		case ManualCRDTSyncTitanMessage(ccrdt: ComputationalCRDT, key: Long, myPartition: Int, myPartitioningSize: Int) => {
			this.merge(ccrdt)
			if(this.hack==null)
				this.hack = new Array[Int](myPartitioningSize)
//			if(this.hack.length<2)
//				println(this.ccrdt.reference+"# ->->-> "+"length below 2!")  //links
			println(this.ccrdt.reference+"# ->->-> "+myPartitioningSize+"")      //links is increasing to 2 (because I'm sending the ranks!)
			this.hack(myPartition-1) = 1;
			if(this.hack.sum==this.hack.length){
//				println("TitanPartition#"+this.partition.skeleton.reference+" all local replicas merged")
				//then sync sysmaps
				//TODO: would be much better if it synced what it could, so it would just be a simple signal to start the next step
				this.localReplicas.foreach{actor: ActorRef =>
					actor ! new EpochSync(this.ccrdt.skeleton.reference, 0)//TODO: need to get Epoch reference...
				}
				this.compute
			}
			/*var hackish: String = ""
			this.hack.foreach{i: Int =>
				hackish+=i+""
			}
			println("hack: "+hackish+" | "+hack.sum)*/
		}
			//TODO: deprecating...
		case "manualsync" => {
			println("TITANPARTITION# DEPRECATE THIS")
			this.localReplicas.foreach{actor: ActorRef =>
				actor ! "manualSync"
			}
		}
		case EpochSync(source: String, epoch: Int) => {
			this.localReplicas.foreach{actor: ActorRef =>
				actor ! new EpochSync(source, epoch)
			}

		}
		case CCRDTDataRequest(target: String, source: String, iteration: Int) => {
//			println("> Partition "+this.ccrdt.reference+"("+this.partitionPlace+") received request for data from <"+source+">")
			titan ! new CCRDTDataRequestReply(this.ccrdt, source, iteration: Int, this.ccrdt.partitioningSize)
		}
		case CCRDTDataRequestReply(data: ComputationalCRDT, target: String, iteration: Int, expectedReplies: Int) => {
			if(this.it_Counter<iteration){
				this.it_Counter = iteration
				this.it_maxSize = expectedReplies
				this.it_size = 1
				this.remote_CCRDT = data.hollowReplica
			}else if(this.it_Counter==iteration){
				this.it_size += 1
			}else if(this.it_Counter>iteration) println("> > Received an old iteration request???")
			this.remote_CCRDT.merge(data)
//			println("> Partition "+this.ccrdt.reference+"("+this.partitionPlace+") received data request reply with data size <"+data.size()+">, total data size <"+this.remote_CCRDT.size()+">")
			if(this.it_size==this.it_maxSize){
//				println("doing next computation: "+iteration)
				this.lastIteration = iteration
				this.computeIteration
			}
		}
		case IterationCheckSyncTitanMessage(target: String, iterationStep: Int, stop: Boolean) => {
			if(iterationStep>check_iteration){
				this.check_iteration = iterationStep
				this.check_results = 1
				if(stop)
					this.check_confirmed = 1
				else
					this.check_confirmed = 0
			}else if(iterationStep==check_iteration){
				this.check_results += 1
				if(stop)
					this.check_confirmed +=1
			}else println("Received an old iteration check message???")
			if(this.check_results == this.partitionsSize){
				//then I have received all the messages and should make a decision
				if(this.partitionsSize == this.check_confirmed){
					println("Iteration stopped @ step: "+iterationStep)
				}else{
					this.messagePartitions("links")//start the next iteration!
				}
			}
		}
	  case _ => {
			println(this.toString+": Unkown message received")
		}
  }
	var check_iteration: Int = 0
	var check_confirmed: Int = 0
	var check_results: Int = 0
}
