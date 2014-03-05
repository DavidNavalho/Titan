package main.titan

import main.titan.data.ccrdt.{CCRDTSkeleton, ComputationalCRDT}
import akka.actor.{ActorSelection, Props, ActorRef, ActorSystem, Actor}
import scala.collection.mutable.{ListBuffer, HashMap}
import main.titan.data.messaging.Messaging._
import main.titan.data.messaging.TitanData
import main.titan.computation.Trigger
import main.titan.data.messaging.Messaging.DataTitanMessage
import main.titan.data.messaging.Messaging.TriggerTitanMessage
import main.titan.data.messaging.Messaging.CRDTCreationTitanMessage
import main.titan.data.messaging.Messaging.TargetedDataTitanMessage
import akka.cluster.ClusterEvent.{MemberRemoved, UnreachableMember, MemberUp, CurrentClusterState}
import com.typesafe.config.ConfigFactory
import main.titan.data.messaging.catadupa.{MiniTitan, CatadupaKey}
import akka.util.Timeout
import scala.concurrent.duration._
import akka.pattern.ask
import main.titan.data.messaging.Messaging.TargetedDataTitanMessage
import main.titan.data.messaging.Messaging.CCRDTDataRequest
import main.titan.data.messaging.Messaging.CRDTSyncTitanMessage
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import main.titan.data.messaging.Messaging.ManualCRDTSyncTitanMessage
import main.titan.data.messaging.Messaging.DataTitanMessage
import main.titan.data.messaging.Messaging.EpochSync
import main.titan.data.messaging.Messaging.CCRDTDataRequestReply
import akka.cluster.ClusterEvent.CurrentClusterState
import main.titan.data.messaging.Messaging.CRDTCreationTitanMessage
import main.titan.data.messaging.Messaging.TargetedDataTitanMessageWithReply
import main.titan.data.messaging.Messaging.IterationCheckSyncTitanMessage
import akka.cluster.ClusterEvent.UnreachableMember
import main.titan.data.messaging.Messaging.TriggerTitanMessage
import scala.concurrent.Await

//required for '?'

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 14/11/13
 * Time: 15:43
 * To change this template use File | Settings | File Templates.
 */
//TODO: when a Trigger is added, check for existing data: if there is any, it should be retrieved and processed (depending on configuration/keywords...)
//TODO: do not return until a partition/Trigger is actually fully installed (e.g, use '?' in actors)
class TitanActor extends Actor{
	val partitions: HashMap[Long, ActorRef] = new HashMap[Long, ActorRef]();
	val cf = ConfigFactory.load("app")
	val titanActorSystem: ActorSystem = ActorSystem("TitanMessaging",cf)
	val namedPartitions: HashMap[String, CCRDTSkeleton] = new HashMap[String, CCRDTSkeleton]();

	//set this node up as a DHT Node

  /**
   * MiniTitan Controller for finding other DHT Nodes
   * */

  //val key: CatadupaKey = new CatadupaKey(0)

  val mtCF = ConfigFactory.load("TitanActor")
  val actorSystem: ActorSystem = ActorSystem("TitanNode",mtCF)
  val mt: ActorRef = actorSystem.actorOf(Props[MiniTitan](new MiniTitan))
  implicit val timeout = Timeout(30 seconds)
  //val future = mt ? (key)

	//TODO: Communication protocol

  def findNode(partitionKey: Long): ActorSelection = {
    val key: CatadupaKey = new CatadupaKey(partitionKey)
    val future = mt ? key
    val titan: ActorSelection = Await.result(future, 1 minute).asInstanceOf[ActorSelection]
    return titan
  }

  //TODO: I'm not waiting for this result currently - mistake!
  def createPartition(ccrdt: ComputationalCRDT, partition: Int, size: Int, partitionKey: Long){
    val skelleton: CCRDTSkeleton = ccrdt.skeleton
    if(!this.namedPartitions.contains(skelleton.reference))
      this.namedPartitions.put(skelleton.reference, skelleton)
    else
      println("Skeletton already existed, ignoring")
    val partitionActor: ActorRef = titanActorSystem.actorOf(Props[TitanPartition](new TitanPartition(ccrdt.hollowReplica, self, partition, size)))
    if(this.partitions.contains(partitionKey)){
      println("Titan: partitionKey already existed!!!")
      return
    }
    this.partitions.put(partitionKey, partitionActor);
    println("Partition ["+partition+"] "+skelleton.reference+" created.")
  }
	//TODO: check for repeated keys
  //TODO: WHat happens with multiple partitions? How am I sending messages? -> estou a dar self como referência - pode ser, mas isto tem de ser 'distribuido', com base na chave de cada partição
	def addCCRDT(ccrdt: ComputationalCRDT){
		val skel: CCRDTSkeleton = ccrdt.skeleton
		this.namedPartitions.put(skel.reference, skel);
		for(i <- 1 to skel.partitioningSize){
      println("Requesting to create partition ["+i+"] from: "+skel.reference)
      val key: Long = skel.getPartitionKey(i)
      val node: ActorSelection = findNode(key)
      println("node found, sending message")
      node ! new RemoteCreateCRDT(ccrdt, i, skel.partitioningSize, key)
//      node ! new CRDTCreationTitanMessage(ccrdt);

			/*val partitionKey: Long = skel.getPartitionKey(i);
			val partitionActor: ActorRef = titanActorSystem.actorOf(Props[TitanPartition](new TitanPartition(ccrdt.hollowReplica, self, i, skel.partitioningSize)))
			if(this.partitions.contains(partitionKey)){
				println("Titan: partitionKey already exists!!!")
				return
			}
			this.partitions.put(partitionKey, partitionActor);*/
		}
    /*val systemActors = this.partitions.get(partitionKey);
			var list: ListBuffer[ActorRef] = null;
			if(!systemActors.isDefined){
				list = new ListBuffer[ActorRef]();
				this.partitions.put(partitionKey, list)
			}else
				list = systemActors.get;*/
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

  def createTrigger(trigger: Trigger, targetHollowReplica: ComputationalCRDT, key: Long){
    this.partitions.get(key).get ! new TriggerTitanMessage(trigger, targetHollowReplica)
  }

	//TODO: problem: can create a trigger before the partitions have all been instantiated -> that's a problem! (not now)
	//TODO: this is actually the method after having passed the Trigger to the main (target) CCRDT
	def addTrigger(trigger: Trigger, targetHollowReplica: ComputationalCRDT){
		val source: String = trigger.source;
		println("#>TRIGGER SOURCE: "+source)
		println("#>TRIGGER "+this.namedPartitions.size)
		println("#>TRIGGER "+this.namedPartitions.contains(trigger.source))
		val sourceSkeleton: CCRDTSkeleton = this.namedPartitions.get(source).get;
		for(i <- 1 to sourceSkeleton.partitioningSize){
      val key: Long = sourceSkeleton.getPartitionKey(i)
      val node: ActorSelection = findNode(key)
      node ! new RemoteCreateTrigger(trigger, targetHollowReplica, key)
//			this.partitions.get(sourceSkeleton.getPartitionKey(i)).get ! new TriggerTitanMessage(trigger, targetHollowReplica)
		}
	}

	def receive = {
    case RemoteCreateTrigger(trigger: Trigger, targetHollowReplica: ComputationalCRDT, key: Long) =>
      this.createTrigger(trigger, targetHollowReplica, key)
    case RemoteCreateCRDT(ccrdt: ComputationalCRDT, partition: Int, size: Int, partitionKey: Long) =>
      this.createPartition(ccrdt, partition, size, partitionKey)
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
			println("received sync")
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

	//#######
		//Cluster management messages
		case state: CurrentClusterState ⇒
			println("Current members: {}", state.members.mkString(", "))
		case MemberUp(member) ⇒
			println("Member is Up: {}", member.address)
		case UnreachableMember(member) ⇒
			println("Member detected as unreachable: {}", member)
		case MemberRemoved(member, previousStatus) ⇒
			println("Member is Removed: {} after {}",
				member.address, previousStatus)

		case _ => println("Titan: unkown message received")
	}
}
