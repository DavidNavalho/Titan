package examples.tests

import main.titan.data.control.{SysMap}
import akka.actor._
import akka.pattern.ask
import main.titan.{TitanNode, TitanActor}
import main.titan.data.ccrdt.{CCRDTSkeleton, ComputationalCRDT}
import main.hacks.data.ccrdts.{ScratchpadRanks, ORSetCCRDT, Ranks, Links}
import scala.io.Source
import main.titan.data.messaging.Messaging._
import main.titan.computation.{ManualTrigger, Trigger}
import main.titan.data.messaging.TitanData
import main.titan.data.messaging.Messaging.EOE
import main.titan.data.messaging.Messaging.CRDTCreationTitanMessage
import main.titan.data.messaging.Messaging.TriggerTitanMessage
import main.titan.data.messaging.Messaging.TargetedDataTitanMessage
import scala.concurrent.Await
import akka.util.Timeout
import scala.concurrent.duration._
import main.titan.data.messaging.Messaging.EOE
import main.titan.data.messaging.Messaging.CRDTCreationTitanMessage
import main.titan.data.messaging.Messaging.TriggerTitanMessage
import main.titan.data.messaging.Messaging.TargetedDataTitanMessageWithReply
import main.titan.data.messaging.Messaging.TargetedDataTitanMessage
import main.hacks.data.triggers.{CheckTrigger, LinksRanksTrigger, ReaderLinksTrigger}
import sys.dht.api.DHT
import sys.Sys
import sys.dht.catadupa.KryoCatadupa
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.ClusterDomainEvent
import main.titan.data.messaging.catadupa.{MiniTitan, CatadupaReply, CatadupaKey, CatadupaRequest}
import main.java.TitanHandler
import main.java.TitanHandler.TitanReplyHandler
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.HashMap
import main.titan.data.messaging.Messaging.TargetedDataTitanMessage
import main.titan.data.messaging.Messaging.EpochSync
import main.titan.data.messaging.Messaging.CRDTCreationTitanMessage
import main.titan.data.messaging.Messaging.TargetedDataTitanMessageWithReply
import main.titan.data.messaging.Messaging.TriggerTitanMessage

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 18/11/13
 * Time: 14:16
 * To change this template use File | Settings | File Templates.
 */
object Tests {

	def testSysmap(){
		//define a trigger
		val myTrigger: Trigger = new Trigger("first", "all", "second", 100)
		//setup a SysMap thread
		val actorSystem: ActorSystem = ActorSystem("SysMapTesting")
		val sysmapRef: ActorRef = actorSystem.actorOf(Props[SysMap](new SysMap(null, null, 0, 0)))
		sysmapRef ! myTrigger
		/*for(i <- 0 to 1000000)
			sysmapRef ! "inc"*/
	}

	def testTitan(){
		val titan: TitanActor = new TitanActor();
		//create a CCRDT
		val links: Links = new Links("links", 2)
		titan.addCCRDT(links)
		val links2: Links = new Links("links2", 2)
		titan.addCCRDT(links2)
		val myTrigger: Trigger = new Trigger("links", "all", "links2", 100)
		titan.addTrigger(myTrigger, links2.hollowReplica)
	}

	def readStuffOnto(titan: ActorRef, target: String){
		for(line <- Source.fromFile("pagerank_data.txt").getLines()) {
			val words: Array[String] = line.split(' ');
			if(words.length!=2)
				println("Read something weird...")
			titan ! new TargetedDataTitanMessage(target, new TitanData(words(0),words(1)))
			/*words.foreach{ w =>
				titan ! new TargetedDataMessage(target, new TitanData(w, w));
			}*/
		}
		//send EOEpoch message
//		titan ! new EOE("reader",new EOEData(0,0))
		println("done reading links")
	}

  def findNode(partitionKey: Long, mt: ActorRef): ActorSelection = {
    val key: CatadupaKey = new CatadupaKey(partitionKey)
    implicit val timeout = Timeout(30 seconds)
    val future = mt ? key
    val titan: ActorSelection = Await.result(future, 1 minute).asInstanceOf[ActorSelection]
    return titan
  }

	def readStuffWithCheckOnto(titan: ActorSelection, target: String, reader: ComputationalCRDT, mt: ActorRef){
    val pointers: HashMap[Long, ActorSelection] = new HashMap[Long, ActorSelection]();
    for(i <- 1 to reader.partitioningSize){
      val key: Long = reader.skeleton.getPartitionKey(i)
      pointers.put(key, findNode(key, mt))
    }
		println("Time: "+System.nanoTime())
		for(line <- Source.fromFile("pagerank_data.txt").getLines()) {
			val words: Array[String] = line.split(' ');
			if(words.length!=2)
				println("Read something weird...")
//			implicit val timeout = Timeout(1 minutes)
			/*val future = */
      val data: TitanData = new TitanData(words(0),words(1))
      pointers.get(reader.skeleton.hashingFunction(data.key)).get ! new TargetedDataTitanMessageWithReply(target, data);
//			val result: String = Await.result(future.mapTo[String], 1 minute)
//			println(result)
			/*words.foreach{ w =>
				titan ! new TargetedDataMessage(target, new TitanData(w, w));
			}*/
		}
	}

/*	def iteration(links: Links){
		//Init: create empty Rank
		val ranks: Ranks = new Ranks("ranks",1);
		//using: links
	}*/

	def testPageRank(){
		//start Titan
		val actorSystem: ActorSystem = ActorSystem("Titan")
		val titan: ActorRef = actorSystem.actorOf(Props[TitanActor](new TitanActor))

//		val titan: Titan = new Titan();
		//create/add a new CCRDT
		val reader: ComputationalCRDT = new ORSetCCRDT("reader",1);
		titan ! new CRDTCreationTitanMessage(reader);
		//create/add the LinksSet
		val links: Links = new Links("links",4);
		titan ! CRDTCreationTitanMessage(links)
		//create Trigger:
		//basically, I need: source; compute Function (that receives sources Tuples, and transforms); target
		val firstTrigger: Trigger = new Trigger("reader", "all", "links",100);
		titan ! TriggerTitanMessage(firstTrigger, links.hollowReplica)

		//only start adding data after everything is created
		//populate reader
//		Thread.sleep(500)
		readStuffOnto(titan, "reader");

	}

	def it_init(titan: ActorSelection){
		//initial rankings -> a normal computation
		val ranks: ScratchpadRanks = new ScratchpadRanks("ranks",4,2)
//		this.stub.send(new CatadupaKey("ranks"),new CatadupaMessage(new CRDTCreationTitanMessage(ranks)))
		titan ! CRDTCreationTitanMessage(ranks)
		val secondTrigger: LinksRanksTrigger = new LinksRanksTrigger("links", "all", "ranks")
//		this.stub.send(new CatadupaKey("links"), new CatadupaMessage(new TriggerTitanMessage(secondTrigger, ranks.hollowReplica)))
    println("Breathe again...")
    Thread.sleep(5000)
    println("GULP!")
		titan ! TriggerTitanMessage(secondTrigger, ranks.hollowReplica)
	}

	def condition: Boolean = {
		return false
	}

	def it_iteration(titan: ActorSelection, links: Links){
		val source: String = "links"
		val source2: String = "ranks"
		//since I'm using ranks to compute new ranks, I want to get 'hollow' (full) replicas onto the scratchpad
		//and run the computation/Trigger there
		val iterationTrigger: CheckTrigger = new CheckTrigger("ranks","links","key-join","ranks",5)
//		this.stub.send(new CatadupaKey("ranks"), new CatadupaMessage(new TriggerTitanMessage(iterationTrigger, links.hollowReplica)))
		titan ! TriggerTitanMessage(iterationTrigger, links.hollowReplica)     //null hollow replica?
	}

	def iteration(titan: ActorSelection, links: Links){
		it_init(titan)
		it_iteration(titan, links)
	}


	sys.Sys.init()
//	new TitanNode
	var stub: DHT = Sys.Sys.getDHT_ClientStub()



	def testPhasedPageRank(titan: ActorSelection, mt: ActorRef){
//		import com.typesafe.config.ConfigFactory
//		System.setProperty("akka.remote.netty.tcp.port", 2551+"")
		/*val actorSystem = ActorSystem("Titan")//, ConfigFactory.load("TitanNode"))
//		val actorSystem: ActorSystem = ActorSystem("Titan")
		val titan: ActorRef = actorSystem.actorOf(Props[TitanActor], "titanCluster")//(new TitanActor()))*/
//		Cluster(actorSystem).subscribe(titan, classOf[ClusterDomainEvent])


//		val mt = new MiniTitan
//		val titan: ActorSelection = mt.getReference(new CatadupaKey(1))
		val reader: ComputationalCRDT = new ORSetCCRDT("reader",4);
//		this.stub.send(new CatadupaKey("reader"), new CatadupaMessage(new CRDTCreationTitanMessage(reader)))
		titan ! new CRDTCreationTitanMessage(reader);



		//setup the rest of the system nodes
		val links: Links = new Links("links",4);
//		this.stub.send(new CatadupaKey("links"), new CatadupaMessage(new CRDTCreationTitanMessage(links)))
    titan ! new CRDTCreationTitanMessage(links)
    println("breathe...")
    Thread.sleep(5000)
    println("GULP")

    //setup the triggers
    val firstTrigger: ReaderLinksTrigger = new ReaderLinksTrigger("reader", "all", "links");
//		this.stub.send(new CatadupaKey("reader"), new CatadupaMessage(new TriggerTitanMessage(firstTrigger, links.hollowReplica)))
    titan ! TriggerTitanMessage(firstTrigger, links.hollowReplica)
    //send signal to start computation?
    //a partition needs knowledge about how many partitions it should get data from.

    iteration(titan, links)
    println("Breathe YET again...")
    Thread.sleep(5000)
    println("GULP!")
    readStuffWithCheckOnto(titan, "reader", reader, mt)
    println("file successfully read to system", System.nanoTime());
    println("Time after read: "+System.nanoTime())
    titan ! new EpochSync("reader", 0)

	}

	def testKeyHashing(){
		val skel: CCRDTSkeleton = new CCRDTSkeleton("A",4)
		for(i <- 1 to 10){
			val key: Long = skel.hashingFunction(i+"")
			val partition: Long = skel.getPartitionKey(i);
			println(key+"->"+partition)
		}
	}

	def testCRDTS(){
		val orSet: ORSetCCRDT = new ORSetCCRDT("name",1)
		orSet.addData(new TitanData("a","a"))
		orSet.addData(new TitanData("a","b"))
		orSet.addData(new TitanData("a","d"))
		orSet.addData(new TitanData("a","a"))
		println(orSet.toString)
	}

	def main(args: Array[String]){
		//testSysmap()
		//testTitan()
//		testPageRank()
//		println("Full Start time: "+System.nanoTime())
//		  testPhasedPageRank()
//		testKeyHashing()
//		testCRDTS
//
////		val mt = new MiniTitan
////		mt.TitanOnline
////		Thread.sleep(5000)                 \\\\\\\\\\\\\\
//		//val titanString: ActorSelection =
//		val reader: ComputationalCRDT = new ORSetCCRDT("reader",1);
//		val msg: TitanMessage = new CRDTCreationTitanMessage(reader);
		val key: CatadupaKey = new CatadupaKey(0)

		val cf = ConfigFactory.load("MiniTitan")
		val actorSystem: ActorSystem = ActorSystem("TitanNode",cf)
		val mt: ActorRef = actorSystem.actorOf(Props[MiniTitan](new MiniTitan))
		implicit val timeout = Timeout(30 seconds)
		val future = mt ? (key)
		val titan: ActorSelection = Await.result(future, 1 minute).asInstanceOf[ActorSelection]
//		titan ! new CRDTCreationTitanMessage(reader)
		testPhasedPageRank(titan, mt)
		//send this reader, through catadupa
//		titan ! new CRDTCreationTitanMessage(reader);
//		testPhasedPageRank(titan)

//		println(result.toString)
//		println("Received a titanRef!: "+titan.toString())
	}



	/*
	Computation Objective:
		read links pairs from a file
			onto a SetCCRDT(1)
		create a LinksSet with no duplicates (link, list of links) (2)
			from (1)
		Init:
			create an initial Ranks Set (3)       (Scratchpad)
        from (2)
		Iteration:
			for each step:
				join links+ranks -> join((2),(3)) -> tuple: (link, rank, list of links)
					doComputationStep (returns several sum() tuples)
				each tuple added to a new RanksSet (Scratchpad)
		Stop:
			For every (optimization: changed/new) Rank tuple:
				compare with previous version (on Scratchpad)
				return true/false
			For now: assume that, if all true, then computation stops





	 */
}
