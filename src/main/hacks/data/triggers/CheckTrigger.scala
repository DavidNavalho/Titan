package main.hacks.data.triggers

import main.titan.computation.ManualTrigger
import main.titan.data.messaging.TitanData
import scala.collection.mutable.ListBuffer
import vrs0.crdts.more.CRDTDouble
import main.hacks.data.ccrdts.{Links, Ranks, ScratchpadRanks}
import main.titan.data.ccrdt.ComputationalCRDT
import sys.dht.catadupa.crdts.ORSet

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 27/11/13
 * Time: 11:42
 * To change this template use File | Settings | File Templates.
 */
class CheckTrigger(computationSource1: String, computationSource2: String, operation: String, triggerTarget: String, threshold: Double) extends ManualTrigger(computationSource1, operation, triggerTarget){
//TODO: have to rethink this part - make it work for this example
	this.iteration = 1
	this.triggerType = "fullIteration"
	val minThreshold: Double = threshold

	def this() = this("","","","",0.0)

//	var blah_max: Double = 0

	private def weight(value: Double): Double = {
		return (0.15+0.85*value)
	}

	//expected data: titanData(key, Tuple(CRDTDouble, CRDTDouble))
	override def compute(titanData: TitanData): ListBuffer[TitanData] = {
		val dataSet: ListBuffer[TitanData] = new ListBuffer[TitanData]
		val comparison: Boolean = false
		val versions: (Double, Double) = titanData.data.asInstanceOf[(Double, Double)]
		val diff: Double = math.abs(weight(versions._2)-weight(versions._1))
//		if(diff>blah_max) blah_max = diff
		if(diff<= this.minThreshold)
			dataSet+=new TitanData(titanData.key,true.asInstanceOf[AnyRef])
		else
			dataSet+=new TitanData(titanData.key,false.asInstanceOf[AnyRef])
//		println("First: "+versions._1+" Second: "+versions._2+" min_threshold: "+(math.abs(versions._2-versions._1)>= this.minThreshold))
		return dataSet
	}

	 def check(scratchpad: ScratchpadRanks): Boolean = {
		 val startTime = System.nanoTime()
//		 var falses: Int = 0
//			this.blah_max = 0
//		 println("SCRATCHPAD LENGTH: last->"+scratchpad.getLast.size+", previous->"+scratchpad.getPrevious.size)
	   val current: Ranks = scratchpad.getLast.asInstanceOf[Ranks]
		 val previous: Ranks = scratchpad.getPrevious.asInstanceOf[Ranks]
		 //for each in current, compare with previous
		 val it = current.ranks.getValue.entrySet().iterator()
		 while(it.hasNext){
			 val currentState = it.next()
			 val key: String = currentState.getKey
			 val currentValue: Double = currentState.getValue.iterator().next().value()
			 var previousValue: Double = currentValue
			 if(previous.ranks.lookup(key))
			    previousValue = previous.ranks.get(key).iterator().next().value()
			 if(!this.compute(new TitanData(key,(currentValue, previousValue))).head.data.asInstanceOf[Boolean]){
//				 println("Check Trigger max difference: "+this.blah_max)
				 println("Check completion time: "+((System.nanoTime()-startTime)/1000000))
				 return false
//				 falses+=1
			 }
		 }
//		println("Check Trigger max difference: "+this.blah_max)
//		 return falses
		 println("Check completion time: "+((System.nanoTime()-startTime)/1000000))
		 return true
	}

	//[link(key), [[links], rank](data)]
	def iteration_compute(data: TitanData): ListBuffer[TitanData] = {
		val dataSet: ListBuffer[TitanData] = new ListBuffer[TitanData]
		val tupleData: (ORSet[String], Double) = data.data.asInstanceOf[(ORSet[String], Double)]
		val distributedRank: Double = tupleData._2/tupleData._1.size()
		val it = tupleData._1.iterator()
		while(it.hasNext){
			val next = it.next()
			println("Adding "+distributedRank+" to "+next)
			dataSet+=new TitanData(next, distributedRank.asInstanceOf[AnyRef])
		}
		return dataSet
	}

	var doneBefore: Boolean = false

	def skippedLeftJoin(scratchpad: ScratchpadRanks, links: Links){
		val startTime = System.nanoTime()

		val oldRanks: Ranks = scratchpad.getLast.asInstanceOf[Ranks]
		scratchpad.addNew(oldRanks.hollowReplica)
		val newRanks: Ranks = scratchpad.getLast.asInstanceOf[Ranks]

		/*val it = oldRanks.ranks.

		val it = links.myorMap.getValue.entrySet().iterator()

		while(it.hasNext){
			val link_links = it.next()
			val links_list = link_links.getValue.iterator().next()
			val list_size = links_list.size()
			val links_it = links_list.iterator()
			while(links_it.hasNext){
				val linked_link: String = links_it.next()
				if(oldRanks.ranks.lookup(link_links.getKey)){
					newRanks.addData(new TitanData(linked_link, ((weight((oldRanks.ranks.get(link_links.getKey).iterator().next().value())))/list_size).asInstanceOf[AnyRef]))
				}
			}
		}*/

//		val units = 1000000
		val endTime = System.nanoTime()
		val totalTime = (endTime+startTime)

		println("Skip-Join completion times: "+totalTime)
	}

	//TODO: problem: if a link isn't linked, what's it's score? 1?
	//for each join, run the computation and add it to a ListBuffer of titandata of the format: [link(key), [[links], rank](data)]
	def leftJoin(scratchpad: ScratchpadRanks, links: Links){
		/*if(doneBefore){
			skippedLeftJoin()
			return
		}*/
		val startTime = System.nanoTime()
//		println("Left join with links:\r\n"+links.toString+"...and ranks: \r\n"+scratchpad.getLast.toString)
//		println("LEFTJOIN SCRATCHPAD LENGTH: "+scratchpad.scratchpads.length)
		val oldRanks: Ranks = scratchpad.getLast.asInstanceOf[Ranks]
		scratchpad.addNew(oldRanks.hollowReplica)
//		println("LEFTJOIN SCRATCHPAD LENGTH AFTER ADD: "+scratchpad.scratchpads.length)
		val newRanks: Ranks = scratchpad.getLast.asInstanceOf[Ranks]
//		println("Previous join different: "+current.ranks.getValue.entrySet().size())
		val it = links.myorMap.getValue.entrySet().iterator()

		var int_total: Double = 0
		var int_counter: Int = 0

		var int_total_weight: Double = 0
		var int_counter_weight: Double = 0

		while(it.hasNext){
			val link_links = it.next()
			val links_list = link_links.getValue.iterator().next()
			val list_size = links_list.size()
			val links_it = links_list.iterator()
			val internalTime = System.nanoTime()
			while(links_it.hasNext){
				val linked_link: String = links_it.next()
				if(oldRanks.ranks.lookup(link_links.getKey)){
					val weighted: Double = weight((oldRanks.ranks.get(link_links.getKey).iterator().next().value()))/list_size
//					println("previous rank: "+oldRanks.ranks.get(linked_link).iterator().next().value)
//					println("Adding "+((weight((oldRanks.ranks.get(link_links.getKey).iterator().next().value())))/list_size+" to "+linked_link))
					val weightTime = System.nanoTime()
					newRanks.addData(new TitanData(linked_link, weighted.asInstanceOf[AnyRef]))
					val weightTimeFinish = System.nanoTime()
					int_total_weight+=weightTimeFinish-weightTime
				}

			}
			int_counter_weight+=1
			val internalFinish = System.nanoTime()
			int_counter+=1
			int_total+=internalFinish-internalTime
		}



		val midTime = System.nanoTime() //TODO: why am I doing this below?!?!?!?!
/*		val it2 = links.myorMap.getValue.entrySet().iterator()
		while(it2.hasNext){
			val key = it2.next().getKey
			if(oldRanks.ranks.lookup(key))
				if(!newRanks.ranks.lookup(key))
					newRanks.addData(new TitanData(key, 1.0.asInstanceOf[AnyRef]))
		}*/

		/*val it = current.ranks.getValue.entrySet().iterator()
		while(it.hasNext){
			val currentState = it.next()
			val key: String = currentState.getKey
			val currentValue: Double = currentState.getValue.iterator().next().value()
			if(links.myorMap.lookup(key)){
				val list: ORSet[String] = links.myorMap.get(key).iterator().next()
				val tuple: TitanData = new TitanData(key, (list, currentValue))
				val rankData: ListBuffer[TitanData] = this.iteration_compute(tuple)
				rankData.foreach{ tData: TitanData =>
					newRanks.addData(tData)
				}
			}else{
				newRanks.addData(new TitanData(key,currentValue.asInstanceOf[AnyRef]))
			}

		}*/
//		println("Left join with previous ranks size: "+current.size())
//		println("Left join done: "+newRanks.size()+" of size")
		val units = 1000000
		val endTime = System.nanoTime()
		val firstHalf = (midTime-startTime)/units
		val secondHalf = (endTime-midTime)/units
		val totalTime = (firstHalf+secondHalf)

		println("Join completion times: "+totalTime+" ("+((int_total/int_counter)/1000000)+" | "+((int_total_weight/int_counter_weight)/1000000)+")")
		doneBefore = true
	}
}
