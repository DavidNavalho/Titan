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

	//TODO: something weird going on....think it's the CRDTs....
	 def check(scratchpad: ScratchpadRanks): Boolean = {
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
				 return false
			 }

		 }
//		println("Check Trigger max difference: "+this.blah_max)
		 return true
	}

	//[link(key), [[links], rank](data)]
	def iteration_compute(data: TitanData): ListBuffer[TitanData] = {
		val dataSet: ListBuffer[TitanData] = new ListBuffer[TitanData]
		val tupleData: (ORSet[String], Double) = data.data.asInstanceOf[(ORSet[String], Double)]
		val distributedRank: Double = tupleData._2/tupleData._1.size()
		val it = tupleData._1.iterator()
		while(it.hasNext)
			dataSet+=new TitanData(it.next(), distributedRank.asInstanceOf[AnyRef])
		return dataSet
	}

	//TODO: problem: if a link isn't linked, what's it's score? 1?
	//for each join, run the computation and add it to a ListBuffer of titandata of the format: [link(key), [[links], rank](data)]
	def leftJoin(scratchpad: ScratchpadRanks, links: Links){
//		println("LEFTJOIN SCRATCHPAD LENGTH: "+scratchpad.scratchpads.length)
		val current: Ranks = scratchpad.getLast.asInstanceOf[Ranks]
		scratchpad.addNew(current.hollowReplica)
//		println("LEFTJOIN SCRATCHPAD LENGTH AFTER ADD: "+scratchpad.scratchpads.length)
		val newRanks: Ranks = scratchpad.getLast.asInstanceOf[Ranks]
//		println("Previous join different: "+current.ranks.getValue.entrySet().size())
		val it = current.ranks.getValue.entrySet().iterator()
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

		}
//		println("Left join with previous ranks size: "+current.size())
//		println("Left join done: "+newRanks.size()+" of size")
	}
}
