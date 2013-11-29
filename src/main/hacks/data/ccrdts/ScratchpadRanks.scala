package main.hacks.data.ccrdts

import main.titan.data.ccrdt.{CCRDTSkeleton, ComputationalCRDT}
import vrs0.crdts.ORMap
import vrs0.crdts.more.CRDTDouble
import scala.collection.mutable.ListBuffer
import main.titan.data.messaging.TitanData

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 26/11/13
 * Time: 21:15
 * To change this template use File | Settings | File Templates.
 */
//TODO: missing generic scratchpad used in DSL implementation
//TODO: it seems a very common comparison is to copmpare same-key scratchpad objects
//TODO: scratchpad's implementation should take this into account...
class ScratchpadRanks(nodeName: String, partitions: Int, maxsize: Int) extends ComputationalCRDT{
	val reference: String = nodeName
	val partitioningSize: Int = partitions
	val skeleton: CCRDTSkeleton = new CCRDTSkeleton(reference, partitioningSize)
	val maxSize: Int = maxsize



//	val ranks: ORMap[String, CRDTDouble] = new ORMap[String, CRDTDouble]();
	var scratchpads: ListBuffer[ComputationalCRDT] = new ListBuffer[ComputationalCRDT]
	//initialize the first ranks
	this.scratchpads+=new Ranks(nodeName, partitions);

	def get(pos: Int): ComputationalCRDT = {
		return this.scratchpads(pos)
	}

	def getLast: ComputationalCRDT = {
		return this.scratchpads.last
	}

	def getPrevious: ComputationalCRDT = {
		return this.scratchpads(this.scratchpads.length-2)
	}

	def addNew(newCRDT: ComputationalCRDT) = {
		if(this.scratchpads.length>=maxSize)
			this.scratchpads.remove(0)
		this.scratchpads += newCRDT
	}

	override def size(): Int = {
		return this.getLast.size
	}

	override def merge(ccrdt: ComputationalCRDT): ListBuffer[TitanData] = {
		return this.getLast.merge(ccrdt.asInstanceOf[ScratchpadRanks].getLast)
	}

	override def addData(data: TitanData): TitanData = {
		return this.getLast.addData(data)
	}

	override def hollowReplica: ComputationalCRDT = {
		return new ScratchpadRanks(nodeName, partitions, maxSize)
	}
}
