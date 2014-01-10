package main.hacks.data.ccrdts

import sys.dht.catadupa.crdts.{CRDTRuntime, ORSet}
import main.titan.data.ccrdt.{CCRDTSkeleton, ComputationalCRDT}
import scala.collection.mutable.ListBuffer
import main.titan.data.messaging.TitanData
import java.lang.String
import scala.Predef.String


/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 06/11/13
 * Time: 11:41
 * To change this template use File | Settings | File Templates.
 */
class ORSetCCRDT(nodeName: String, partitions: Int) extends ComputationalCRDT{
	val reference: String = nodeName
	val partitioningSize: Int = partitions
	val skeleton = new CCRDTSkeleton(reference, partitioningSize)

	var orSet: ORSet[AnyRef] = new ORSet[AnyRef]();
	val runtime: CRDTRuntime = new CRDTRuntime(nodeName)

	//TODO: required for Kryo - really hate this
	def this() = this("",0)

	def addData(titanData: TitanData): TitanData = {
		this.orSet.add(titanData, runtime.getCausalityClock().recordNext(nodeName))
		return titanData;
	}

	def hollowReplica: ComputationalCRDT = {
		return new ORSetCCRDT(reference, partitioningSize)
	}

	private def ontoList(): ListBuffer[TitanData] = {
		val it: java.util.Iterator[AnyRef] = this.orSet.iterator()
		val lb: ListBuffer[TitanData] = new ListBuffer[TitanData]()
		while(it.hasNext)
			lb+=it.next().asInstanceOf[TitanData]
		return lb
	}

	//TODO: should not return the same data, but only the actually inserted onto the CCRDT
	override def merge(ccrdt: ComputationalCRDT): ListBuffer[TitanData] = {
		if(ccrdt!=null)
			this.orSet.merge(ccrdt.asInstanceOf[ORSetCCRDT].orSet);
		this.ontoList()
	}

	override def size(): Int = {
		return this.orSet.size()
	}

	override def toString(): String = {
		this.orSet.toString
	}
}
