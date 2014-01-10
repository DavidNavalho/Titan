package main.hacks.data.ccrdts

import vrs0.crdts.ORMap
import sys.dht.catadupa.crdts.CRDTRuntime
import main.titan.data.ccrdt.{CCRDTSkeleton, ComputationalCRDT}
import scala.collection.mutable.ListBuffer
import main.titan.data.messaging.TitanData

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 07/11/13
 * Time: 09:24
 * To change this template use File | Settings | File Templates.
 */
class MapCCRDT(nodeName: String, partitions: Int) extends ComputationalCRDT{
	val reference: String = nodeName
	val partitioningSize: Int = partitions
	val skeleton = new CCRDTSkeleton(reference, partitioningSize)

	val orMap: ORMap[String, String] = new ORMap[String, String]();
	val runtime: CRDTRuntime = new CRDTRuntime(nodeName);

	def this() = this("",0)

	def addData(titanData: TitanData): TitanData = {
		//val tuple: (String, String) = data.asInstanceOf[(String, String)];
		val key: String = titanData.key
		val data: String = titanData.data.asInstanceOf[String]
		this.orMap.insert(key, data)
		return titanData
	}

	def hollowReplica: ComputationalCRDT = {
		return new MapCCRDT(reference, partitioningSize)
	}

	//TODO
	def merge(ccrdt: ComputationalCRDT): ListBuffer[TitanData] = {
		println("niksa: "+this.reference)
		return null;
	}

	def size(): Int = {
		return this.orMap.size()
	}
}
