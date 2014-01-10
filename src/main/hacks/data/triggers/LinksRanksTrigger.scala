package main.hacks.data.triggers

import main.titan.computation.ManualTrigger
import main.titan.data.messaging.TitanData
import sys.dht.catadupa.crdts.ORSet
import scala.collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 22/11/13
 * Time: 15:13
 * To change this template use File | Settings | File Templates.
 */
class LinksRanksTrigger(computationSource: String, operation: String, triggerTarget: String) extends ManualTrigger(computationSource, operation, triggerTarget){

	def this() = this("","","")

	override def compute(titanData: TitanData): ListBuffer[TitanData] = {
//		val key: String = titanData.key //not required
		val data: ORSet[String] = titanData.data.asInstanceOf[ORSet[String]]
		val dataSet: ListBuffer[TitanData] = new ListBuffer[TitanData]()
		val initRank: Double = 1.0;
		val it: java.util.Iterator[String] = data.iterator()
		val size: Double = data.size()
		while(it.hasNext){
			val key: String = it.next()
			val titanData: TitanData = new TitanData(key, (initRank/size).asInstanceOf[AnyRef])
			dataSet+=titanData
		}
		return dataSet
	}

}
