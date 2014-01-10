package examples.tests

import java.lang.String
import main.titan.data.ccrdt.ComputationalCRDT
import main.hacks.data.ccrdts.{ScratchpadRanks, Links, ORSetCCRDT}
import scala.io.Source
import main.titan.data.messaging.TitanData
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import sys.dht.catadupa.crdts.ORSet

/**
 * Created by davidnavalho on 11/12/13.
 */
object CRDTTesting {

	def LinksRanksTrigger_compute(titanData: TitanData): ListBuffer[TitanData] = {
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

	def main(args: Array[String]){
		val reader: ComputationalCRDT = new ORSetCCRDT("reader",1);
		val links: Links = new Links("links",2);
		val init_ranks: ScratchpadRanks = new ScratchpadRanks("ranks",2,2)

		for(line <- Source.fromFile("pagerank_data_mini.txt").getLines()) {
			val words: Array[String] = line.split(' ');
			if(words.length!=2)
				println("Read something weird...")
			val data = new TitanData(words(0), words(1))
			reader.addData(data)
			links.addData(data)
		}
		println("Reader size: "+reader.size())
		println("Links size:"+links.size())
		println(links.toString)

		val it = links.myorMap.getValue.entrySet().iterator()
		while(it.hasNext){
			val next = it.next()
			val source = next.getKey
			val linked_it = next.getValue.iterator().next().iterator()
		}


	}
}
