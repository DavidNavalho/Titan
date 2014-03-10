package examples.tests.deprecated

import main.titan.data.ccrdt.ComputationalCRDT
import main.hacks.data.ccrdts.{ScratchpadRanks, Links, ORSetCCRDT}
import scala.io.Source
import main.titan.data.messaging.TitanData
import examples.tests.RanksDelta


/**
 * Created by davidnavalho on 11/12/13.
 */
object CRDTTesting {

	/*def LinksRanksTrigger_compute(titanData: TitanData): ListBuffer[TitanData] = {
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
	}*/

	def populateLinks(source: String): Links = {
		val links: Links = new Links("links",1);
		for(line <- Source.fromFile(source).getLines()) {
			val words: Array[String] = line.split(' ');
			if(words.length!=2)
				println("Read something weird...")
			val data = new TitanData(words(0), words(1))
			links.addData(data)
		}
		return links
	}

	def oldTest(){
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

	def newRank(previousRanks: RanksDelta, links: Links, iteration: Int): RanksDelta = {
		val newRanks: RanksDelta = new RanksDelta
		val it = links.myorMap.getValue.entrySet().iterator()
		//for each link, get it's name and previous rank
		while(it.hasNext){
			val next = it.next()
			val source = next.getKey
			if(!previousRanks.ranks.contains(source))
				previousRanks.simpleUpdate(source,1.0,0)
			val previousRank: Rank = previousRanks.ranks.get(source).get //I likely need some creation magic here, too (some ranks are only inserted on this step)
			val previousValue: Double = previousRank.initRank+previousRank.delta
			val targets = next.getValue.iterator().next().iterator()
			val size: Int = next.getValue.iterator().next().size()
//			println(size)
			//iterate over it's ranks
			while(targets.hasNext){
				val target: String = targets.next()
				newRanks.simpleUpdate(target,(previousValue/size),iteration)
			}
		}
		return newRanks
	}

	def pageRankWithDelta(source: String){
		val links: Links = this.populateLinks(source)
		var ranks0: RanksDelta = new RanksDelta
//		println(links)
		val it = links.myorMap.getValue.entrySet().iterator()
		while(it.hasNext){
			val next = it.next()
			val source = next.getKey
			ranks0.simpleUpdate(source,1.0,0)
		}
//		println(ranks0)
		for(i <- 1 to 50){
			val ranks1: RanksDelta = newRank(ranks0, links,1)
			val shouldStop: Boolean = this.checkEveryRankBelow(10, ranks1, ranks0)
//			println(ranks1)
			ranks0 = ranks1
			println("Iteration "+i+" complete.")
		/*	if(i>28)
				println(ranks0)*/

			if(shouldStop)
				println("Stop conditions met")
		}

	}

	def checkEveryRankBelow(threshold: Double, ranks: RanksDelta, previousRanks: RanksDelta): Boolean = {
	  ranks.ranks.foreach{ r=>
		  if(math.abs(previousRanks.ranks.get(r._1).get.delta-r._2.delta)>threshold)
			  return false
	  }
		return true
	}

	def main(args: Array[String]){
		pageRankWithDelta("pagerank_data.txt")

	}
}
