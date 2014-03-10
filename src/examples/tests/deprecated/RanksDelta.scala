package examples.tests.deprecated

import scala.collection.mutable

/**
 * Created by davidnavalho on 16/01/14.
 */
class RanksDelta {

	val ranks: mutable.HashMap[String, Rank] = new mutable.HashMap[String, Rank]()

	def simpleUpdate(targetLink: String, newDelta: Double, iteration: Int){
		var rank: Rank = null
		if(this.ranks.contains(targetLink)){
			rank = this.ranks.get(targetLink).get
			rank.delta+= newDelta
			rank.lastIteration = iteration
		}else{
			rank = new Rank(0.0,newDelta,iteration)
			this.ranks.put(targetLink, rank)
		}
	}

	override def toString: String = {
		var str: String = ""
		this.ranks.foreach{ r =>
			str+=r._1+": "+(r._2.initRank+r._2.delta)+" ["+r._2.lastIteration+"]\r\n"
		}
		return str
	}

}

class Rank(var initRank: Double, var delta: Double, var lastIteration: Int){
	def addDelta(delta: Double){
		this.delta+=delta
	}
}