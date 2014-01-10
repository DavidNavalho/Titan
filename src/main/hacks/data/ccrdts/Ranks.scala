package main.hacks.data.ccrdts

import vrs0.crdts.ORMap
import vrs0.crdts.more.CRDTDouble
import main.titan.data.ccrdt.{CCRDTSkeleton, ComputationalCRDT}
import scala.collection.mutable.ListBuffer
import main.titan.data.messaging.TitanData
import java.util.{Set, Iterator}

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 07/11/13
 * Time: 11:21
 * To change this template use File | Settings | File Templates.
 */                      //check how ranking is being done?
class Ranks(nodeName: String, partitions: Int) extends ComputationalCRDT{
	val reference: String = nodeName
	val partitioningSize: Int = partitions
	val skeleton = new CCRDTSkeleton(reference, partitioningSize)

	val ranks: ORMap[String, CRDTDouble] = new ORMap[String, CRDTDouble]();

	def this() = this("",0)

	//receives tuple: Link, ORSet(?) of links
	def addData(titanData: TitanData): TitanData = {
		//val tuple: (String, Double) = data.asInstanceOf[(String, Double)];
		val link: String = titanData.key;
		val linkRank: Double = titanData.data.asInstanceOf[Double]
		if(this.ranks.lookup(link)){
			//println("before: "+this.ranks.get(link).iterator().next().value())
			val newValue: Double = this.ranks.get(link).iterator().next().add(linkRank)
			//println("after"+this.ranks.get(link).iterator().next().value())
			return new TitanData(link, newValue.asInstanceOf[AnyRef]);
		}
		else{
			val newCounter: CRDTDouble = new CRDTDouble(linkRank);
			this.ranks.insert(link, newCounter);
			return titanData;
		}
	}



	def hollowReplica: ComputationalCRDT = {
		return new Ranks(this.reference, this.partitioningSize);
	}

	def addTuple(tuple: (AnyRef, AnyRef)): (AnyRef, AnyRef) = {
		return null
	}

	private def ontoList():ListBuffer[TitanData] = {
		val lb: ListBuffer[TitanData] = new ListBuffer[TitanData]()
		val it = this.ranks.getValue.entrySet().iterator()
		while(it.hasNext){
			val next = it.next()
			val td: TitanData = new TitanData(next.getKey, next.getValue.iterator().next())
			lb+=td
		}
		return lb
	}

	override def merge(ccrdt: ComputationalCRDT): ListBuffer[TitanData] = {
		val externalCCRDT: Ranks = ccrdt.asInstanceOf[Ranks];
		val externalKeys: Set[String] =  externalCCRDT.ranks.getValue.keySet()
		val it: java.util.Iterator[String] = externalKeys.iterator()
		while(it.hasNext){
			val next: String = it.next()
			val value: CRDTDouble = externalCCRDT.ranks.get(next).iterator().next()
			var myRank: CRDTDouble = null
			if(this.ranks.lookup(next)){
				val blah: java.util.Iterator[CRDTDouble] = this.ranks.get(next).iterator()
				myRank = blah.next()
				myRank.add(value.value())//TODO:this is not correct....but it suffices for now...
				while(blah.hasNext)
					blah.next()
			}
			else{
				myRank = new CRDTDouble()
				myRank.add(value.value())
				this.ranks.insert(next,myRank)
			}
		}
//		println(this.toString())
		this.ontoList()
	}

	override def size(): Int = {
		return this.ranks.size()
	}

	private def weight(value: Double): Double = {
		return (0.15+0.85*value)
	}

	override def toString(): String = {
		var result: String = "";
		val it = this.ranks.getValue.entrySet().iterator()
		var counter: Int = 0;
		while(it.hasNext){
			counter+=1
			val itRank = it.next()
			result+="Ranking: "+itRank.getKey+"["+weight(itRank.getValue.iterator().next().value())+"]\r\n"
		}
		return result
	}
}
