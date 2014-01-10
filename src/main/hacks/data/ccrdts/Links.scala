package main.hacks.data.ccrdts

import vrs0.crdts.ORMap
import sys.dht.catadupa.crdts.ORSet
import main.titan.data.messaging.TitanData
import scala.collection.mutable.ListBuffer
import main.titan.data.ccrdt.ComputationalCRDT
import scala.Predef._
import java.util.{Set, Iterator}

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 07/11/13
 * Time: 10:20
 * To change this template use File | Settings | File Templates.
 */
class Links(nodeName: String, partitions: Int) extends MapCCRDT(nodeName, partitions){

	//TODO: the ORSet should also allow for NO repeats
	val myorMap: ORMap[String, ORSet[String]] = new ORMap[String, ORSet[String]]();

	def this() = this("",0)

	override def toString: String = {
		var result: String = ""
		val it = this.myorMap.getValue.entrySet().iterator()
		while(it.hasNext){
			val next = it.next()
			val linked = next.getValue.iterator().next().iterator()
			var linked_links: String = "{"
			while(linked.hasNext)
				linked_links+=linked.next()+" "
			result+=next.getKey+": "+linked_links+"}\r\n"
		}
		return result
	}

	override def size(): Int = {
		return this.myorMap.size()
	}

	override def hollowReplica: ComputationalCRDT = {
//		println("Creating an empty Links!")
		return new Links(this.reference, this.partitioningSize);
	}

	//no linking to itself (key==value);
	// repeat keys simply add the data to the existing one;
	//repeat data does not repeat (ORSet does this already)
	//TODO: doing unecessary stuff here
	override def addData(titanData: TitanData): TitanData = {
		//val tuple: (String, String) = data.asInstanceOf[(String, String)];
		//val key: String = tuple._2.asInstanceOf[String];
		val key: String = titanData.key;
		val data: String = titanData.data.asInstanceOf[String]
//		println(key+"|"+data)
		if(key.equals(data))
			return null//do not add repeats
		if(!this.myorMap.lookup(data)){
			val set: ORSet[String] = new ORSet[String]()
			this.myorMap.insert(data, set)//TODO:return null???
		}
		if(this.myorMap.lookup(key)){
			val set: ORSet[String] = this.myorMap.get(key).iterator().next()
			set.add(data,runtime.getCausalityClock().recordNext(reference))
			return titanData
			//TODO: is this correct???
			/*if(!set.contains(key)){
				set.add(data,runtime.getCausalityClock().recordNext(reference))
				return titanData
			}*/
		}else{
			val set: ORSet[String] = new ORSet[String]()
			//println(tuple._1)
			set.add(data,runtime.getCausalityClock().recordNext(reference))
			this.myorMap.insert(key, set)
			return titanData
		}
		return null
	}

	def addNoRepeats(key: String, value: String) {
		if(key.equals(value)) return;
		if(this.myorMap.lookup(key)){
			val set: ORSet[String] = this.myorMap.get(key).iterator().next()
			if(!set.contains(key)){
				set.add(value,runtime.getCausalityClock().recordNext(reference))
				return;
			}
		}else{
			val set: ORSet[String] = new ORSet[String]()
			//println(tuple._1)
			set.add(value,runtime.getCausalityClock().recordNext(reference))
			this.myorMap.insert(key, set)
			return;
		}
	}

	//????
	private def ontoList():ListBuffer[TitanData] = {
		val lb: ListBuffer[TitanData] = new ListBuffer[TitanData]()
		val it = this.myorMap.getValue.entrySet().iterator()
		while(it.hasNext){
			val next = it.next()
			val td: TitanData = new TitanData(next.getKey, next.getValue.iterator().next())
			lb+=td
		}
		return lb
	}

	//TODO: return the added elements instead
	//TODO: crap, just implement one
	override def merge(ccrdt: ComputationalCRDT): ListBuffer[TitanData] = {
	  val externalCCRDT: Links = ccrdt.asInstanceOf[Links]
		val insideData: Set[String] = externalCCRDT.myorMap.getValue.keySet()
		val it: Iterator[String] = insideData.iterator();
		while(it.hasNext){
			val next: String = it.next()
			val values: ORSet[String] = externalCCRDT.myorMap.get(next).iterator().next()
			var myOrSet: ORSet[String] = null
			if(myorMap.lookup(next)){
				val blah: java.util.Iterator[ORSet[String]] = myorMap.get(next).iterator()
				myOrSet = blah.next();
				myOrSet.merge(values)
				//TODO: not ideal, but can give concurrency exceptions if I don't 'close' it
				while(blah.hasNext)
					blah.next()
			}
			else{
				myOrSet = new ORSet[String]()
				myOrSet.merge(values);
				myorMap.insert(next, myOrSet);
			}
//			values.add(next,runtime.getCausalityClock().recordNext(reference))
			//addNoRepeats(next, values)
		}
		//println("new size: "+this.myorMap.size())
//		return null;
		ontoList()
	}
}
