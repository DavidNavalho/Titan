package main.hacks.data.ccrdts

import main.titan.data.ccrdt.{CCRDTSkeleton, ComputationalCRDT}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import scala.collection.mutable.ListBuffer
import com.esotericsoftware.kryo.io.{Input, Output}
import main.titan.data.messaging.TitanData

/**
 * Created by davidnavalho on 10/02/14.
 */
class ExtendedScratchpadRanks(nodeName: String, partitions: Int, maxsize: Int) extends ComputationalCRDT with KryoSerializable{
	val reference: String = nodeName
	val partitioningSize: Int = partitions
	val skeleton: CCRDTSkeleton = new CCRDTSkeleton(reference, partitioningSize)
	val maxSize: Int = maxsize



	//	val ranks: ORMap[String, CRDTDouble] = new ORMap[String, CRDTDouble]();
	var scratchpads: ListBuffer[ComputationalCRDT] = new ListBuffer[ComputationalCRDT]
	//initialize the first ranks
	this.scratchpads+=new Ranks(nodeName, partitions);

	def write(kryo: Kryo, output: Output) {
		output.writeString(reference)
		output.writeInt(partitioningSize)
		//		kryo.writeClassAndObject(output, skeleton)
		output.writeInt(maxSize)
		//ListBuffer
		//		println("Writing "+scratchpads.size)
		output.writeInt(scratchpads.size)
		scratchpads.foreach{ccrdt: ComputationalCRDT =>
			kryo.writeClassAndObject(output, ccrdt)
		}

	}

	def read(kryo: Kryo, input: Input): Unit = {
		//		println("Reading ref")
		val ref: String = input.readString()
		//		println("Reading partSize")
		val partSize: Int = input.readInt()
		//		val skel: CCRDTSkeleton = kryo.readClassAndObject(input).asInstanceOf[CCRDTSkeleton]
		//		println("Reading maxSize")
		val maxSize: Int = input.readInt()
		//		println("Reading pads")
		val pads: Int = input.readInt()
		val sr: ScratchpadRanks = new ScratchpadRanks(ref, partSize, maxSize)
		for(i <- 1 to pads){
			//			println("Reading ccrdts")
			val ccrdt: ComputationalCRDT = kryo.readClassAndObject(input).asInstanceOf[ComputationalCRDT]
			sr.scratchpads+=ccrdt
		}
		return sr
	}


	def this() = this("",0,0)

	def get(pos: Int): ComputationalCRDT = {
		return this.scratchpads(pos)
	}

	def getLast: ComputationalCRDT = {
		return this.scratchpads(this.scratchpads.length-1)
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
