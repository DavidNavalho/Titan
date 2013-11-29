package main.titan.data.ccrdt

import java.security.MessageDigest
import java.math.BigInteger
import scala.collection.mutable.ListBuffer
import main.titan.data.messaging.TitanData

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 06/11/13
 * Time: 10:50
 * To change this template use File | Settings | File Templates.
 */
trait ComputationalCRDT{
	//(nodeName: String, partitions: Int)
	val reference: String //= nodeName;
  val partitioningSize: Int //= partitions;
	val skeleton: CCRDTSkeleton// = new CCRDTSkeleton(reference, partitioningSize)


	def addData(data: TitanData): TitanData
	def hollowReplica: ComputationalCRDT

	def merge(ccrdt: ComputationalCRDT): ListBuffer[TitanData]

	def size(): Int

}
