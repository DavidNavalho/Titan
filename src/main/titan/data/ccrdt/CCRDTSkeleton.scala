package main.titan.data.ccrdt

import java.math.BigInteger
import java.security.MessageDigest

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 06/11/13
 * Time: 13:53
 * To change this template use File | Settings | File Templates.
 */
class CCRDTSkeleton(name: String, size: Int) extends Serializable{
	val reference: String = name;
	val partitioningSize: Int = size;
	var digest: MessageDigest = null

	def this() = this("",0)

	//maybe the hashing function should be a class of it's own??
	def hashingFunction(key: String): Long = {
		if(digest==null) digest = java.security.MessageDigest.getInstance("MD5")
		digest.synchronized {
			digest.reset()
			val partition: Int = math.abs(key.hashCode%partitioningSize) + 1;//or maybe this is what I really need to define??
			digest.update((reference+partition).getBytes());
			return new BigInteger(1, digest.digest()).longValue() >>> 1;
		}
	}

	def getPartitionKey(position: Int): Long = {
		if(digest==null) digest = java.security.MessageDigest.getInstance("MD5")
		digest.synchronized{
			digest.reset()
			digest.update((reference+position).getBytes())
			return new BigInteger(1, digest.digest()).longValue() >>> 1;
		}
	}
}
