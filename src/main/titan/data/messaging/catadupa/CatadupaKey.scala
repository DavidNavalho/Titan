package main.titan.data.messaging.catadupa

import sys.dht.api.DHT
import java.security.MessageDigest

/**
 * Created by davidnavalho on 09/12/13.
 */
class CatadupaKey extends DHT.Key{
	var key: Long = 0

	def this(msgKey: Long){
		this
		this.key = msgKey
	}

	@Override
	def longHashValue(): Long = {
		return this.key
	}
}
