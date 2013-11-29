package main.titan.data.messaging

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 20/11/13
 * Time: 09:31
 * To change this template use File | Settings | File Templates.
 */
class TitanData(dataKey: String, dataRef: AnyRef) {
	val key: String = dataKey;
	val data: AnyRef = dataRef;
}
