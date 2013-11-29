package main.titan.computation

import java.lang.String
import scala.Predef.String
import main.titan.data.messaging.TitanData
import scala.collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 18/11/13
 * Time: 14:58
 * To change this template use File | Settings | File Templates.
 */
//TODO: ideally, it should return multiple Tuple2
//source(s) -> operation -> processing -> target
class Trigger(computationSource: String, operation: String, triggerTarget: String, timing: Int) {
	val source:String = computationSource;
	val operationID: String = operation;
	val target: String = triggerTarget;
	var local: Boolean = false;
	val time: Int = timing;
	var triggerType: String = "periodic"
	var iteration: Int = 0

	def getID(): String = {
		return source+"-"+operationID+"-"+target
	}

	//TODO: right now, this is how I set up the transformation function...
	//not quite working yet...
//	var computation: () => (AnyRef, AnyRef) = null
//	def setComputation(act: => (AnyRef, AnyRef)) = computation = () => act

	def compute(titanData: TitanData): ListBuffer[TitanData] = {
		if(source.equalsIgnoreCase("reader")){
//			println("coisas e tal")
			//basically, add the same tuple to links, which will handle removing duplicates, etc
			//TODO
			return null
		}else if(source.equalsIgnoreCase("links")){
			//TODO
		}else if(source.equalsIgnoreCase("ranks")){
			//TODO
		}
		return null;
	}

	override def toString(): String = {
		if(this.operationID.equalsIgnoreCase("key-join"))
			return source+" --("+operationID+"[?])--> "+target
		return source+" --("+operationID+")--> "+target
	}
}
