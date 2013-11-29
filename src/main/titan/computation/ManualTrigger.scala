package main.titan.computation

import main.titan.data.messaging.TitanData
import scala.collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 22/11/13
 * Time: 10:54
 * To change this template use File | Settings | File Templates.
 */
class ManualTrigger(computationSource: String, operation: String, triggerTarget: String) extends Trigger(computationSource, operation, triggerTarget, 0){
	triggerType = "activated"

	override def compute(titanData: TitanData): ListBuffer[TitanData] = {
//		println("blah")
		return null
	}
}
