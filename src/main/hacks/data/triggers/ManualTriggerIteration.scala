package main.hacks.data.triggers

import main.titan.computation.ManualTrigger
import scala.collection.mutable.ListBuffer
import main.titan.data.messaging.TitanData

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 27/11/13
 * Time: 10:53
 * To change this template use File | Settings | File Templates.
 */
class ManualTriggerIteration(computationSource: String, operation: String, triggerTarget: String) extends ManualTrigger(computationSource, operation, triggerTarget){

	override def compute(titanData: TitanData): ListBuffer[TitanData] = {
		return null//TODO
	}
}
