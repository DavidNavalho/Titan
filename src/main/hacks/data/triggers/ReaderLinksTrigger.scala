package main.hacks.data.triggers

import main.titan.computation.{ManualTrigger, Trigger}
import main.titan.data.messaging.TitanData
import scala.collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * User: davidnavalho
 * Date: 19/11/13
 * Time: 16:33
 * To change this template use File | Settings | File Templates.
 */
class ReaderLinksTrigger(computationSource: String, operation: String, triggerTarget: String) extends ManualTrigger(computationSource, operation, triggerTarget){

	def this() = this("","","")

	override def compute(titanData: TitanData): ListBuffer[TitanData] = {
		return (new ListBuffer[TitanData]()+=titanData);
	}
}
