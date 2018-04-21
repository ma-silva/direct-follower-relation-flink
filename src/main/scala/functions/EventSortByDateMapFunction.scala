package functions

import mining.Trace
import org.apache.flink.api.common.functions.RichMapFunction
import org.joda.time.DateTime

class EventSortByDateMapFunction extends RichMapFunction[Trace, Trace] {
  implicit def dateOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)

  override def map(value: Trace): Trace = {
    Trace(value.traceId, value.events.sortBy(_.start))
  }
}