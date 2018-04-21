package functions

import mining.{Event, Trace}
import org.apache.flink.api.common.functions.AggregateFunction

class TraceAggregateFunction extends AggregateFunction[Event, Trace, Trace] {
  override def add(value: Event, accumulator: Trace): Trace = Trace(value.traceId, accumulator.events :+ value)

  override def createAccumulator(): Trace = Trace("", Seq())

  override def getResult(accumulator: Trace): Trace = accumulator

  override def merge(a: Trace, b: Trace): Trace = a
}
