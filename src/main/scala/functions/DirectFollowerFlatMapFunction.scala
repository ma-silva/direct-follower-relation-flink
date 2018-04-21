package functions

import mining.Trace
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

class DirectFollowerFlatMapFunction extends RichFlatMapFunction[Trace, ((String, String), Int)]{
  override def flatMap(in: Trace, collector: Collector[((String, String), Int)]): Unit = {
    val activities = in.events.map(x=>x.activity)
    for(activity <- activities.zip(activities.tail).map(x=>(x,1)))
      collector.collect(activity)
  }
}
