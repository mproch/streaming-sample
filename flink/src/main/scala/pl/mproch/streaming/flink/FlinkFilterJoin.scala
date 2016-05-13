package pl.mproch.streaming.flink

import java.util.Arrays

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector

object FlinkFilterJoin extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(5)

  env.setParallelism(10)



  env.fromCollection(Arrays.asList("ala", "ma", "kota"))
      .connect(env.fromElements(""))

    .keyBy(1, 2)

    .flatMap(new RichCoFlatMapFunction[String, String, String] {
    override def flatMap2(in2: String, collector: Collector[String]) = ???

    override def flatMap1(in1: String, collector: Collector[String]) = ???
  }).startNewChain()
     // .window()

    .print

  env.execute

}
