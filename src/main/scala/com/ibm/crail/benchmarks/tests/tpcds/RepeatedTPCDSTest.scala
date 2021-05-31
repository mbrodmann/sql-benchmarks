package com.ibm.crail.benchmarks.tests.tpcds

import com.ibm.crail.benchmarks.{ParseOptions, SQLTest}
import org.apache.spark.sql.SparkSession

import scala.concurrent.{Await, Future, future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

/**
  * Created by atr on 04.09.17.
  */
class RepeatedTPCDSTest(val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) {
  
  private val repetitions = options.repetitions;
  private val parallelism = 1
  
  // you set up the temp view
  TPCDSSetup.readAndRegisterTempTables(options, spark)
  private val query = TPCDSQueries.query(options.getTPCDSQuery+".sql")
  private var results: List[org.apache.spark.sql.DataFrame] = List()
  
  for(i <- 1 to parallelism) {
    results = spark.sql(query) :: results
  }

  results.foreach(x => println(x))

  private val result = results(0)
  

  

  override def execute(): String = {
    var res: String = "";  
    for(i <- 1 to repetitions) {

      // val sqlTasks: Seq[Future[String]] = for (j <- 1 to parallelism) yield future {
      //   println("Running query " + options.getTPCDSQuery + " " + i + "/" + repetitions + " as parallel task " + j)
      //   takeAction(options, results(j-1))
      // }

      // val aggregated = Future.sequence(sqlTasks)

      // val res = Await.result(aggregated, 1000.seconds)

      val start = System.currentTimeMillis()
      takeAction(options, results(0))
      val end = System.currentTimeMillis()
      println(end + ": Running query " + options.getTPCDSQuery + " " + i + "/" + repetitions + " took " + (end-start) + " ms")

    }

    "Completed " + repetitions + " repetitions of " + parallelism + " parallel tasks."
  }

  def foo(x:Int) = ()

  override def explain(): Unit = ()

  override def plainExplain(): String = s"TPC-DS query ${options.getTPCDSQuery} on " + options.getInputFiles()(0)

  override def printAdditionalInformation(): String = s"SQL query ${options.getTPCDSQuery}: ${query}"

}
