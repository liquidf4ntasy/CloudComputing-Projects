package edu.uta.cse6331

import org.apache.spark.{SparkConf, SparkContext}

/** CSE 6331: Project 3 - scala Matrix Multiplication
  *  Name: Satyajit Deshmukh
  *  UTA ID: 1001417727
  */
object Multiply{

  def main(args: Array[String]): Unit = {
	  // set spark config and context 
    val sparkConf = new SparkConf().setAppName("Multiply").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
	// parse 1st argument to the program as 1st matrix using , as delimiter
    val Mmatrix = sparkContext.textFile(args(0)).map(line => 
	{
      val lineElements = line.split(",")
      (lineElements(0).toInt, lineElements(1).toInt, lineElements(2).toDouble)
    }
	)
	// parse 2nd argument file into 2nd matrix
    val Nmatrix = sparkContext.textFile(args(1)).map(line => {
      val lineElements = line.split(",")
      (lineElements(0).toInt, lineElements(1).toInt, lineElements(2).toDouble)
    })


    val ip1Mapper = Mmatrix.map(elem => (elem._2, elem))
    val ip2Mapper = Nmatrix.map(elem => (elem._1, elem))
    val mapper1Output = ip1Mapper join ip2Mapper
    mapper1Output.collect.foreach {
      println(_)
    }
    val mapper2Output = mapper1Output map { case (j, (a, b)) => ((a._1, b._2), a._3 * b._3) }
    val reducer1Input = mapper2Output
    val reducer2Output = reducer1Input.reduceByKey(_ + _)
    val sortedOutput = reducer2Output.sortByKey(ascending = true, 0)
    sortedOutput.collect.foreach((a) => {
      println(s"${a._1},${a._2}")
    })
	// save the output to a text file specified by the file name in 3rd argument
    sortedOutput.saveAsTextFile(args(2))
    sparkContext.stop()
  }
}