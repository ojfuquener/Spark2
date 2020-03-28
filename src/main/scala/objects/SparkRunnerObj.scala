package objects

import scala.math.max
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

object SparkRunnerObj {

  def appName = "SparkWithKafka"
  def master = "local"

  def procesarQuijote(){
    /*val spark = oSparkSession.getSparkSession(appName)*/
    val sc = SparkSessionObj.getSparkContext(appName, master)
    val input = sc.textFile("file:///C:/Users/ojfuquene/Documents/don_quijote.txt")
    val palabras = input.flatMap(linea => linea.split("\\W+")) // "\\W+ Expresión regular parea representar una palabra
    val contarPalabras = palabras.countByValue()
    contarPalabras.foreach(println)
    sc.stop()
  }

  def procesarCSV()={
    val sc = SparkSessionObj.getSparkContext(appName, master)
    val lineas = sc.textFile("file:///C:/Users/ojfuquene/Downloads/data_sesion_spark/1800.csv")
    val lineasCSV = lineas.map(procesarLinea(_))
    lineasCSV.take(100).foreach(println)
    val temperaturasMaximas = lineasCSV.filter(campoRDD => campoRDD._2 == "TMAX")
    temperaturasMaximas.foreach(println)
    val estacionesYTemperaturas = temperaturasMaximas.map(elemento => (elemento._1, elemento._3.toFloat))
    estacionesYTemperaturas.foreach(println)
    val temperaturaMaxPorEstacion = estacionesYTemperaturas.reduceByKey((valor_tupla1, valor_tupla2) => max(valor_tupla1, valor_tupla2))
    temperaturaMaxPorEstacion.foreach(println)
    val resultados = temperaturaMaxPorEstacion.collect()
    for(resultado <- resultados.sorted){
      val estacion = resultado._1
      val temperatura = resultado._2
      val formato = f"${temperatura}%.2f F"
      println(s"${estacion} tiene como temperatura máxima ${formato}")
    }
    sc.stop()

  }

  def procesarLinea(linea:String) = {

    val campos = linea.split(",")
    val estacionMeteo = campos(0)
    val tipoInfo = campos(2)
    val temperatura = campos(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f // Conversión de grados Farenheit a Celsius
    (estacionMeteo, tipoInfo, temperatura)
  }

  def readDataSQL () = {
    SparkSessionObj.getDataCassandra(appName)
  }
}
