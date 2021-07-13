import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._

import com.typesafe.config.ConfigFactory
import java.lang.Math._
import pmmlmodel.KMeansIrisPMML
import org.pmml4s.model.Model

object Main {
  def main(args: Array[String]): Unit = {
    val session = getSnowflakeSession()
    val libPath = new java.io.File("").getAbsolutePath
    session.addDependency(s"$libPath/src/main/resources/pmml4s_2.12-0.9.11.jar")
    session.addDependency(s"$libPath/src/main/resources/spray-json_2.12-1.3.6.jar")
    session.addDependency(s"$libPath/src/main/resources/scala-xml_2.12-1.2.0.jar")
    session.addDependency(s"$libPath/src/main/resources/iris.jar")

    val df = getIrisDf(session)
    println(df.show())

    val testFunc = new SerTestFunc
    val irisTransformationUDF = udf(testFunc.irisTransformationFunc)
    
    val dfFitted = df.withColumn(
      "label",
      irisTransformationUDF(
        col("sepal_length"), col("sepal_width"), col("petal_length"), col("petal_width"))
    )

    println(dfFitted.show())
  }

  /** debug code */
  // def clusteringIrisUdf(session: Session): Unit = {
  //
  //   val df = getIrisDf(session)
  //
  //   // val model = new KMeansIrisPMML("KMeansIris.pmml").getModel()
  //   val model: Model = Model.fromFile("src/main/resources/KMeansIris.pmml")
  //
  //   val irisTransformationFunc = (
  //     sepal_length: Double,
  //     sepal_width: Double,
  //     petal_length: Double,
  //     petal_width: Double) => {
  //       val v = Array[Double](sepal_length, sepal_width, petal_length, petal_width)
  //       model.predict(v).last.asInstanceOf[String]
  //     }
  //   val irisTransformationUDF = udf(irisTransformationFunc)
  //   df.withColumn(
  //     "label",
  //     irisTransformationUDF(
  //       col("sepal_length"), col("sepal_width"), col("petal_length"), col("petal_width"))
  //   ).show()
  // }
  //
  // def udfSample(session: Session): Unit = {
  //   // 初回登録用
  //   println(IEEEremainder(5.1, 0.2))
  //   val ereFunc = (x:Double, y:Double) => IEEEremainder(x, y)
  //   val myEreUdf = session.udf.registerPermanent("ereUdf1", ereFunc, "%iris_data")
  //   // session.table("iris_data").select(myEreUdf(col("sepal_width"), col("petal_width"))).show()

  //   val df = session.table("IRIS_DATA")
  //   df.select(callUDF("ereUdf", col("sepal_width"), col("petal_width"))).show()
  //   df.select(callUDF("ereUdf1", col("sepal_width"), col("petal_width"))).show()
  // }
  /** debug code */

  def getSnowflakeSession(): Session = {
    val conf = ConfigFactory.load
    val configs = Map(
      "URL" -> conf.getString("snowflake.url"),
      "USER" -> conf.getString("snowflake.user"),
      "PASSWORD" -> conf.getString("snowflake.password"),
      "ROLE" -> conf.getString("snowflake.role"),
      "WAREHOUSE" -> conf.getString("snowflake.warehouse"),
      "DB" -> conf.getString("snowflake.db"),
      "SCHEMA" -> conf.getString("snowflake.schema")
    )

    val session = Session.builder.configs(configs).create
    session
  }

  def getIrisDf(session: Session): DataFrame = {
    val irisSchema = StructType(
      StructField("sepal_length", DoubleType, nullable = true) ::
      StructField("sepal_width", DoubleType, nullable = true) ::
      StructField("petal_length", DoubleType, nullable = true) ::
      StructField("petal_width", DoubleType, nullable = true) ::
      StructField("class", StringType, nullable = true) ::
      Nil
    )
    val df = session.read.schema(irisSchema).table("iris_data")
    df
  }
}

class TestFunc {
  val model: Model = new KMeansIrisPMML("KMeansIris.pmml").getModel()
  // KMeansIrisPMML("KMeansIris.pmml"))

  val irisTransformationFunc = (
    sepal_length: Double,
    sepal_width: Double,
    petal_length: Double,
    petal_width: Double) => {
      val v = Array[Double](sepal_length, sepal_width, petal_length, petal_width)
      model.predict(v).last.asInstanceOf[String]
    }
}

class SerTestFunc extends Serializable {
  val model: Model = new KMeansIrisPMML("KMeansIris.pmml").getModel()

  val irisTransformationFunc = (
    sepal_length: Double,
    sepal_width: Double,
    petal_length: Double,
    petal_width: Double) => {
      val v = Array[Double](sepal_length, sepal_width, petal_length, petal_width)
      model.predict(v).last.asInstanceOf[String]
    }  
}
