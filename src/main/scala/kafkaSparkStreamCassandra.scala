import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.SomeColumns
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

object kafkaSparkStreamCassandra {
  
  def updateByDateItem(countByDepartment: DStream[((String,String),Int)]){
    //combine data (existing and stream) into variable
    countByDepartment.print()
    //var testdataD = ""
    //var testdataI = ""
    val joinStream = countByDepartment.transform(eachRDD=>{
      if(!eachRDD.isEmpty()){
        val sqlContext = SQLContext.getOrCreate(eachRDD.sparkContext)
        import sqlContext.implicits._
        
        //put to string all date in stream
        val dateList = eachRDD.map{
          case ((date,item),count) => s"'$date'"
        }.distinct().collect().toList.mkString(",")
        //testdataD = dateList
        print("dateList: " + dateList)
        
        //put to string all item in stream
        val itemList = eachRDD.map{
          case ((date,item),count) => s"'$item'"
        }.distinct().collect().toList.mkString(",")
        //testdataI = itemList
        print("itemList: " + itemList)
        
        eachRDD.foreach(println)
        
        //query data in cassandra for data = where date in (date) and item in (item)        
        val stateDF = sqlContext.read.format("org.apache.spark.sql.cassandra")
           .options(Map("table" -> "logdata", "keyspace" -> "test"))
           .load()
        stateDF.registerTempTable("logdata")

        val existingRDD = sqlContext.sql(s"select * from logdata where date in ($dateList) and item in ($itemList)").map{
          case Row(date: String, item: String, count: Int) => ((date,item),count)
        }
        
        //union with this dstream and agg(using reduceBykey)
        eachRDD.union(existingRDD).reduceByKey(_+_)
      }else{
        print("................................EMPTY DSTREAM.........................")
        eachRDD        
      }
    })
    
    //save combined data to cassandra for update
    //print("testdataD: " + testdataD)
    //print("testdataI: " + testdataI)
    joinStream.print()
    joinStream.map({
      case((date,item),count) => (date,item,count)
    }).saveToCassandra("test","logdata",SomeColumns("date","item","count"))
    
  }
  
  
  def main(args: Array[String]) {
        
    val sparkConf = new SparkConf().setAppName("DepartmentCount").setMaster("local")
    
    val topicSet = "flumeToKafka112817".split(",").toSet
    
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "quickstart.cloudera:9092")
    
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    
    
    
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    
    val lines = messages.map(_._2)
    
    val linesFiltered = lines.filter(rec => rec.contains("GET /department/"))
    
    val countByDepartment = linesFiltered.map(rec => ((rec.split(" ")(3).replace("[","").split(":")(0),rec.split(" ")(6).split("/")(2)),1)).reduceByKey(_ + _)
    
    countByDepartment.saveAsTextFiles(args(0))
   // countByDepartment.map(r => (r._1._1,r._1._2,r._2)).saveToCassandra("test", "logdata", SomeColumns("date","item","count"))
    updateByDateItem(countByDepartment)
       
    ssc.start()
    
    ssc.awaitTermination()
    
    
  }
}