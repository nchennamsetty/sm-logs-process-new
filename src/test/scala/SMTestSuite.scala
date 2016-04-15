/**
  * Created by nchennamsetty on 4/13/2016.
  */

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark._
import org.gogoair.info._
import java.io.File
import org.apache.commons.io.FileUtils

class SMTestSuite extends FunSuite with BeforeAndAfterAll  {



 override def beforeAll() {

   val winutilsDir = new File(".\\src\\test\\resources\\lib\\winutils")

   System.setProperty("hadoop.home.dir", winutilsDir.getAbsolutePath)
   println(System.getProperty("hadoop.home.dir"))

   val conf = new SparkConf().setAppName("SM Test").setMaster("local[1]")
   val sc = new SparkContext(conf)
   val outFilePath:String = ".\\src\\test\\resources\\out"
   val outFile = new File(outFilePath)
   FileUtils.deleteDirectory(outFile)

   SMLogProcessor.runSMSLaProcess(sc, ".\\src\\test\\resources\\sm_input\\", outFilePath)
   sc.stop()

 }


  test("SM SLA Log processor must transform SM input data correctly "){
    //TODO:Fix this to smart-compare content
    val actual = new File(".\\src\\test\\resources\\out\\part-00000")
    val expected = new File(".\\src\\test\\resources\\out\\part-00000")
    assert(FileUtils.contentEquals(actual, expected))


  }



}
