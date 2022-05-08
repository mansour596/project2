package Alchemy

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter
import HiveDatabase._
import HiveTable._



object Project2 {

  val path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/"
  def main(args: Array[String]): Unit ={
      copyFromLocal()
    
  }

  def copyFromLocal(): Unit = {
    val src = "file:///home/maria_dev/ecommerce.csv"
    val target = path + "ecommerce.csv"
    println(s"Copying local file $src to $target ...")
    
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val localpath = new Path(src)
    val hdfspath = new Path(target)
    
    fs.copyFromLocalFile(localpath, hdfspath)
    println(s"Done copying local file $src to $target ...")

    println("Creating DataBase")
    createHiveDatabase.makeDatabase()

    println("Establishing Table...")
    createHiveTable.makeTable()

  }
}

