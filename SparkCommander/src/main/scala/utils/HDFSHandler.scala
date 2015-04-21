package utils

import java.io.{InputStream, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Created by Valentin on 21/04/15.
 */
class HDFSHandler(conf: Configuration) {
  private val hdfs = FileSystem.get(conf)

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!hdfs.exists(path)) {
      hdfs.mkdirs(path)
    }
  }

  def getFile(path: Path): InputStream = {
    require(hdfs.exists(path))
    hdfs.open(path)
  }

  def appendToFile(path: Path)(data: List[String]) = {
    require(hdfs.exists(path))
    val stream = new PrintWriter(hdfs.append(path))
    data.foreach(stream.println)
    stream.close()
  }

  def createFile(path: Path)(data: String) = {
    require(hdfs.exists(path))
    val stream = new PrintWriter(hdfs.create(path))
    data.foreach(stream.println)
    stream.close()
  }

}
