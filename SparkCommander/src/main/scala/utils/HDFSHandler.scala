package utils

import java.io.{InputStream, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/*
 * Contributors:
 *  - Valentin Rutz
 */

/**
 * Created by Valentin on 21/04/15.
 */
class HDFSHandler(conf: Configuration) {
  private val hdfs = FileSystem.get(conf)

  /**
   * Creates a folder on HDFS
   * @param folderPath the path where we want to create the folder
   */
  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!hdfs.exists(path)) {
      hdfs.mkdirs(path)
    }
  }

  /**
   * check if a path exist
   * @param path the path to check
   * @return whether the path exists or not
   */
  def exists(path: Path): Boolean = hdfs.exists(path)

  /**
   * Opens the file given as parameter
   * @param path the file to open
   * @return the stream to the file
   */
  def getFile(path: Path): InputStream = {
    require(hdfs.exists(path), s"${path.getName} should exist")
    hdfs.open(path)
  }

  /**
   * Writes to a file on HDFS
   * @param path the file to write to
   * @param data the data to write on the file
   * @return
   */
  def appendToFile(path: Path)(data: List[String]) = {
    if (!hdfs.exists(path)) {
      createFile(path: Path)(data: List[String])
    } else {
      require(hdfs.exists(path), s"${path.getName} should exist")
      val stream = new PrintWriter(hdfs.append(path))
      data.foreach(stream.println)
      stream.close()
    }
  }

  /**
   * Creates a file on HDFS
   * @param path the file to create
   * @param data the data to write to the file
   * @return nothing
   */
  private def createFile(path: Path)(data: List[String]) = {
    require(hdfs.exists(path.getParent), s"${path.getParent.getName} should exist")
    val stream = new PrintWriter(hdfs.create(path))
    data.foreach(stream.println)
    stream.close()
  }

  /**
   * Closes the access to HDFS
   */
  def close() = hdfs.close()

}
