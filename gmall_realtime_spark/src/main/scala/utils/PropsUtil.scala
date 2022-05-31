package utils

import java.util.ResourceBundle

/**
 * Read and process config.properties
 */
object PropsUtil {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  /**
   *
   * @param propKey
   * @return value for propKey
   */
  def apply(propKey: String): String = {
    bundle.getString(propKey)
  }

  /**
   * Test code
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    println(PropsUtil(Config.KAFKA_BOOTSTRAP_SERVERS))
  }
}
