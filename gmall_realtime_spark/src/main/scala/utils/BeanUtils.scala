package utils

import bean.{DauInfo, PageLog}

import java.lang.reflect.Modifier
import scala.util.control.Breaks

object BeanUtils {
  /**
   * Copy properties from srcObj to destObj using reflection
   *
   * @param srcObj
   * @param destObj
   */
  def copyProperties(srcObj: AnyRef, destObj: AnyRef): Unit = {
    if (srcObj == null || destObj == null) {
      return
    }

    val srcFields = srcObj.getClass.getDeclaredFields()

    for (srcField <- srcFields) {

      Breaks.breakable {

        // getter name
        val getMethodName = srcField.getName
        // setter name
        val setMethodName = srcField.getName + "_$eq"

        val getMethod = srcObj.getClass.getDeclaredMethod(getMethodName)

        val setMethod = {
          try {
            destObj.getClass.getDeclaredMethod(setMethodName, srcField.getType)
          } catch {
            case ex: Exception => Breaks.break()
          }
        }

        // do not copy val properties
        val destField = destObj.getClass.getDeclaredField(srcField.getName)
        if (destField.getModifiers.equals(Modifier.FINAL)) {
          Breaks.break()
        }

        // call getMethod on srcObj and pass the result to the setMethod of the destObj
        setMethod.invoke(destObj, getMethod.invoke(srcObj))

      }
    }
  }

  def main(args: Array[String]): Unit = {
    val pageLog = PageLog("mid101", "uid101", "prov101", null, null, null, null, null, null, null, null, null, null, 0L, null, 123456)

    val dauInfo = new DauInfo()
    println("before copy...." + dauInfo)

    copyProperties(pageLog, dauInfo)
    println("after copy...." + dauInfo)

  }

}
