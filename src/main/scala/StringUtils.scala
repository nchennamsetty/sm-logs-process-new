/**
  * Created by NChennamsetty on 4/7/2016.
  */
package org.gogoair.info {

  object StringUtils {

    implicit class StringParseImprovements(s:String)
    {
      def toDoubleOpt:Option[Double] = {
        try {
          Some(s.toDouble)
        }
        catch {
          case e:NumberFormatException => None
        }
      }

      def toIntOpt:Option[Int] ={
          try {
            Some(s.toInt)
          }
          catch {
            case e:NumberFormatException => None
          }

      }
    }


  }

}
