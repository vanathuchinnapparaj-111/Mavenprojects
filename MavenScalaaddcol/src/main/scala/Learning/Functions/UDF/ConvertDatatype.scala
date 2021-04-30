package Learning.Functions.UDF

object ConvertDatatyp {
  def main(args:Array[String]): Unit ={
    var conObj = new ConvertDatatyp()
    var retVal= conObj.convertType(10,"52.12")
    println("The Return value is:"+retVal)
   // retVal.productIterator.foreach(println)
    val list = List("one","two","three") //find the text length
    val lengths = list.map(_.length)
    println(lengths)

  }

}
private class ConvertDatatyp{
  def convertType(a:Int,b:String){
    var a1=a.asInstanceOf[Long]
    var b1=b.asInstanceOf[String]
    println("a1:"+a1+"b1:"+b1)
    //return (a1,b1)
    var tuple =(a1,b1)
    tuple
  }

}


