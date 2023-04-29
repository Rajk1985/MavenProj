package SelfLearningScala

object ValuesVariableTypes extends App {
  val x: Int = 42
  println(x)
  //Re-assignment of  "val" cannot be done. These act as constant. The types of Val is Optional.
  // Compiler automatically infer types
  // Scala Datatypes
  val aString: String = "hello";
  val aboolean: Boolean = false;
  val achar: Char = 'a';
  val anInt: Int = x;
  val aShort: Short = 4613;
  val aLong: Long = 4352562987L;
  val aFloat: Float = 2.0F;
  val aDouble: Double = 3.14

  println("String -> " + aString)
  println("Boolean ->" + aboolean)
  println("Char ->" + achar)
  println("Int ->" + anInt)
  println("Long ->" + aLong)
  println("Short ->" + aShort)
  println("Float ->" + Float)
  println("Double ->" + aDouble)

  //Variables
  //A variable can be reassigned unlike Val.
  var aVariable: Int = 5
  println("Initial Value ->" +aVariable)
  aVariable = 10
  println("After Reassignment ->" + aVariable)

}
