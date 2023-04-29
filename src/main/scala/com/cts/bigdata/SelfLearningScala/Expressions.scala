package com.cts.bigdata.SelfLearningScala

object Expressions extends App {

  //Math expressions are most basic Expressions
  //+ - *  /
  // & | ->(Bitwise AND & OR)
  // ^ ->(Bitwise Exclusive OR)
  // << >> ->(Bitwise Left Shift and right shift)
  // >>> ->(This bitwise operator is specific to scala and this si called right shift with Zero Extension

  val x = 1 + 2 //Expression
  println(x)

  //-----------------------------------------------------------
  //Relational Expressions
  //boolean Expression
  println("== Operator -> " + (1 == x)) // equals Operator       --returns False
  println("!= Operator -> " + (1 != 2)) //Not equal to Operator  -- returns True
  println(">= Operator -> " + (1 > 2)) // greater than Operator  -- returns False
  // Works with increment operator.
  var aVariable = 2
  aVariable += 5 // This also works with -= *=  /=
  println("+= Operator -> " + aVariable)
  aVariable -= 3
  println("-= Operator -> " + aVariable)
  aVariable *= 3
  println("*= Operator -> " + aVariable)
  aVariable /= 4
  println("/= Operator -> " + aVariable)

  //If expression
  val aCondition = true
  val aConditionValue = if (aCondition) 5 else 3 //if expression
  println("Expression Result -> " + aConditionValue)
  println("Condition Inline -> " + (if (aCondition) 5 else 3))

  //Code Blocks - This is kind of function whose last value is assigned to the variable.This starts with {}.
  val aCodeBlock = {
    val y = 2
    val z = y + 1
    if (z > 2) "Hello" else "Bye"
  }
  println(aCodeBlock)

  //Loops
  var i = 0
  while (i < 10) {
    print(i + "\n")
    i += 1
  }
  //Never use loops in scala using while lops since its returns "Unit" which is "Void"
  val aVar = (aVariable = 3) //Example of expression returning Unit
  println("Value of Unit --> " + aVar) //this prints  ()-> which is value of Unit.
}
