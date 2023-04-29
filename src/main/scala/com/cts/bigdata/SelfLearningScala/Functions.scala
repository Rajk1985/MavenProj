package com.cts.bigdata.SelfLearningScala

object Functions extends App {
  //A function is defined using "def" keyword. Below function has 2 input and return type of function is String

  def aFunction (a: String,b: Int): String =
    a + "  "+ b
  print("Function Usgae -> "+  aFunction("Hello", 3) + "\n")

  //A function can also be a code block. Function Overloading with a code block
  def aFunction (a: String,b: Int ,c: Int): String = {
    a + "  "+ b * c
  }
  print("Function Usgae Using code block -> "+  aFunction("Hello", 3 ,2) + "\n")

  //A parameterless function
  def aPramlessFunc (): Int = 42
  println("A parameterless function call-> "+ aPramlessFunc())
  println("A parameterless function can also be called with name -> "+ aPramlessFunc + "\n")

  //Usage of Recursion using "function" in place of "Loop" as its not good practice to use Loops in functional programming
  //Its mandatory have a return type of recursive function. Unlike normal function, a recursive function cannot determine
  // the return type its own.
  def aRepeaterfunc(aStr: String, n: Int): String = {
    if (n == 1)
      aStr
    else
      aStr +"\n"+ aRepeaterfunc(aStr, n-1)
  }
  println("Recursion Output -> "+ aRepeaterfunc ("Hello",5))

  // A function defined inside a main function is called Auxiliary Function
  // A code block can be used for defining a values/variables/Functions
  def aBigfunction(n: Int): Int = {
    def aSmallfunction(a: Int, b: Int): Int =  a - b // Example of auxiliary function
    aSmallfunction(n,n-1)
  }
  println( aBigfunction(5))

  //*****************************************
  //Practice Example Greeting
  def nameage(nm: String , age: Int) = {
    "Hi,My name is "+ nm +" and I am "+ age+" years old"
  }
  println(nameage("Raj",36))

  //*****************************************
  //Factorial of a number
  def factorial(n: Int): Int = {
    if (n <= 0)
      1
    else
      n * factorial(n-1)
  }
  println("Factorial is "+ factorial(5))

  //*****************************************
  //Fibonacci Number
  def fibonaci(n: Int): Int = {
    if ((n==1) | (n==2))
      1
    else
      fibonaci(n-1) + fibonaci(n-2)
  }
  println("fibonaci is "+ fibonaci(8))

  //*****************************************
  // Prime number
  def Isprime (n: Int): Boolean = {
    if (n % 2 != 0 | n <= 1)
      true
    else
      false
  }
  println("Is number prime ? -> " + Isprime(4))

}



