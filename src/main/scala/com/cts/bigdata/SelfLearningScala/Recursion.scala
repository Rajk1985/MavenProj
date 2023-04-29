package SelfLearningScala


import SelfLearningScala.Functions.factorial

import scala.annotation.tailrec

object Recursion extends App {
  //Factorial of a number
  def factorial(n: Int): Int =
    if (n <= 1)
      1
    else
    {
      println("Compute factorial of " + n + " I need factorial of "+ (n-1))
      val result=  n * factorial(n-1)
      println ("Computed factorial of " + n)
      result
    }
  println(factorial(100))
  // println(factorial(5000)) //<== Will lead to Stackoverflow error

  @tailrec //Annotation to tell compiler that the function is tail recursive
  def anotherFactorial(x: Int, Accumulator: BigInt): BigInt =
    if(x <=1 ) Accumulator
    else anotherFactorial(x-1, x * Accumulator) //Tail Recursion uses recursive call as Last Expression
  // anotherFacatorial(10)= anotherFacatorial(10,1)
  //                      = anotherFacatorial(9, 10 *1 )
  //                      = anotherFacatorial(8, 9 * 10 * 1 )
  //                      = anotherFacatorial(7 , 8 * 9 * 10  *1 )

  println(anotherFactorial(5000,1))

  //Example of tailrecvfunctions
  @tailrec
  def concatstring(strg: String , n: Int, accum: String): String =
    if (n<=0) accum
    else concatstring (strg ,n-1, strg + accum)
  println(concatstring("Hello  \n",5,""))

  //is prime using tail recursion
  def isprime (n: Int): Boolean = {
    @tailrec
    def isPrimetailrec(t: Int ,isStillprime: Boolean): Boolean=
      if (!isStillprime) false
      else if (t <= 1 ) true
      else isPrimetailrec(t-1,n % t != 0 && isStillprime)

    isPrimetailrec(n/3,true)
  }
  println(isprime(3))

}
