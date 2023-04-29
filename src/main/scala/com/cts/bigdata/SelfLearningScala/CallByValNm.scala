package com.cts.bigdata.SelfLearningScala

object CallByValNm extends App{

  def calledbyVal(x: Long): Unit = { //Used Long datatype since System.nanoTime() retruns a Long.
    println("by Value: " + x) // Here Value of nanoTime() is evaluted once and passed
    println("by Value: " + x)
  }

  def calledbyName(x: Long): Unit = {
    println("by name: " + x)  // Here Value of nanoTime() is evaluted all the time its called
    println("by name: " + x)
  }

  calledbyName(System.nanoTime())
  calledbyVal(System.nanoTime())

}


