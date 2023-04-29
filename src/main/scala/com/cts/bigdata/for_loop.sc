//If you want to process collection of elememt you can use for loop.

val name = Array("Venu","Ramu","Suraj","hari")

for (x<-name) println(x +"\n")  //Here x is a temp variable and name is collection of names

val num = 1 to 20 toArray

// Yield is being used to do some processing
val res = for (x<- num) yield (x+x)

//If usage. Only IF allowed else is not possible in for loop
val res = for (x<- num if x%2==0 ) yield (x*x)

val res = for (x<- num if x%2==0 ) yield x match {
 case a if(a%2==0) => x*x
     case _ => x*x*x
}

//Processing 2 Arrays togther
val num1 = 1 to 10 toArray
val num2 = 1 to 20 toArray

for (x<-num1;x<-num2) println(x)

for (x<-num1;y<-num2) println(s"$x * $y = ${x*y}")

// To comment a block “shift+ctrl ?” to comment code in Intellij

// Used case for loop
def table(x:Int) = {
  val num = 1 to  10 toArray ;
  for(a<-num) println(s"$x * $a = ${x*a}")
}
table(5)

//Defining a function with Array as input
def mularr (num:Array[Int],num1: Array[Int]) ={
  for (x<-num1;y<-num2) println(s"$x * $y = ${x*y}")
}


val num = 1 to 10 toArray
val num1 = 1 to 10 toArray

mularr(num,num1)


//Recursive Function – A function calling itself or a function within itself
//Recursive function must retrun a datatype
def fact(a: Int): Int = a match {
 case x if (x<=0) => 1
     case _ => a * fact(a-1)
     }

fact(5)


