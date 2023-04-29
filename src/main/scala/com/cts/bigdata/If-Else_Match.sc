val name = "Ravi"                                                                //name: String = Ravi
val age = 15                                                                     //age: Int = 15

if (age > 18) s"Hi your name is $name and your age is $age "                     //res0: String = "Your age is less "
else "Your age is less "

//Nested If-Else Block
if (age>0 && age < 13) "j&j product free"                                        //res1: String = Mobile 60% off
else if (age >13 && age <25) "Mobile 60% off"
else if (age > 25 && age < 51) "Medical insurance free"
else "No offer"

//Create function with def key work
//You can implement your logic in a fuction to hide your logic and reuse it
def offer(age: Int) = {                                                          //offer: (age: Int)String
  if (age>0 && age < 13) "j&j product free"
  else if (age >13 && age <25) "Mobile 60% off"
  else if (age > 25 && age < 50) "Medical insurance free"
  else "No offer"
}
offer (19)                                                                       //res2: String = Mobile 60% off

//Match
val name = "ravi"                                                                //name: String = ravi
val age = 30                                                                     //age: Int = 30
val day1 = "Sunday"                                                              //day1: String = Sunday

val res = name match {                                                           //res: String = Hello ravi must complete report task
  case "ravi" => s"Hello $name must complete report task"
  case "vemu" => s"Hello $name must complete development task"
  case "ramu" => s"Hello $name complete development task"
  case _ => s"Hello $name you are not member of the project"
}

def emptask(name: String)=name match {                                           //emptask: (name: String)String
  case "ravi" => s"Hello $name must complete report task"
  case "vemu" => s"Hello $name must complete development task"
  case "ramu" => s"Hello $name complete development task"
  case _ => s"Hello $name you are not member of the project"
}

emptask("vemu")                                                                  //res0: String = Hello vemu must complete development task
emptask("ravi")                                                                  //res1: String = Hello ravi must complete report task

//Used Case
val day = "Sunday"                                                               //day: String = Sunday
val offr = day.toLowerCase  match {                                              //offr: String = 50% off
  case "sun"|"sunday"| "sat" |"Saturday" => "50% off"
  case "mon"|"Monday"|"wed"|"Wednesday"  => "60% off "
  case _ => "No offer"
}
//Function with Match
def offer (day: String): String = day.toLowerCase  match {                       //offer: (day: String)String
  case "sun"|"sunday"| "sat" |"Saturday" => "50% off"
  case "mon"|"Monday"|"wed"|"Wednesday"  => "60% off "
  case _ => "No offer"
}
offer("mon")                                                                     //res2: String = "60% off "

//Range can be compared using safegurad
val age = 30                                                                     //age: Int = 30
def agerange(age: Int) = age match {                                             //agerange: (age: Int)String
  case x if (x > 0 && x < 18) => "j& J free"
  case x if (x >= 19 && x < 30) => "Mobile free"
  case age if (age >= 30 && age < 50) => "Laptop Free"
  case _ => "Nothing free"
}
agerange(30)                                                                     //res3: String = Laptop Free

//Used Case
//val det=("venu",32,"m")

def genageoff (det: (String, Int,String)) = det match                            //genageoff: (det: (String, Int, String))String
{
  case (n,a,g ) if (a>10 && g=="f" && a<35  ) => s"Hi $n you will get liptik free"
  case(n,a,g) if (a>18 && g=="m" ) => s"Hi $n you will get laptop"
  case _ => "No offer"
}

genageoff("venu",32,"m")                                                         //res4: String = Hi venu you will get laptop
