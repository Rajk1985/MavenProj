val x =99                                                                        //x: Int = 99
  x*x                                                                              //res0: Int = 9801
  val y = 888.8                                                                    //y: Double = 888.8

  var a =555                                                                       //a: Int = 555
  a =111                                                                           //a: Int = 111

  //Any name must start with Character but not with number
  //val 7abc = 999 <-- Is wrong
  val abc7 = 999                                                                   //abc7: Int = 999

  //after "." anything is considered as methdd.
  //val x.y = 7 <-- You get error
  val x,y = 44                                                                     //x: Int = 44
  //y: Int = 44
  //special chararters can be escaped by ``
  val `a.b` = 99                                                                   //a.b: Int = 99

  //Anyting within double quotes considered as String
  val t = "333"                                                                   //t: String = 333

  //\n is new line character
  //msg: String =
  val msg = "Hi Am raj, \nI live in chennai\n"                                     //"Hi Am raj,
  //I live in chennai
  //"

  //""" is used for multi line character                                           //qry: String =
  val qry=                                                                         //"Select case age >0 and age < 20  from end //2
    """Select case age >0 and age < 20  from end                                   //where salary > 30 //3
      |where salary > 30                                                           //and age = 40 //4
      |and age = 40                                                                //"
      |""".stripMargin

  //String interpolation / substituion                                             //name: String = Raj
  val name = "Raj"                                                                 //age: Int = 30
  val age = 30                                                                     //
  val msg =s"hi my name is  $name \n and my age is $age and my age after 10 year will be ${age + 10}" //msg: String =
  //hi my name is  Raj
  //and my age is 30 and my age after 10 year will be 40


  //Collection of same datatype elements is called array
  //Arrays are mutable means it can be changed
  //Array datatype of element cannot be changed but data can be

  val arr = Array("raj","venkat")                                                  //arr: Array[String] = Array(raj, venkat)
  arr(0) // Element at 0 index                                                     //res1: String = raj
  arr(0) ="modi"  // Element is being replaced
  arr                                                                              //res3: Array[String] = Array(modi, venkat)

  arr.length//putting just . infront of array will suggest methods that can be used //res4: Int = 2

  //Generate array
  val num = 1 to 20 toArray                                                        //num: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)

  //Remove element at regualr interval here by 2
  val nums = 1 to 20 by 2 toArray                                                  //nums: Array[Int] = Array(1, 3, 5, 7, 9, 11, 13, 15, 17, 19)

  // Tuple is used when multiple datatypes
  var tup=("AB",32,"mas")                                                          //tup: (String, Int, String) = (AB,32,mas)
  tup._1                                                                           //res5: String = AB
  tup._2                                                                           //res6: Int = 32
  if (tup._2 > 18 )" You are major" else "minor"                                   //res7: String = " You are major"

  //Expression
  val blk = {                                                                      //blk: Int = 2
    val x=4
    val y = 8
    y/x
  }

  val blk={                                                                        //blk: String = venuraj
    val fname = "venu"
    val lname = "raj"
    fname + lname
  }

  //List is same as array but its values are immutable(cannot changed)

  val lst = 1 to 20 toList                                                         //lst: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)

  //lst(2)=5  <-- return error

  //Sequence is a ollection of tuple. But in the form of List. this is used to create dataset
  val se =Seq (("raj",30,"mas"),("venu",44,"hyd"))                                 //se: Seq[(String, Int, String)] = List((raj,30,mas), (venu,44,hyd))

