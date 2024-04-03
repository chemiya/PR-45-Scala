   // val imutables
   val hello: String = "Hola!"

   // var mutables
   var helloThere: String = hello
   helloThere = hello + " There!"
   println(helloThere)

   val immutableHelloThere = hello + " There"
   println(immutableHelloThere)

   // tipos
   val numberOne: Int = 1
   val truth: Boolean = true
   val letterA: Char = 'a'
   val pi: Double = 3.14159265
   val piSinglePrecision: Float = 3.14159265f
   val bigNumber: Long = 123456789
   val smallNumber: Byte = 127


   //imprimir
   println("Here is a mess: " + numberOne + truth + letterA + pi + bigNumber)

   println(f"Pi is about $piSinglePrecision%.3f")
   println(f"Zero padding on the left: $numberOne%05d")

   println(s"I can use the s prefix to use variables like $numberOne $truth $letterA")

   println(s"The s prefix isn't limited to variables; I can include any expression. Like ${1+2}")

   val theUltimateAnswer: String = "To life, the universe, and everything is 42."
   val pattern = """.* ([\d]+).*""".r
   val pattern(answerString) = theUltimateAnswer
   val answer = answerString.toInt
   println(answer)

   // operadores logicos
   val isGreater = 1 > 2
   val isLesser = 1 < 2
   val impossible = isGreater & isLesser
   val impossibleBetter = isGreater && isLesser
   val anotherWay = isGreater || isLesser

   val picard: String = "Picard"
   val bestCaptain: String = "Picard"
   val isBest: Boolean = picard == bestCaptain


