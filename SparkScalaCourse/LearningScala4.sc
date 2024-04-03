// Data structures

// Tuples
// imutable

val captainStuff = ("Picard", "Enterprise-D", "NCC-1701-D")
println(captainStuff)

// acceso
println(captainStuff._1)
println(captainStuff._2)
println(captainStuff._3)

val picardsShip = "Picard" -> "Enterprise-D"
println(picardsShip._2)

val aBunchOfStuff = ("Kirk", 1964, true)

// Listas
val shipList = List("Enterprise", "Defiant", "Voyager", "Deep Space Nine")


//acceso
println(shipList(1))


println(shipList.head)
println(shipList.tail)


//recorrer
for (ship <- shipList) {println(ship)}


//dar vuelta
val backwardShips = shipList.map( (ship: String) => {ship.reverse})
for (ship <- backwardShips) {println(ship)}

// reduce para acumular
val numberList = List(1, 2, 3, 4,5 )
val sum = numberList.reduce( (x: Int, y: Int) => x + y)
println(sum)

// filter para filtrar
val iHateFives = numberList.filter( (x: Int) => x != 5)

val iHateThrees = numberList.filter(_ != 3)

// Concatenar
val moreNumbers = List(6,7,8)
val lotsOfNumbers = numberList ++ moreNumbers


//otras operaciones
val reversed = numberList.reverse
val sorted = reversed.sorted
val lotsOfDuplicates = numberList ++ numberList
val distinctValues = lotsOfDuplicates.distinct
val maxValue = numberList.max
val total = numberList.sum
val hasThree = iHateThrees.contains(3)

// MApas
val shipMap = Map("Kirk" -> "Enterprise", "Picard" -> "Enterprise-D", "Sisko" -> "Deep Space Nine", "Janeway" -> "Voyager")
println(shipMap("Janeway"))
println(shipMap.contains("Archer"))
val archersShip = util.Try(shipMap("Archer")) getOrElse "Unknown"
println(archersShip)

