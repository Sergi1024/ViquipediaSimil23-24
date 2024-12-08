package main

import scala.io.Source
// FREQUÈNCIES DE PARAULES
object primeraPart extends App{
  // Funció que normalitza el text, compta les paraules i calcula les freqüències
  def freq(text: String): List[(String, Int)] = {
    //Normalitzem eliminant caràcters especials
    text.toLowerCase.replaceAll("[^a-zà-ÿ]", " ")
      //Traiem els espais en blanc
      .split("\\s+").filter(_.nonEmpty)
      //Agrupem les paraules iguals en un map
      .groupBy(identity)
      //Apliquem length a cada llista del map i ho retornem en forma de llista de l'estil: paraula,numAparicions
      .mapValues(_.length).toList
      //Retornem la llista ordenada descendentment pel nombre d'ocurrències
      .sortBy { case (_, count) => -count }
  }

  //Llegim el text del fitxer
  val text = {
    val source = Source.fromFile("C:/Users/Sergi/Downloads/PrimeraPart_PabloPozo-SergiLopez/ViquipediaSimil23-24-main/primeraPartPractica/pg11.txt")
    try {
      // Passem el text a un sol string separat per espais
      source.getLines().mkString(" ")
    } finally {
      source.close()
    }
  }

  var frequencies = freq(text)
  var totalWords = frequencies.map { case (_, count) => count }.sum
  var uniqueWords = frequencies.size
  var freq10 = frequencies.take(10)

  print(s"Num de paraules: $totalWords    Diferents: $uniqueWords")
  println("Paraules Ocurrències Freqüència")
  println("-------------------------------")
  freq10.foreach {
    case (word, count) =>
      val relativeFreq = count.toDouble * 100 / totalWords
      println(f"$word: $count $relativeFreq%.2f")
  }


  //SENSE STOP-WORDS

  def nonstopfreq(text: String, stopWords: Set[String]): List[(String, Int)] = {
    //Normalitzem eliminant caràcters especials
    text.toLowerCase.replaceAll("[^a-zà-ÿ]", " ")
      //Traiem els espais en blanc i les paraules que apareixen a stopwords
      .split("\\s+").filter(word => word.nonEmpty && !stopWords.contains(word))
      //Agrupem les paraules iguals en un map
      .groupBy(identity)
      //Apliquem length a cada llista del map i ho retornem en forma de llista de l'estil: paraula,numAparicions
      .mapValues(_.length).toList
      //Retornem la llista ordenada descendentment pel nombre d'ocurrències
      .sortBy { case (_, count) => -count }
  }

  //Llegim el fitxer de nonstopwords
  var stopwords = {
    val source = Source.fromFile("C:/Users/Sergi/Downloads/PrimeraPart_PabloPozo-SergiLopez/ViquipediaSimil23-24-main/primeraPartPractica/english-stop.txt")
    try {
      // Eliminem els espais de cada linia i passem les paraules a un set
      source.getLines().map(_.trim).toSet
    } finally {
      source.close()
    }
  }


  frequencies = nonstopfreq(text, stopwords)
  totalWords = frequencies.map { case (_, count) => count }.sum
  uniqueWords = frequencies.size
  freq10 = frequencies.take(10)

  print(s"Num de paraules: $totalWords    Diferents: $uniqueWords")
  println("Paraules Ocurrències Freqüència")
  println("-------------------------------")
  freq10.foreach {
    case (word, count) =>
      val relativeFreq = count.toDouble * 100 / totalWords
      println(f"$word: $count $relativeFreq%.2f")
  }


  // DISTRIBUCIÓ DE PARULES

  def paraulafreqfreq(text: String): (List[(Int, Int)], List[(Int, Int)]) = {
    // Apliquem la funció freq i retornem totes les freqüències de les paraules
    val freqCounts = freq(text).map { case (_, count) => count }
    val freqOfFreqs = freqCounts
      // Agrupem les paraules iguales en un map
      .groupBy(identity)
      //Apliquem length a cada llista del map i ho retornem en forma de llista de l'estil: paraula,numAparicions
      .mapValues(_.length).toList
      //Retornem la llista ordenada descendentment pel nombre d'ocurrències
      .sortBy { case (_, count) => -count }
    // Retornem les 10 paraules més freqüents i les 5 menys freqüents
    (freqOfFreqs.take(10), freqOfFreqs.takeRight(5))
  }

  val (l1, l2) = paraulafreqfreq(text)

  println("Les 10 frequencies mes frequents:")
  l1.foreach {
    case (freq, count) =>
      println(f"$count paraules apareixen $freq vegades")
  }

  println("\nLes 5 frequencies menys frequents:")
  l2.foreach {
    case (freq, count) =>
      println(f"$count paraules apareixen $freq vegades")
  }

  // NGRAMES


  def ngrams(text: String, n: Int): List[(String, Int)] = {
    //Normalitzem eliminant caràcters especials
    text.toLowerCase.replaceAll("[^a-zà-ÿ]", " ")
      //Traiem els espais en blanc
      .split("\\s+").filter(_.nonEmpty)
      // Totes les subllistes de n paraules consecutives
      .sliding(n)
      // Passem cada subllista a un string amb les n paraules consecutives
      .map(_.mkString(" ")).toList
      //Agrupem les subllistes iguales en un map
      .groupBy(identity)
      //Apliquem length a cada subllista del map i ho retornem en forma de llista de l'estil: paraula,numAparicions
      .mapValues(_.length).toList
      //Retornem la llista ordenada descendentment pel nombre d'ocurrències
      .sortBy { case (_, count) => -count }
  }

  ngrams(text,3).take(10).foreach {
    case (seq, count) =>
      println(f"$seq $count")
  }


  // VECTOR SPACE MODEL

  def cosinesim(text1: String, text2: String): Double = {
    val stopwords = {
      val source = Source.fromFile("C:/Users/Sergi/Downloads/PrimeraPart_PabloPozo-SergiLopez/ViquipediaSimil23-24-main/primeraPartPractica/english-stop.txt")
      try {
        // Eliminem els espais de cada linia i passem les paraules a un set
        source.getLines().map(_.trim).toSet
      } finally {
        source.close()
      }
    }
    // Obtenim la llista de parelles (paraula, frequencia) sense les stopwords
    val freq1 = nonstopfreq(text1, stopwords).toMap
    val freq2 = nonstopfreq(text2, stopwords).toMap
    // Vector amb totes les paraules dels dos strings
    val allWords = (freq1.keySet ++ freq2.keySet).toList
    // Si la paraula esta a freq1 retorna el seu valor en el map (frequencia), altrament 0
    // i el mateix amb freq2
    val (weights1, weights2) = allWords.map {
      word => (freq1.getOrElse(word, 0).toDouble,
        freq2.getOrElse(word, 0).toDouble)
    }.unzip
    // Producte dels vectors a·b
    val prod = weights1.zip(weights2).map { case (x, y) => x * y }.sum
    // Part del denominador
    val magnitude1 = math.sqrt(weights1.map(x => x * x).sum)
    val magnitude2 = math.sqrt(weights2.map(x => x * x).sum)

    if (magnitude1 == 0 || magnitude2 == 0) 0.0 else prod / (magnitude1 * magnitude2)
  }

  val text2 = {
    val source = Source.fromFile("C:/Users/Sergi/Downloads/PrimeraPart_PabloPozo-SergiLopez/ViquipediaSimil23-24-main/primeraPartPractica/pg12.txt")
    try {
      // Passem el text a un sol string separat per espais
      source.getLines().mkString(" ")
    } finally {
      source.close()
    }
  }

  val text3 = {
    val source = Source.fromFile("C:/Users/Sergi/Downloads/PrimeraPart_PabloPozo-SergiLopez/ViquipediaSimil23-24-main/primeraPartPractica/pg11-net.txt")
    try {
      // Passem el text a un sol string separat per espais
      source.getLines().mkString(" ")
    } finally {
      source.close()
    }
  }
  val text4 = {
    val source = Source.fromFile("C:/Users/Sergi/Downloads/PrimeraPart_PabloPozo-SergiLopez/ViquipediaSimil23-24-main/primeraPartPractica/pg12-net.txt")
    try {
      // Passem el text a un sol string separat per espais
      source.getLines().mkString(" ")
    } finally {
      source.close()
    }
  }
  val text5 = {
    val source = Source.fromFile("C:/Users/Sergi/Downloads/PrimeraPart_PabloPozo-SergiLopez/ViquipediaSimil23-24-main/primeraPartPractica/pg74.txt")
    try {
      // Passem el text a un sol string separat per espais
      source.getLines().mkString(" ")
    } finally {
      source.close()
    }
  }
  val text6 = {
    val source = Source.fromFile("C:/Users/Sergi/Downloads/PrimeraPart_PabloPozo-SergiLopez/ViquipediaSimil23-24-main/primeraPartPractica/pg74-net.txt")
    try {
      // Passem el text a un sol string separat per espais
      source.getLines().mkString(" ")
    } finally {
      source.close()
    }
  }
  val text7 = {
    val source = Source.fromFile("C:/Users/Sergi/Downloads/PrimeraPart_PabloPozo-SergiLopez/ViquipediaSimil23-24-main/primeraPartPractica/pg2500.txt")
    try {
      // Passem el text a un sol string separat per espais
      source.getLines().mkString(" ")
    } finally {
      source.close()
    }
  }
  val text8 = {
    val source = Source.fromFile("C:/Users/Sergi/Downloads/PrimeraPart_PabloPozo-SergiLopez/ViquipediaSimil23-24-main/primeraPartPractica/pg2500-net.txt")
    try {
      // Passem el text a un sol string separat per espais
      source.getLines().mkString(" ")
    } finally {
      source.close()
    }
  }

  var similitud = cosinesim(text,text2)
  println(f"pg11.txt - pg12.txt $similitud%.4f")

  similitud = cosinesim(text,text3)
  println(f"pg11.txt - pg11-net.txt $similitud%.4f")

  similitud = cosinesim(text2,text4)
  println(f"pg12.txt - pg12-net.txt $similitud%.4f")

  similitud = cosinesim(text5,text6)
  println(f"pg74.txt - pg74-net.txt $similitud%.4f")

  similitud = cosinesim(text7,text8)
  println(f"pg2500.txt - pg2500-net.txt $similitud%.4f")

  // Mirem per ngrames
  similitud = cosinesim(ngrams(text,2).map { case (word, _) => word }.toString(),ngrams(text2,2).map { case (word, _) => word }.toString())
  println(f"pg11 - pg12 amb digrames $similitud%.4f")

  similitud = cosinesim(ngrams(text,3).map { case (word, _) => word }.toString(),ngrams(text2,3).map { case (word, _) => word }.toString())
  println(f"pg11 - pg12 amb trigrames $similitud%.4f")

  similitud = cosinesim(ngrams(text,2).map { case (word, _) => word }.toString(),ngrams(text3,2).map { case (word, _) => word }.toString())
  println(f"pg11 - pg11-net amb digrames $similitud%.4f")

  similitud = cosinesim(ngrams(text2,3).map { case (word, _) => word }.toString(),ngrams(text4,3).map { case (word, _) => word }.toString())
  println(f"pg12 - pg12-net amb trigrames $similitud%.4f")

}






