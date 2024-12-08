import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._
import mapreduce._
import main.primeraPart.cosinesim

object segonaPart extends App {

  val directoryPath = "C:/Users/Sergi/Downloads/PrimeraPart_PabloPozo-SergiLopez/ViquipediaSimil23-24-main/viqui_files_short/"
  val files = ProcessListStrings.getListOfNames(directoryPath)

  // Sistema d'actors
  implicit val timeout: Timeout = Timeout(60.seconds)
  val system = ActorSystem("MapReduceSystem")

  // Funció per calcular el nombre mitjà de referències
  def avgReferences(documents: List[String]): Double = {
    val mapFunc = (file: String, content: List[String]) => {
      val result = ViquipediaParse.parseViquipediaFile(file)
      List("refs" -> result.refs.size)
    }

    val reduceFunc = (key: String, values: List[Int]) => {
      key -> values.sum
    }

    val mapReduce = system.actorOf(
      Props(new MapReduce[String, String, String, Int, Int](
        files.zip(files.map(content => List(content))), mapFunc, reduceFunc))
    )

    val futureResult = mapReduce ? MapReduceCompute()
    val result = Await.result(futureResult, timeout.duration).asInstanceOf[Map[String, Int]]
    result("refs").toDouble / files.size
  }

  val avgRef = avgReferences(files)
  println(f"Nombre mitjà de referències: $avgRef%.2f")

  // Funció de mapeig per al càlcul del PageRank
  def mapPageRank(queryDocs: List[String], links: Map[String, List[String]], d: Double, N: Int): List[(String, Double)] = {
    queryDocs.map { title =>
      val inbound = links.filter(_._2.contains(title)).keys
      val rank = (1 - d) / N + d * inbound.map { prevTitle =>
        val prevRank = 1.0 / N
        prevRank / links(prevTitle).size
      }.sum
      (title, rank) // Retorna el títol amb el seu rank calculat
    }
  }

  // Funció de reducció per al PageRank
  def reducePageRank(key: String, values: List[Double]): (String, Double) = {
    (key, values.sum)
  }

  //PageRank amb MapReduce
  def pageRankWithMapReduce(documents: List[String], links: Map[String, List[String]], query: String, d: Double = 0.85, epsilon: Double = 1e-3, maxSteps: Int = 5): Map[String, Double] = {
    val N = documents.size
    val docs = for { doc <- documents
                     result = ViquipediaParse.parseViquipediaFile(doc)
                     if (result.titol + " " + result.contingut).toLowerCase.contains(query.toLowerCase)
                     } yield result

    val titles = docs.map(_.titol)

    def iteratePageRank(iteration: Int, pr: Map[String, Double]): Map[String, Double] = {
      // Retorna una llista de tuples (String, Double)
      val mapFunc = (file: String, docs: List[String]) => mapPageRank(docs, links, d, N)

      val reduceFunc = (key: String, values: List[Double]) => reducePageRank(key, values)

      // Creem l'actor MapReduce
      val mapReduceActor = system.actorOf(
        Props(new MapReduce[String, String, String, Double, Double](
          titles.zip(titles.map(d => List(d))),
          mapFunc,
          reduceFunc
        ))
      )

      val futureResult = mapReduceActor ? MapReduceCompute()
      val result = Await.result(futureResult, timeout.duration).asInstanceOf[Map[String, Double]]

      // Comprovem si la variació entre iteracions és menor que epsilon o si hem arribat a la t maxima
      val diff = result.map { case (k, v) => math.abs(v - pr.getOrElse(k, 0.0)) }.sum
      if (diff < epsilon || iteration >= maxSteps) {
        result // Si la variació és suficientment petita, retornem el resultat
      } else {
        iteratePageRank(iteration + 1, result) // Continuem amb el seguent instant de temps
      }
    }

    // Iniciem la primera iteració amb un rank inicial per a cada document
    val initialPr = documents.map(_ -> 1.0 / N).toMap
    val finalPr = iteratePageRank(0, initialPr)
    finalPr
  }

  // Funció per trobar pàgines similars mitjançant MapReduce
  def similarPages(queryDocs: List[String], threshold: Double = 0.5): List[(String, String)] = {
    // Preparem les dades d'entrada per als mapeigs
    val docs = queryDocs.map { doc =>
      val result = ViquipediaParse.parseViquipediaFile(doc)
      (doc, result) // Emparellament (fileName, Document)
    }

    val mapFunc = (file: String, docs: List[String]) => {
      val result1 = ViquipediaParse.parseViquipediaFile(file)
      docs.flatMap { doc =>
        val result2 = ViquipediaParse.parseViquipediaFile(doc)
        val sim = cosinesim(result1.contingut, result2.contingut)
        if (sim > threshold && !result1.refs.contains(result2.titol) && !result2.refs.contains(result1.titol))
          // Si la similitud és suficient, tornem la parella de títols
          List((s"${result1.titol} - ${result2.titol}", 1))
        else
          List.empty
      }
    }

    val reduceFunc = (pair: String, counts: List[Int]) => {
      (pair, counts.sum) // Acumulem les ocurrences de pàgines similars
    }

    // Creem l'actor MapReduce
    val mapReduceActor = system.actorOf(
      Props(new MapReduce[String, String, String, Int, Int](
        docs.map { case (file, doc) => (file, files) },
        mapFunc,
        reduceFunc
      ))
    )

    val futureResult = mapReduceActor ? MapReduceCompute()

    val similarResult = Await.result(futureResult, timeout.duration).asInstanceOf[Map[(String, String), Int]]

    similarResult.keys.toList
  }

  // Funció per filtrar documents segons una consulta
  def filterDocs(documents: List[String], query: String): Map[String, List[String]] = {
    val queryDocs = for { doc <- documents
                          result = ViquipediaParse.parseViquipediaFile(doc)
                          if (result.titol + " " + result.contingut).toLowerCase.contains(query.toLowerCase)
                          } yield result

    val titles = queryDocs.map(_.titol)
    val links = queryDocs.map { result =>
      result.titol -> result.refs.filter(titles.contains)
    }.toMap
    links
  }

  // Exemple de càlcul de PageRank per a un conjunt de documents
  val links = filterDocs(files, "Hentai")
  val pagerankResult = pageRankWithMapReduce(files, links, "Hentai")

  pagerankResult.foreach { case (title, rank) =>
    println(f"Títol: $title, PageRank: $rank")
  }

  // Obtenim les pàgines similars
  val similarDocs = similarPages(files)
  similarDocs.foreach(println)

  // Funció per mesurar el temps d'execució d'un bloc de codi
  // Maquina usada: 13th Gen Intel(R) Core(TM) i5-13600KF   3.50 GHz
  def timeMeasurement[A](block: => A): (A, Long) = {
    val startTime = System.nanoTime()
    val result = block
    val endTime = System.nanoTime()
    val elapsedTime = endTime - startTime
    (result, elapsedTime)
  }

  // Funció per executar el MapReduce amb un nombre específic d'actors
  def runMapReduce(numMappers: Int, numReducers: Int): Unit = {
    val avgRef = avgReferences(files)
    println(f"Nombre mitjà de referències: $avgRef%.2f")

    val links = filterDocs(files, "Hentai")
    val pagerankResult = pageRankWithMapReduce(files, links, "Hentai")
    pagerankResult.foreach { case (title, rank) =>
      println(f"Títol: $title, PageRank: $rank")
    }

    val (similarDocs, time) = timeMeasurement(similarPages(files))
    similarDocs.foreach(println)

    println(f"Temps d'execució amb $numMappers mapejadors i $numReducers reductors: ${time / 1e6}%.2f ms")

  }


  val numActorsList = List(1, 4, 10, 20)

  // Executem el MapReduce amb diferents configuracions de mapejadors i reductors
  numActorsList.foreach { numActors =>
    val numMappers = numActors
    val numReducers = numActors
    runMapReduce(numMappers, numReducers)
  }

  // Tanquem el sistema d'actors
  system.terminate()
}
