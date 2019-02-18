/* solver.scala */
import scala.io._
import scala.collection.mutable.WrappedArray

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame,Dataset,Row,SparkSession}
import org.apache.spark.sql.functions._

import java.io._
import java.lang.Long

case class PWordRow(id: Long, word: String)
case class Phrase(freq: Int, phrase: String)
case class PhraseRow(id: Long, freq: Long, phrase: String)

object MyFunctions {
  def getPerms(
    scrambledWord : String,
    dict: Broadcast[Map[String, Integer]]
  ): Dataset[String] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Get permutations for scrambled word and remove duplicates
    val ds = spark.createDataset(scrambledWord.permutations.toList).distinct()

    // Filter out non-words
    return ds.filter(dict.value.get(_).getOrElse(new Integer(-1)) >= 0)
  }

  def getPword(
    words: Seq[String],
    marks: Array[Seq[Long]],
    pLengths: Array[Long]
  ): String = {
    // Calculate scrambled phrase word
    val builder = StringBuilder.newBuilder
    var i = 0
    var k = 0
    for (p <- 0 to pLengths.length.toInt - 1) {
      var len = pLengths(p).toInt
      for (n <- 0 until len) {
        val nextLetterIndex = marks(i)(k).toInt
        val nextLetter = words(i)(nextLetterIndex)
        builder.append(nextLetter)

        // Move on to "next letter" index in marks
        k += 1
        if (k == marks(i).length.toInt) {
          k = 0
          i += 1
        }
      }
    }
    return builder.toString()
  }

  def getPhrase(
    pword: String,
    pLengths: Array[Long]
  ): Array[String] = {
    var phrase: Array[String] = Array.fill[String](pLengths.length)("")

    // split pword into phrase
    var beg = 0
    for (p <- 0 to pLengths.length.toInt - 1) {
      var len = pLengths(p).toInt
      var word = pword.substring(beg, beg + len)
      phrase(p) = word
      beg += len
    }

    return phrase
  }

  def getPhrase(
    pword: String,
    pLengths: Array[Long],
    dict: Broadcast[Map[String, Integer]]
  ): Option[Phrase] = {
    var phraseFreq = 0 // lower number better

    // split pword into phrase
    var beg = 0
    for (p <- 0 to pLengths.length.toInt - 1) {
      var len = pLengths(p).toInt
      var word = pword.substring(beg, beg + len)
      val freq = dict.value.get(word).getOrElse(new Integer(0))
      if (freq > 0) {
        phraseFreq += freq
      } else {
        // Abort early
        val result: Option[Phrase] = None
        return result
      }
      beg += len
    }

    val result: Option[Phrase] = Some(Phrase(phraseFreq, pword))
    return result
  }

  def getPhrases(
    pwPerms: Dataset[PWordRow],
    pLengths: Array[Long],
    dict: Broadcast[Map[String, Integer]]
  ): Dataset[PhraseRow] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    return pwPerms.flatMap(row => {
      val phrase: Option[Phrase] = MyFunctions.getPhrase(row.word, pLengths, dict)
      if (phrase.isEmpty)
        Seq()
      else
        Seq(PhraseRow(row.id, phrase.get.freq, phrase.get.phrase))
    })
  }

  def getWordsById(
    words: org.apache.spark.sql.DataFrame,
    id: Long
  ): Seq[String] = {
    return words.filter(_.getAs[Long]("id") == id).first().getAs[Seq[String]]("words")
  }
}

class Puzzle(dictFile: String) {
  val spark = SparkSession.builder().getOrCreate()
  var dict = spark.sparkContext.broadcast(readDict())

  def readDict() : Map[String, Integer] = {
    val json = Source.fromFile(dictFile)
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val dictionary = mapper.readValue[Map[String, Integer]](json.reader())
    return dictionary
  }

  def solve(puzzleFile: String) : Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val puzzle = spark.read.json(puzzleFile)

    val scrambledWords = puzzle.select(col("word")).na.drop().map(row => row.getString(0)).collect()

    var wordCombinations = MyFunctions.getPerms(scrambledWords(0), dict).toDF("word")
    wordCombinations = wordCombinations.withColumn("words", array($"word")).drop("word")

    for (i <- 1 until scrambledWords.length) {
      var perms = MyFunctions.getPerms(scrambledWords(i), dict).toDF("word")
      perms = perms.withColumn("words2", array($"word")).drop("word")

      // cartesian product
      perms = wordCombinations.crossJoin(perms)

      wordCombinations = perms.withColumn("words", concat($"words", $"words2")).drop("words2")
    }

    // Add index for keeping track of where best phrase came from.
    wordCombinations = wordCombinations.withColumn("id", monotonically_increasing_id)

    val marks = puzzle.select(col("mark")).na.drop().map(row => row.getSeq[Long](0)).collect()
    val pLengthsWrapped = puzzle.select(col("phrase")).na.drop().first
    val pLengths = pLengthsWrapped(0).asInstanceOf[WrappedArray[Long]].array

    // Get scrambled phrase words
    var pwords: Dataset[PWordRow] = wordCombinations.map(row => {
      val id = row.getAs[Long]("id")
      val words = row.getAs[Seq[String]]("words")
      PWordRow(id, MyFunctions.getPword(words, marks, pLengths))
    })

    // Get permutations of scrambled phrase words
    pwords = pwords.flatMap(row =>
      row.word.permutations.map(PWordRow(row.id, _))
    ) //    ).distinct()

    // Calculate possible phrases
    val phrases: Dataset[PhraseRow] = MyFunctions.getPhrases(pwords, pLengths, dict)
 
    // Reduce the phrases, keeping the lowest freq. Only need one.
    val winner = phrases.reduce((a, b) => if (a.freq < b.freq) a else b)

    val pw = new PrintWriter(new File(puzzleFile + ".result"))

    val phrase = MyFunctions.getPhrase(winner.phrase, pLengths)
    pw.write(s"Phrase: " + phrase.mkString(" ") + s"\n")
    pw.write(s"Words: ")

    val words = MyFunctions.getWordsById(wordCombinations, winner.id)
    words.foreach((w:String) => pw.write(w+" "))
    pw.write(s"\n")
    pw.close
  }
}

object Solver {
  def main(args: Array[String]) {
    val puzzleFile = sys.env("PUZZLE")

    val spark = SparkSession.builder.appName("Solver").getOrCreate()

    val puzzle = new Puzzle("/source/freq_dict.json")
    puzzle.solve(puzzleFile)

    spark.stop()
  }
}
