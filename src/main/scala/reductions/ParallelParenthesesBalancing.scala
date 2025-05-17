package reductions

import scala.annotation.*
import org.scalameter.*

object ParallelParenthesesBalancingRunner:

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns := 40,
    Key.exec.maxWarmupRuns := 80,
    Key.exec.benchRuns := 120,
    Key.verbose := false
  ) withWarmer(Warmer.Default())

  def main(args: Array[String]): Unit =
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
//      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface:

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    @tailrec
    def loop(acc: Int, restChars: Array[Char]): Boolean = {
      if (acc < 0 || (restChars.isEmpty && acc !=0)) false
      else
        restChars match {
          case Array() => true
          case arr if arr.head == '(' => loop(acc + 1, arr.tail)
          case arr if arr.head == ')' => loop(acc - 1, arr.tail)
          case arr => loop(acc, arr.tail)
        }
    }

    loop(0, chars)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean =

    @tailrec
    def traverse(idx: Int, until: Int, open: Int, close: Int): (Int, Int) = {
      if (idx >= until) (open, close)
      else {
        if (chars(idx) == '(') traverse(idx + 1, until, open + 1, close)
        else if (chars(idx) == ')') {
          if (open > 0) traverse(idx + 1, until, open - 1, close)
          else traverse(idx + 1, until, open, close + 1)
        } else traverse(idx + 1, until, open, close)
      }
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      val dif = until - from
      if (dif <= threshold) traverse(from, until, 0, 0)
      else {
        val mid = dif / 2
        val ((open1, close1), (open2, close2)) = parallel(reduce(from, from + mid), reduce(from + mid, until))
        val openSum = open1 - close2 + open2
        val closeSum = close1
        (openSum, closeSum)
      }
    }

    reduce(0, chars.length) == (0, 0)

  // For those who want more:
  // Prove that your reduction operator is associative!

