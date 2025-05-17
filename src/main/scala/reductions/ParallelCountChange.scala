package reductions

import org.scalameter.*

object ParallelCountChangeRunner:

  @volatile var seqResult = 0

  @volatile var parResult = 0

  val standardConfig = config(
    Key.exec.minWarmupRuns := 20,
    Key.exec.maxWarmupRuns := 40,
    Key.exec.benchRuns := 80,
    Key.verbose := false
  ) withWarmer(Warmer.Default())

  def main(args: Array[String]): Unit =
    val amount = 250
    val coins = List(1, 2, 5, 10, 20, 50)
    val seqtime = standardConfig measure {
      seqResult = ParallelCountChange.countChange(amount, coins)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential count time: $seqtime")

    def measureParallelCountChange(threshold: => ParallelCountChange.Threshold): Unit = try
      val fjtime = standardConfig measure {
        parResult = ParallelCountChange.parCountChange(amount, coins, threshold)
      }
      println(s"parallel result = $parResult")
      println(s"parallel count time: $fjtime")
      println(s"speedup: ${seqtime.value / fjtime.value}")
    catch
      case e: NotImplementedError =>
        println("Not implemented.")

    println("\n# Using moneyThreshold\n")
    measureParallelCountChange(ParallelCountChange.moneyThreshold(amount))
    println("\n# Using totalCoinsThreshold\n")
    measureParallelCountChange(ParallelCountChange.totalCoinsThreshold(coins.length))
    println("\n# Using combinedThreshold\n")
    measureParallelCountChange(ParallelCountChange.combinedThreshold(amount, coins))

object ParallelCountChange extends ParallelCountChangeInterface:

  /** Returns the number of ways change can be made from the specified list of
   *  coins for the specified amount of money.
   */
  def countChange(money: Int, coins: List[Int]): Int = (money, coins) match {
    case (0, _) => 1
    case (m, _) if m < 0 => 0
    case (_, Nil) => 0
    case (restMoney, ::(head, next)) =>
      val f = countChange(money - head, coins)
      val s = countChange(money, next)
      f + s
  }

  type Threshold = (Int, List[Int]) => Boolean

  /** In parallel, counts the number of ways change can be made from the
   *  specified list of coins for the specified amount of money.
   */
  def parCountChange(money: Int, coins: List[Int], threshold: Threshold): Int = (money, coins) match {
    case (0, _) => 1
    case (m, _) if m < 0 => 0
    case (_, Nil) => 0
    case (restMoney, ::(head, next)) =>
      if (threshold(money, coins)) {
        val f = countChange(money - head, coins)
        val s = countChange(money, next)
        f + s
      } else {
        val (f, s) = parallel(parCountChange(money - head, coins, threshold), parCountChange(money, next, threshold))
        f + s
      }
  }

  /** Threshold heuristic based on the starting money. */
  def moneyThreshold(startingMoney: Int): Threshold =
    (moneyAmount:Int, coins:List[Int]) => if (startingMoney * 2 / 3 >= moneyAmount) true else false

  /** Threshold heuristic based on the total number of initial coins. */
  def totalCoinsThreshold(totalCoins: Int): Threshold =
    (moneyAmount: Int, coins: List[Int]) => if (totalCoins * 2 / 3 >= coins.length) true else false

  /** Threshold heuristic based on the starting money and the initial list of coins. */
  def combinedThreshold(startingMoney: Int, allCoins: List[Int]): Threshold =
    (moneyAmount: Int, coins: List[Int]) => if (startingMoney * allCoins.length / 2 >= moneyAmount * coins.length) true else false
