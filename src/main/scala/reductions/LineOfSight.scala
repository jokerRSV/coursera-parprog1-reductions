package reductions

import org.scalameter.*
import reductions.Tree.{Leaf, Node}

object LineOfSightRunner:

  val standardConfig = config(
    Key.exec.minWarmupRuns := 40,
    Key.exec.maxWarmupRuns := 80,
    Key.exec.benchRuns := 100,
    Key.verbose := false
  ) withWarmer(Warmer.Default())

  def main(args: Array[String]): Unit =
    val length = 1000000
    val input = (0 until length).map(_ % 100 * 1.0f).toArray
    val output = new Array[Float](length + 1)
    val seqtime = standardConfig measure {
//      LineOfSight.lineOfSight(input, output)
    }
    println(s"sequential time: $seqtime")

    val partime = standardConfig measure {
      LineOfSight.parLineOfSight(input, output, 10000)
    }
    println(s"parallel time: $partime")
    println(s"speedup: ${seqtime.value / partime.value}")

enum Tree(val maxPrevious: Float):
  case Node(left: Tree, right: Tree) extends Tree(left.maxPrevious.max(right.maxPrevious))
  case Leaf(from: Int, until: Int, override val maxPrevious: Float) extends Tree(maxPrevious)

object LineOfSight extends LineOfSightInterface:

  def lineOfSight(input: Array[Float], output: Array[Float]): Unit = {
    val in = input.zipWithIndex
      .map(p => p._1 / p._2)
      .foldLeft(Array.empty[Float]) {
        case (acc@Array(), el) => acc :+ 0
        case (acc, el) =>
          if (acc.last < el) acc :+ el
          else acc :+ acc.last
      }
    System.arraycopy(in, 0, output, 0, input.length)
  }

  /** Traverses the specified part of the array and returns the maximum angle.
   */
  def upsweepSequential(input: Array[Float], from: Int, until: Int): Float =
    input.slice(from, from + until).zip(from until until)
      .map(p => p._1 / p._2)
      .max

  /** Traverses the part of the array starting at `from` and until `end`, and
   *  returns the reduction tree for that part of the array.
   *
   *  The reduction tree is a `Tree.Leaf` if the length of the specified part of the
   *  array is smaller or equal to `threshold`, and a `Tree.Node` otherwise.
   *  If the specified part of the array is longer than `threshold`, then the
   *  work is divided and done recursively in parallel.
   */
  def upsweep(input: Array[Float], from: Int, end: Int, threshold: Int): Tree = {
    val diff = end - from
    if (diff <= threshold) Leaf(from, end, upsweepSequential(input, from, end))
    else {
      val mid = diff / 2
      val (node1, node2) = parallel(upsweep(input, from, from + mid, threshold), upsweep(input, from + mid, end, threshold))
      Node(node1, node2)
    }
  }

  /** Traverses the part of the `input` array starting at `from` and until
   *  `until`, and computes the maximum angle for each entry of the output array,
   *  given the `startingAngle`.
   */
  def downsweepSequential(input: Array[Float], output: Array[Float], startingAngle: Float, from: Int, until: Int): Unit = {
    var max = startingAngle
    (from until until).foreach { i =>
      val element = if (i == 0) 0 else input(i) / i
      if (element > max) max = element
      output(i) = max
    }
  }

  /** Pushes the maximum angle in the prefix of the array to each leaf of the
   *  reduction `tree` in parallel, and then calls `downsweepSequential` to write
   *  the `output` angles.
   */
  def downsweep(input: Array[Float], output: Array[Float], startingAngle: Float, tree: Tree): Unit = {
    tree match
      case Tree.Node(left, right) =>
        parallel(downsweep(input, output, startingAngle, left),
          downsweep(input, output, Math.max(left.maxPrevious, startingAngle), right))
      case Tree.Leaf(from, until, maxPrevious) =>
        downsweepSequential(input, output, Math.max(startingAngle, maxPrevious), from, until)
  }

  /** Compute the line-of-sight in parallel. */
  def parLineOfSight(input: Array[Float], output: Array[Float], threshold: Int): Unit = {
    val tree = upsweep(input, 0, input.length, threshold)
    downsweep(input, output, 0, tree)
  }
