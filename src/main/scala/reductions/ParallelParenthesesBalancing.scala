package reductions

import scala.annotation._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    var cnt = 0
    for (c <- chars) {
      c match {
        case '(' => cnt += 1
        case ')' => cnt -= 1
        case _ =>
      }
      if (cnt < 0) {
        return false
      }
    }
    cnt == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, open: Int, close: Int): (Int, Int) = {
      if (idx == until) (open, close)
      else chars(idx) match {
        case '(' => traverse(idx + 1, until, open + 1, close)
        case ')' => if (open > 0) traverse(idx + 1, until, open - 1, close) else traverse(idx + 1, until, open, close + 1)
        case _ => traverse(idx + 1, until, open, close)
      }
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val m = from + (until - from) / 2
        val ((lo, lc), (ro, rc)) = parallel(reduce(from, m), reduce(m, until))
        if (lo > rc) (lo - rc + ro, lc) else (ro, rc - lo + lc)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
