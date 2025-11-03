package com.rag

object Vectors {
  /** L2-normalize a float vector (needed if we use cosine/IP). */
  def l2(v: Array[Float]): Array[Float] = {
    val n = math.sqrt(v.foldLeft(0.0)((s, x) => s + x * x)).toFloat
    if (n == 0f) v else v.map(_ / n)
  }
}
