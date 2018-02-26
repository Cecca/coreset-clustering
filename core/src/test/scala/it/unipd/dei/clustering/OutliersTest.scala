package it.unipd.dei.clustering

import org.scalacheck.Prop.{forAll, BooleanOperators, all}
import it.unipd.dei.clustering.Utils.{maxMinDistance, minDistance}
import org.scalacheck.Prop.forAll
import org.scalacheck.{Gen, Properties}

class OutliersTest extends Properties("Outliers algorithm") {

  val distance: (Point, Point) => Double = Distance.euclidean

  property("relationship between optima") =
    forAll(Gen.listOf[Double](Gen.choose[Double](0.0, 1.0)), Gen.choose[Int](2, 100), Gen.choose[Int](2, 100))
    { (pts: List[Double], k: Int, z: Int) =>
      (pts.length >= 2 && k < pts.size && z < pts.size) ==> {
        val points = pts.map(p => Point(p)).toArray
        val (result, outliers) = Outliers.run(points, k, z, distance)
        val resultGMM = GMM.run(points, k + z, distance)

        val radius = maxMinDistance(result, points.toSet.diff(outliers.toSet).toVector, distance)
        val radiusGMM = maxMinDistance(resultGMM, points, distance)
        radiusGMM <= radius
      }
    }

}
