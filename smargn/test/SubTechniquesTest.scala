import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.ShouldMatchers
import techniques.NaiveShiftComparison._
import techniques.NaiveInverseComparisons._
import techniques.Divergence._
import utils.SubTechniques._

class SubTechniquesTest extends SparkTestUtils with ShouldMatchers {

  sparkTest("testNaiveInverseComparisons1") {
    val testedWord = ("sky", Array(2.0, 3.0, 4.0, 12.0, 3.0))
    val dataRaw = Array(("sea", Array(3.0, 11.0, 3.0, 2.0, 1.0)), ("earth", Array(3.0, 12.0, 4.0, 3.0, 2.0)), ("yellow", Array(13.0, 14.0, 13.0, 16.0, 15.0)), ("flower", Array(14.0, 15.0, 16.0, 114.0, 15.0)))
    val data = sc.parallelize(dataRaw)
    val b = naiveInverseDifference(data, testedWord, List(2.0))
    b.collect().sortWith(_ < _) should be(Array("earth", "sea"))
  }

  sparkTest("testNaiveInverseComparisons2") {
    val testedWord = ("sky", Array(12.0, 13.0, 14.0, 112.0, 13.0))
    val dataRaw = Array(("sea", Array(1.0, 2.0, 3.0, 4.0, 5.0)), ("earth", Array(2.0, 1.0, 2.0, 4.0, 4.0)), ("yellow", Array(3.0, 4.0, 3.0, 6.0, 5.0)), ("flower", Array(4.0, 5.0, 6.0, 14.0, 5.0)), ("orange", Array(1.0, 5.0, 6.0, 13.0, 2.0)), ("dummy", Array(20.0, 30.0, 4.0, 2.0, 3.0)), ("parrot", Array(2.0, 3.0, 4.0, 12.0, 3.0)))
    val data = sc.parallelize(dataRaw)
    naiveInverseDifference(data, testedWord, List(0.2)).collect().sortWith(_ < _) should be(Array())
  }

  // SHIFT TEST BASED ON VALUE SHIFTLEN = 3, SHIFTSTEP = 1
  sparkTest("NaiveShiftComparison1") {
    val testedWord = ("word1", Array(2.0, 1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    val dataRaw = Array(("sea", Array(5.0, 4.0, 3.0, 2.0, 1.0, 0.0, 1.0, 2.0, 3.0)), ("boring", Array(15.0, 16.0, 71.0, 8.0, 19.0, 10.0, 11.0, 12.0, 13.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifferenceShift(data, testedWord, List(0.2)).collect().sortWith(_ < _) should be(Array("sea"))
  }

  sparkTest("NaiveShiftComparison2") {
    val testedWord = ("word1", Array(2.0, 1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    val dataRaw = Array(("sea", Array(10.0, 5.0, 4.0, 3.0, 2.0, 1.0, 0.0, 1.0, 2.0)), ("boring", Array(5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifferenceShift(data, testedWord, List(0.2)).collect().sortWith(_ < _) should be(Array())
  }

  sparkTest("NaiveShiftComparison3") {
    val testedWord = ("word1", Array(2.0, 1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    val dataRaw = Array(("sea", Array(15.0, 14.0, 13.0, 12.0, 11.0, 10.0, 11.0, 12.0, 13.0)), ("boring", Array(5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifferenceShift(data, testedWord, List(0.2)).collect().sortWith(_ < _) should be(Array())
  }

  sparkTest("testDivergence1") {
    val testedWord = ("word1", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 5.0, 7.0, 9.0, 20.0, 23.0))
    val dataRaw = Array(("sea", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 2.0, 2.0, 10.0, 0.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifferenceDivergence(data, testedWord, List(1.0, 3.0, 3.0)).collect().sortWith(_ < _) should be(Array("sea"))
  }

  sparkTest("testDivergence2") {
    val testedWord = ("word1", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 5.0, 7.0, 9.0, 20.0, 23.0))
    val dataRaw = Array(("sea", Array(2.0, 4.0, 2.0, 3.0, 2.0, 2.0, 5.0, 2.0, 2.0, 10.0, 0.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifferenceDivergence(data, testedWord, List(1.0, 3.0, 3.0)).collect().sortWith(_ < _) should be(Array("sea"))
  }

  // Here we would expect an empty array because we have two value in sea that differ form the word1. But the two curves
  // do not diverge
  sparkTest("testDivergence3") {
    val testedWord = ("word1", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 5.0, 7.0, 9.0, 20.0, 23.0))
    val dataRaw = Array(("sea", Array(3.0, 3.0, 3.0, 3.0, 3.0, 2.0, 5.0, 2.0, 2.0, 19.0, 23.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifferenceDivergence(data, testedWord, List(1.0, 3.0, 3.0)).collect().sortWith(_ < _) should be(Array())
  }

  sparkTest("testDivergence4") {
    val testedWord = ("word1", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 5.0, 7.0, 9.0, 20.0, 23.0))
    val dataRaw = Array(("sea", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 2.0, 2.0, 10.0, 0.0)), ("sky", Array(13.0, 23.0, 33.0, 33.0, 0.0, 30.0, 0.0, 2.0, 2.0, 1.0, 0.0)), ("cloud", Array(3.0, 3.0, 3.0, 3.0, 3.0, 0.0, 0.0, 2.0, 2.0, 10.0, 0.0)))
    val data = sc.parallelize(dataRaw)
    naiveDifferenceDivergence(data, testedWord, List(1.0, 3.0, 3.0)).collect().sortWith(_ < _) should be(Array("cloud", "sea"))
  }


  sparkTest("testDivergenceInverse1") {
    val testedWord = ("word1", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 5.0, 7.0, 9.0, 20.0, 23.0).reverse)

    val dataRaw = Array(("sea", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 2.0, 2.0, 10.0, 0.0).reverse))
    val data = sc.parallelize(dataRaw)
    naiveInverseDifferenceDivergence(data, testedWord, List(1.0, 3.0, 3.0)).collect().sortWith(_ < _) should be(Array("sea"))
  }

  // Here we would expect an empty array because we have two value in sea that differ form the word1. But the two curves
  // do not diverge
  sparkTest("testDivergenceInverse2") {
    val testedWord = ("word1", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 5.0, 7.0, 9.0, 20.0, 23.0).reverse)
    val dataRaw = Array(("sea", Array(3.0, 3.0, 3.0, 3.0, 3.0, 2.0, 5.0, 2.0, 2.0, 19.0, 23.0).reverse))
    val data = sc.parallelize(dataRaw)
    naiveInverseDifferenceDivergence(data, testedWord, List(1.0, 3.0, 3.0)).collect().sortWith(_ < _) should be(Array())
  }

  sparkTest("testDivergenceInverse3") {
    val testedWord = ("word1", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 5.0, 7.0, 9.0, 20.0, 23.0).reverse)
    val dataRaw = Array(("sea", Array(3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 2.0, 2.0, 10.0, 0.0)), ("sky", Array(13.0, 23.0, 33.0, 33.0, 0.0, 30.0, 0.0, 2.0, 2.0, 1.0, 0.0)), ("cloud", Array(3.0, 3.0, 3.0, 3.0, 3.0, 0.0, 0.0, 2.0, 2.0, 10.0, 0.0)))
    val dataRawReversed = dataRaw.map(x => (x._1, x._2.reverse))
    val data = sc.parallelize(dataRawReversed)
    naiveInverseDifferenceDivergence(data, testedWord, List(1.0, 3.0, 3.0)).collect().sortWith(_ < _) should be(Array("cloud", "sea"))
  }

  sparkTest("testsmartDivergence1") {
    val testedWord = ("vapeur", Array(421, 491, 387, 195, 220, 187, 171, 269, 213, 250, 282, 439, 348, 459, 903, 853, 1196, 1042, 937, 597, 652, 700, 815, 710, 599, 743, 640, 651, 874, 862, 844, 911, 944, 872, 914, 838, 659, 789, 760, 644, 710, 747, 766, 1003, 806, 780, 647, 860, 699, 692, 649, 1176, 1169, 990, 994, 1061, 1013, 876, 919, 851, 957, 836, 862, 764, 1241, 885, 906, 749, 674, 746, 723, 766, 1055, 873, 1015, 1809, 2127, 701, 261, 132, 289, 289, 353, 544, 387, 550, 714, 909, 940, 958, 697, 544, 402, 352, 428, 505, 435, 792, 420, 603, 611, 535, 401, 276, 248, 212, 248, 246, 167, 228, 225, 202, 212, 198, 208, 246, 247, 318, 385, 278, 240, 213, 194, 219, 198, 180, 179, 164, 159, 154, 194, 154, 113, 182, 138, 112, 150, 121, 174, 220, 167, 228, 298, 217, 328, 312, 554, 564, 540, 190, 257, 836, 2064, 408, 808, 496, 732, 1056, 30).map(x => x.toDouble))
    val dataRaw = Array(("nationales", Array(47, 32, 36, 42, 46, 28, 51, 52, 87, 43, 50, 53, 66, 41, 53, 63, 65, 61, 77, 101, 117, 152, 123, 114, 113, 118, 171, 187, 120, 163, 240, 349, 183, 195, 181, 170, 185, 132, 173, 170, 197, 195, 170, 158, 110, 170, 135, 183, 157, 148, 130, 248, 215, 243, 203, 219, 284, 261, 217, 214, 205, 235, 207, 214, 189, 220, 197, 297, 247, 244, 268, 296, 321, 331, 289, 454, 471, 194, 179, 279, 358, 542, 671, 537, 575, 623, 673, 613, 589, 691, 773, 558, 657, 625, 578, 687, 760, 1190, 828, 624, 372, 372, 453, 439, 469, 469, 591, 568, 581, 649, 580, 534, 565, 628, 640, 507, 551, 647, 733, 899, 1109, 1124, 775, 983, 1285, 973, 1044, 1009, 786, 884, 968, 818, 656, 916, 823, 949, 662, 644, 1103, 838, 650, 757, 954, 989, 830, 688, 1618, 1686, 1822, 775, 781, 3384, 7008, 1786, 2330, 2952, 4353, 5946, 122).map(x => x.toDouble)), ("marches", Array(22, 45, 31, 37, 22, 23, 25, 40, 42, 49, 34, 39, 17, 18, 50, 42, 61, 42, 74, 79, 199, 134, 70, 88, 128, 70, 91, 77, 81, 60, 158, 133, 77, 90, 91, 98, 82, 145, 100, 131, 146, 120, 102, 108, 94, 155, 101, 103, 116, 111, 87, 131, 161, 127, 150, 162, 126, 184, 124, 175, 179, 214, 208, 208, 202, 136, 133, 164, 181, 185, 178, 199, 164, 251, 245, 169, 137, 64, 54, 59, 66, 123, 97, 108, 150, 151, 138, 144, 158, 160, 216, 212, 181, 197, 210, 292, 235, 363, 316, 289, 275, 233, 245, 194, 211, 232, 261, 177, 202, 202, 230, 201, 229, 203, 221, 276, 261, 237, 232, 289, 261, 241, 254, 225, 257, 200, 272, 290, 360, 396, 241, 190, 161, 137, 153, 114, 77, 92, 163, 125, 115, 238, 391, 450, 510, 509, 1534, 1298, 1332, 417, 353, 1136, 4020, 872, 864, 838, 894, 1530, 30).map(x => x.toDouble)), ("nucléaire", Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 2, 1, 1, 1, 0, 0, 0, 4, 5, 0, 6, 1, 8, 0, 7, 64, 51, 52, 39, 48, 66, 222, 297, 232, 628, 554, 1129, 1201, 985, 1005, 1049, 1419, 1940, 1741, 1303, 1538, 1214, 803, 792, 587, 663, 563, 1145, 1217, 1595, 2179, 2293, 2371, 3589, 2176, 2366, 1827, 2091, 2400, 2033, 7320, 5338, 4440, 1328, 1492, 4928, 8226, 2148, 3634, 4186, 3642, 4428, 148).map(x => x.toDouble)))
    val data = sc.parallelize(dataRaw)
    smarterDivergence(data, testedWord, List(7.0, 7.0, 10.0)).collect().sortWith(_ < _) should be(Array("nucléaire"))
  }
}