package com.zstu.libdata.StreamSplit.function

object LevenshteinDistance {
  def score(first:String, second:String):Double = {
    if(first == null || second == null || first.length() == 0 || second.length() == 0) return .0
    val maxLength = Math.max(first.length(), second.length())

    1.0*(maxLength-computeEditDistance(first,second))/maxLength
  }

  /**
   * 计算两个字符串的匹配强度系统 （0～1)*/
  def computeEditDistance(first_in:String, second_in:String): Int = {
    val first = first_in.toLowerCase()
    val second = second_in.toLowerCase()
    val len1 = first.length()
    val len2 = second.length()
    
    var costs = new Array[Int](len2+1)
    
    for (i <- 0 to len1) {
            var previousValue = i;
            for (j <- 0 to len2) {
                if (i == 0) {
                    costs(j) = j;
                }
                else if (j > 0) {
                    var useValue = costs(j - 1);
                    if (first.charAt(i - 1) != second.charAt(j - 1)) {
                        useValue = Math.min(Math.min(useValue, previousValue), costs(j)) + 1;
                    }
                    costs(j - 1) = previousValue;
                    previousValue = useValue;

                }
            }
            if (i > 0) {
                costs(second.length()) = previousValue;
            }
        }
    
    costs(len2)
  }
  
//  def main(args:Array[String]) {
//    println(score("椹厠鎬�,鎭╂牸鏂憲涓叡涓ぎ椹厠鎬�,鎭╂牸鏂�,鍒楀畞,鏂ぇ鏋楄憲浣滃眬璇�", "涓叡涓ぎ椹厠鎬濇仼鏍兼柉鍒楀畞鏂ぇ鏋楄憲浣滃眬"))
//  }
}