package com.zstu.libdata.StreamSplit.function

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/6/7 0007.
  */
object distinctRdd {
  /**
    * 对分好组的数据，对每一组的数据进行查重
    * @param keyAndValue  输入数据，key相同。
    * @return 去重分开之后的数据
    */
  def distinctGroup(keyAndValue: (String, Iterable[(String, String, String, String, String, String,String,String)])): ArrayBuffer[(String, (String, String, String, String, String, String, String,String))]
  = {
    //      (key, (title, journal, creator, id, institute))
    val (key, value) = keyAndValue
    val valueArray = value.toArray
    val resultArray = ArrayBuffer[(String,(String, String, String, String, String, String,String,String))]()

    /**
      * 根据(leftTitle,leftJounal,leftCreator)判断两个数据是否相同
      *
      * @param leftIndex  左侧下标
      * @param rightIndex 右侧下标
      * @return true or false
      */
    def isSame(leftIndex: Int, rightIndex: Int): Boolean = {
      val (leftTitle, leftJounal, leftCreator)
      = (valueArray(leftIndex)._1, valueArray(leftIndex)._2, valueArray(leftIndex)._3)
      val (rightTitle, rightJounal, rightCreator)
      = (valueArray(rightIndex)._1, valueArray(rightIndex)._2, valueArray(rightIndex)._3)
      val leftStr = if (leftCreator != null && rightCreator != null)
        leftJounal + leftTitle + leftCreator
      else leftJounal + leftTitle
      val rightStr = if (leftCreator != null && rightCreator != null)
        rightJounal + rightTitle + rightCreator
      else rightJounal + rightTitle
      val score = LevenshteinDistance.score(leftStr, rightStr)
      if (score > 0.90)
        true
      else false
    }


    for (leftIndex <- valueArray.indices) {
      var hasSame = false
      for (rightIndex <- leftIndex + 1 until valueArray.length) {
        if (isSame(leftIndex, rightIndex))
          hasSame = true
      }
      val resultValue =(key,valueArray(leftIndex))
      if (!hasSame) resultArray += resultValue
    }
    resultArray
  }


  /**
    * 对输入数据进行去重
    * @param simplifiedInputRdd 简化的输入rdd
    * @return 去重后的输入数据
    */
  def distinctInputRdd(simplifiedInputRdd
                       : RDD[(String, (String, String, String, String, String, String,String,String))])
  : (RDD[(String, (String, String, String, String, String, String,String,String))]
    , RDD[(String, String)])

  = {

    val fullId = simplifiedInputRdd.map(value => (value._2._4,""))
    //    (key, (title, journal, creator, id, institute,year))
    val groupedRdd = simplifiedInputRdd
      .filter(value => value._2._1 != null && value._2._2 != null)
      .groupByKey()

    val distinctRdd = groupedRdd.flatMap(distinctGroup).filter(value => value != null)
    val distinctId = distinctRdd.map(value=>(value._2._4,""))
    val repeatedRdd: RDD[(String, String)] =
      fullId.leftOuterJoin(distinctId).filter(value => value._2._2.orNull == null)
      .map(value => (value._1,"内部重复数据"))
    (distinctRdd,repeatedRdd)
  }

}
