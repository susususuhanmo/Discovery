package com.zstu.libdata.StreamSplit.KafkaDataClean

import java.util.regex.Pattern

import scala.util.control.Breaks

/**
  * Created by xiangjh on 2017/3/24.
  */
object MatchUtil {

  def isClcCorrect(clc:String):Boolean={
     isMatch(clc)
  }

  def isMatch(input:String):Boolean = {
    var res=""
    if(input.contains(":")) {
      val idx = input.indexOf(":");
       res = input.substring(0, idx);
    }
    if (input.contains("-")) {
      val idx = input.indexOf("-");
      res = input.substring(0, idx);
    }
    if (input.contains("=")) {
      val idx = input.indexOf("=");
      res = input.substring(0, idx);
    }
    if (input.contains("(")) {
      val idx = input.indexOf("(");
      res = input.substring(0, idx);
    }
    if (input.contains("/")) {
      val idx = input.indexOf("/");
      res = input.substring(0, idx);
    }
    if (input.contains("+")) {
      val idx = input.indexOf("+");
      res = input.substring(0, idx);
    }
    res = res.trim();

    val one =Array("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K",
      "N", "O", "P", "Q", "R", "S", "T", "U", "V", "X", "Z" )
    for(i <- 0 to one.length - 1) {
      if(one(i).equalsIgnoreCase(input)) {
        return true;
      }
    }

    val two = Array("DF","TB", "TD", "TE", "TF", "TG", "TH", "TJ", "TK", "TL", "TM",
      "TN", "TP", "TQ", "TS", "TU", "TV" )
    for(i <- 0 to two.length - 1) {
      if(two(i).equalsIgnoreCase(input)) {
        return true;
      }
    }

    val patternString = "^[A-Z][A-Z0-9\\-:.\\(\\)=]+";
    val p = Pattern.compile(patternString);
    val m = p.matcher(input);

    if (!m.matches()) {
      return false;
    }

    var isCorrect = false;
    val loop = new Breaks;
    loop.breakable{
    for (idx <- 0 to  two.length - 1) {
      val s = two(idx);
      if (input.startsWith(s)) {
        if (input.length() == 2) {
          return false;
        }
        if (!Character.isDigit(input.charAt(2)) && !"-".equals(String.valueOf(input.charAt(2)))) {
          return false;
        }
        isCorrect = true;
        loop.break()
      }
      }
    }
    if (!isCorrect) {
      loop.breakable {
        for (idx <- 0 to one.length - 1)
        {
          val s = one(idx);
          if (input.startsWith(s)) {
            if (input.length() == 1) {
              return false;
            }
            if (!Character.isDigit(input.charAt(1)) && !"-".equals(String.valueOf(input.charAt(1)))) {
              return false;
            }
            isCorrect = true;
            loop.break()
          }
        }
      }
      if (!isCorrect) {
        return false;
      }
    }
    return true;
  }

}
