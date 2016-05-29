package br.com.sma

import java.util.{Date, Locale}
import java.text.DateFormat
import java.text.DateFormat._


object Streaming {
  
  
  
def main(args: Array[String]): Unit = {
    println("Hello, world!")
    
    val now = new Date
    val df = getDateInstance(LONG, Locale.FRANCE)
    println(df format now)
  }
  
}