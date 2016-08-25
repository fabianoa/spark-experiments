package br.com.sma

import java.text.Normalizer

object  TextUtils {
  
 def removerAcentos(s: String):  String = {

 return Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "")
 
 } 
 
  
}