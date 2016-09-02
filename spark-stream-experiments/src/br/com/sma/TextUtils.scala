package br.com.sma

import java.text.Normalizer

object  TextUtils {
  
 def removerAcentos(s: String):  String = {

 def semhtmltags = s.replaceAll("&Aacute","Á")
.replaceAll("&aacute;","á")
.replaceAll("&Acirc;","Â")
.replaceAll("&acirc;","â")
.replaceAll("&Agrave;","À")
.replaceAll("&agrave;","à")
.replaceAll("&Aring;","Å")
.replaceAll("&aring;","å")
.replaceAll("&Atilde;","Ã")
.replaceAll("&atilde;","ã")
.replaceAll("&Auml;","Ä")
.replaceAll("&auml;","ä")
.replaceAll("&AElig;","Æ")
.replaceAll("&aeli;","æ")
.replaceAll("&Eacute;","É")
.replaceAll("&eacute;","é")
.replaceAll("&Ecirc;","Ê")
.replaceAll("&ecirc;","ê")
.replaceAll("&Egrave;","È")
.replaceAll("&egrave;","è")
.replaceAll("&Euml;","Ë")
.replaceAll("&euml;","ë")
.replaceAll("&ETH;","Ð")
.replaceAll("&eth;","ð")
.replaceAll("&Iacute;","Í")
.replaceAll("&iacute;","í")
.replaceAll("&Icirc;","Î")
.replaceAll("&icirc;","î")
.replaceAll("&Igrave;","Ì")
.replaceAll("&igrave;","ì")
.replaceAll("&Iuml;","Ï")
.replaceAll("&iuml;","ï")
.replaceAll("&Oacute;","Ó")
.replaceAll("&oacute;"," ó")
.replaceAll("&Ocirc;","Ô")
.replaceAll("&ocircô;","ô")
.replaceAll("&Ograve;"," Ò")
.replaceAll("&ograve;","ò")
.replaceAll("&Oslash;","Ø")
.replaceAll("&oslash;","ø")
.replaceAll("&Otilde;","Õ")
.replaceAll("&otilde;","õ")
.replaceAll("&Ouml;","o")
.replaceAll("&ouml;","ö")
.replaceAll("&Uacute;"," Ú")
.replaceAll("&uacute;","ú")
.replaceAll("&Ucirc;","Û")
.replaceAll("&ucirc;","û")
.replaceAll("&Ugrave;","  Ù")
.replaceAll("&ugrave;"," ù")
.replaceAll("&Uuml;","Ü")
.replaceAll("&uuml;","ü")
.replaceAll("&Ccedil;","Ç")
.replaceAll("&ccedil;","ç")
.replaceAll("&Ntilde;","Ñ")
.replaceAll("&ntilde;","ñ")
.replaceAll("&lt;","<")
.replaceAll("&gt;",">")
.replaceAll("&amp;","&")
.replaceAll("&quot;","")
.replaceAll("&reg;","®")
.replaceAll("&copy;","©")
.replaceAll("&Yacute;","Ý")
.replaceAll("&yacute;","ý")
.replaceAll("&THORN;","Þ")
.replaceAll("&thorn;","þ")
.replaceAll("&szlig;","ß")
.replaceAll("&nbsp;"," ")
.replaceAll("[\\p{C}]","")
.replaceAll("<a [^>]+>[^<]*<\\/a>","")

return Normalizer.normalize(semhtmltags, Normalizer.Form.NFD).toLowerCase().replaceAll("[^\\p{ASCII}]", "")


 
 } 
 

 
  
}
