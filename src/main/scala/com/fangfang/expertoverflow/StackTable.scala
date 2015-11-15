package com.fangfang.expertoverflow

import java.io.File
import scala.io.{ BufferedSource, Source }

abstract class StackTable[T] {

  val file: File

  def getDate(n: scala.xml.NodeSeq): Long = n.text match {
    case "" => 0
    case s => dateFormat.parse(s).getTime
  }

  def dateFormat = {
    import java.text.SimpleDateFormat
    import java.util.TimeZone
    val f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    f.setTimeZone(TimeZone.getTimeZone("GMT"))
    f
  }

  def getInt(n: scala.xml.NodeSeq): Int = n.text match {
    case "" => 0
    case x => x.toInt
  }

  def parseXml(x: scala.xml.Elem): T


  def parse(s: String): Option[T] =
    if (s.startsWith("  <row ") & s.endsWith(" />")) 
	Some(parseXml(scala.xml.XML.loadString(s)))
//  { try{ Some(parseXml(scala.xml.XML.loadString(s))) }catch{ case _: Throwable => println(s) } }
    else None

  def stackOverflowSource(file: File): BufferedSource = Source.fromInputStream(new DropBOMInputStream(file))(scala.io.Codec.UTF8)
}
