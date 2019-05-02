/*
    IMPOTANTE: Este codigo foi criado em um notebook no ambiente DATABRICKS.
    Assim sendo, algumas partes do codigo, que estariam presentes em uma aplicacao tradicional Spark, nao se encontram neste codigo.
    Por exemplo: construcao da SparkSession
    O Databricks file system (DBFS) foi utilizado para armazenar os arquivos de entrada.
*/

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.functions._

// Classe representando um log de acesso aos servidores Web da NASA
case class AccessLog(host: String, date: Date, httpRequest: String, rspCode: Int, rspSize: Long, url: String)

// Expressoes regulares usadas no parse dos arquivos de entrada (NASA)
val hostRegex = """([^\s]*)"""
val dateRegex = """\[([^\]]*)\]"""
/* Edge cases:
- Linhas que possuem aspas " extra e terminam em HTTP/1.0
- Linhas que nao possuem HTTP/1.0
- Linhas com HTTP/V1.0
- Linhas com http request invalido
*/
val httpRequestRegex = """\"(.* HTTP\/\d.\d|[^\"]*)\""""
val rspCodeRegex = """(\d{1,})"""
/* Edge cases:
- Linhas cujo rspSize = "-", a maioria sao 404(not found), 302(moved temporarily) e 403(forbiden)
*/
val rspSizeRegex = """(\d{1,}|-)"""

// Regex usada para a linha completa
val lineRegex = (f"^${hostRegex} - - ${dateRegex} ${httpRequestRegex} ${rspCodeRegex} ${rspSizeRegex}" + "$").r

// IMPORTANTE! ignorando timezone para manter a data original que o evento foi registrado no timezone do servidor NASA (GMT -4).
// Caso contrario timestamps registrados nas ultimas 4 horas do dia, sao convertidas para o proximo dia na conversao para Date.
// Ex: 01/Jul/1995:22:00:01 -0400 se torna 02/Jul/1995 apos a conversao (devido ao timezone GMT -4)
val dateFormat = "dd/MMM/yyyy"
val dateParser = new SimpleDateFormat(dateFormat)

// Transforma date string em java.sql.Date (tipo de data suportado por Spark DataFrames)
def parseDate(dateStr: String): Date = {
  val date = dateParser.parse(dateStr)  // Parse date 
  
  new Date(date.getTime)  
}

// Parse do response size.
def parseRspSize(rspSize: String): Long = {
  if (rspSize == "-") 0L else rspSize.toLong
}

// Extrai URL de uma string representando um HTTP request
def extractURL(httpRequest: String): String = {
  // Remove metodo HTTP do inicio, versao HTTP do final e espacos no inicio e fim
  httpRequest.trim().replaceAll("""^(?:GET|HEAD|POST|PUT|DELETE)\s*|\s*HTTP/V?\d.\d$""", "")
}

/*
  Faz o parse de uma linha contendo 1 log de acesso
  Em caso de erro de parse, salva linha completa no campo "host" e retorna "rspCode" = -1
  Desta maneira eh possivel identificar e tratar linhas com problema
*/
def parseLine(line: String): AccessLog = {
    line match {
      case lineRegex(host, dateStr, httpRequest: String, rspCode, rspSize) =>
        AccessLog(host, parseDate(dateStr), httpRequest, rspCode.toInt, parseRspSize(rspSize), extractURL(httpRequest))
      case _ =>  
        AccessLog(line, null, null, -1, -1, null)
    }
}

// Arquivos foram salvos no file system Databricks (dbfs)
val inputFile = "dbfs:/FileStore/tables/nasa/NASA_access_log_*.gz"

// Le arquivos, faz parse das linhas, transforma em dataframe e faz cache
val nasaAccessLogAll = sc.textFile(inputFile).map(parseLine).toDF().cache()

// Dataframe com registros onde parse foi bem sucedido
val nasaAccessLog = nasaAccessLogAll.filter(col("rspCode") >= 0)

// Dataframe com registros onde parse falhou
val nasaAccessLogParseError = nasaAccessLogAll.filter(col("rspCode") < 0)

// Obtem total de registros de cada dataframe
val (countAll, countParsed, countError) = (nasaAccessLogAll.count(), nasaAccessLog.count(), nasaAccessLogParseError.count())

println(f"Total: $countAll, Parsed: $countParsed, Error: $countError")

// A soma de registros parsed com sucesso e falhas deve ser igual ao total de registros. 
assert (countAll == countParsed + countError)

// registros com erro HTTP 404
val http404 = nasaAccessLog.filter(col("rspCode") === 404).cache()

// 1- Numero de Hosts distintos 
val distinctHosts = nasaAccessLog.select("host").distinct().count()

// 2- Total de erros 404
val http404Count = http404.count

// 3- Top 5 URLs que causaram HTTP 404
val top5Http404Urls = http404.groupBy("url").count().orderBy(desc("count")).limit(5)

// 4- Quantidade de erros 404 por dia (Datas no timezone do servidor NASA)
val http404PerDay = http404.groupBy("date").count().orderBy("date")

// 5- Total de bytes retornados
val totalBytes = nasaAccessLog.agg(sum("rspSize")).first().getLong(0)
