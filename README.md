# desafio_semantix

IMPORTANTE:

1- O codigo .scala foi criado em um notebook no ambiente DATABRICKS.
Assim sendo, algumas partes do codigo, que estariam presentes em uma aplicacao tradicional Spark, nao se encontram neste codigo. Por exemplo: construcao da SparkSession

2- O Databricks file system (DBFS) foi utilizado para armazenar os arquivos de entrada.

3 - O timezone foi desconsiderado no parse do timestamp de acesso para manter a data original que o evento foi registrado no timezone do servidor NASA (GMT -4). 
Caso contrario timestamps registrados nas ultimas 4 horas do dia, seriam convertidas para o proximo dia na conversao para Date.
Ex: 01/Jul/1995:22:00:01 -0400 se torna 02/Jul/1995 apos a conversao (devido ao timezone GMT -4)

4- Foram verificados registros com formato de requisição HTTP inválido. Não seguiam o formato:
“<HTTP method> <URL> <HTTP version>” onde HTTP method = GET,POST,PUT,…
Estes registros representam uma parcela mínima dos dados (em torno de 15 registros) e foram mantidos nos dados. 
Foram tratados como se a URL fosse o campo http request inteiro.

5- Apenas um registro incompleto foi encontrado nos dados (possuía apenas hostname) e foi ignorado.


Referências:

```
import java.sql.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
```
