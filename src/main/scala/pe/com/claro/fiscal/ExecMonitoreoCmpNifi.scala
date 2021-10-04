package pe.com.claro.fiscal

import java.net.InetAddress
import java.sql.Connection
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import net.liftweb.json._
import org.apache.logging.log4j.Logger
import pe.com.claro.util.OracleUtil

import scala.sys.process._

/** [[pe.com.claro.fiscal.ExecMonitoreoCmpNifi]] : Evalua estado del servicio Apache Nifi,
 * metricas de performance y almacena los valores obtenidos en tablas Oracle.
 *
 * @groupname main_method_funcs Metodo de ejecucion principal en el Objeto
 * @groupname util_funs Funciones de Obtencion de Configuración
 * @groupname select_cols_funcs Funciones de Seleccion de columnas en DF
 */

object ExecMonitoreoCmpNifi {
  /** Obtiene metricas del estado y performance del Componente Apache Nifi.
   * @param ambienteActual: Ambiente actual
   * @param flgSecuredConnection: Flag de Nifi/Airflow en ambiente seguro
   * @param settings:Importa configuracion de clase Setting (Lectura archivo config)
   * @note ''Métricas divididas en dos grupos:
   *      1. Estado General del Componente por servidor de ingesta
   *      2. Métricas de performance del Componente por servidor de ingesta''
   * @author Susan Delgado
   * @version 1.0
   * @group main_method_funcs
   */
  def main(ambienteActual:String,flgSecuredConnection:String,settings: Settings,conn:Connection,logger:Logger): Unit = {
    logger.info("Inicia ejecucion de monitoreo de servicio Apache Nifi")
    logger.info("Obtiene parametros de configuracion desde linea de ejecucion")
    val env=ambienteActual

    logger.info("Obtiene parametros de configuracion desde archivo .conf")
    val user = settings.user
    logger.info("Obtiene parametros tablas destino Oracle")
    /*Parametros tabla Oracle destino*/
    var tableTargetStatusGeneral=settings.tableTargetStatusGeneral
    val owner_tablas=tableTargetStatusGeneral.split("\\.")(0)
    tableTargetStatusGeneral=tableTargetStatusGeneral.split("\\.")(1)
    var tableTargetMetricasPerformance=settings.tableTargetMetricasPerformance
    tableTargetMetricasPerformance=tableTargetMetricasPerformance.split("\\.")(1)


    logger.info("Obtiene Parametros rango de evaluacion de metricas")
    val rangoMaxNormalInput=settings.rangoMaxNormalInput
    val rangoMaxWarningInput=settings.rangoMaxWarningInput
    logger.info("Obtiene *Parametros Servicio Apache Nifi")
    val ruta_bin_nifi = settings.rutaBinNifi
    val puertoNifi=settings.puertoNifi
    val userADNifi=settings.userADNifi
    val passADNifi=settings.passADNifi

    logger.info("Obtiene informacion de Servidor de ejecución actual")
    val localhost = InetAddress.getLocalHost
    val ip_origen = localhost.getHostAddress
    val hostname_origen=localhost.getCanonicalHostName
    val nodo=s"${ip_origen} (${hostname_origen})"
    val servicio="nifi"
    val id_nodo=ip_origen.split("\\.")(3)
    val id_nodo_servicio=s"${servicio}_${id_nodo}"

    logger.info("Obtiene fecha actual")
    val insFecha=LocalDateTime.now()
    val formatter=DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
    val fecha=formatter.format(insFecha)

    logger.info("1. Estado General del Componente por servidor de ingesta")
    logger.info(" Configuración de comando de ejecución y argumentos")
    val shellScript="nifi.sh"
    var command=Seq("sh",ruta_bin_nifi+shellScript)
    var arguments=Seq("status")
    logger.info("Consulta estado de Apache Nifi")
    var msg_retorno=ejecutaShell(command,arguments)
    logger.info("Evalua estado de Apache Nifi")
    val arrayStatusNifi=obtieneEstadoNifi(msg_retorno)
    val estadoNifi=arrayStatusNifi(0)
    val codMsgNifi=arrayStatusNifi(1).toInt
    val desMsgNifi=arrayStatusNifi(2)
    logger.info(s"Estado del Componente Apache Nifi:${arrayStatusNifi(0)}")
    logger.info(s"Codigo Estado del Componente Apache Nifi:${arrayStatusNifi(1)}")
    logger.info(s"Descripcion detallada Estado del Componente Apache Nifi:${arrayStatusNifi(2)}")

    logger.info("Insercion a tablas de Monitoreo en Oracle")
    val listValuesStatusGeneral=List(env,ip_origen,hostname_origen,
      id_nodo_servicio, estadoNifi,codMsgNifi,
      desMsgNifi, fecha)
    val arrayValuesStatusGeneral=listValuesStatusGeneral.toArray
    /*Status General*/
    insertStatusGeneral(conn,
      owner_tablas,
      tableTargetStatusGeneral,
      arrayValuesStatusGeneral,
      "APPEND")
    insertStatusGeneral(conn,
      owner_tablas,
      tableTargetStatusGeneral+"_log",
      arrayValuesStatusGeneral,"APPEND")

    logger.info("2. Métricas de performance del Componente por servidor de ingesta")
    logger.info("Obtiene System Diagnostics Apache Nifi - NIFI API")
    if(flgSecuredConnection=="S"){
      val shellScriptAPI="nifi-api.sh"
      command=Seq("sh",ruta_bin_nifi+shellScriptAPI)
      arguments=Seq(ip_origen,puertoNifi,userADNifi,s"${passADNifi}","system-diagnostics")
      msg_retorno=ejecutaShell(command,arguments)
    }
    else{
      command=Seq("curl","-H","'Content-Type: application/json'")
      val prefixURL=if(flgSecuredConnection=="S") "https://" else "http://"
      val urlNifi=prefixURL+ip_origen+":"+puertoNifi+"/nifi-api/system-diagnostics/"
      arguments=Seq(urlNifi)
      msg_retorno=ejecutaShell(command,arguments)
    }

    logger.info("Parsea valores de corte")
    val rangoMaxNormal= rangoMaxNormalInput.replace("%","").toDouble.round.toInt
    val rangoMaxWarning= rangoMaxWarningInput.replace("%","").toDouble.round.toInt

    logger.info("Obtencion de valores desde del JSON Obtenido de la consulta de System Diagnostics NIFI API")
    println(msg_retorno)
    val parsedSystemDiagnostics = parse(msg_retorno.toString.trim)
    val heapUtilization=obtieneMetricasNifiGeneral(parsedSystemDiagnostics,
      "heapUtilization").replace("%","").toDouble.round.toInt

    logger.info(s"Porcentaje de uso de Memoria Heap:${heapUtilization}")
    val indicadorHeapUtilization=obtieneIndicadorEvalValorMetrica(heapUtilization,
      rangoMaxNormal,rangoMaxWarning)
    var indicador_metrica=indicadorHeapUtilization(0)
    var des_indicador_metrica=indicadorHeapUtilization(1)
    logger.info(s"Indicador uso de Memoria Heap:${des_indicador_metrica}")
    var codMetrica=s"${servicio}_PORCUSO_MEM_${id_nodo}".toUpperCase()
    var descMetrica="Porcentaje de uso de Memoria Heap"
    var metricasArray=List(env,ip_origen,hostname_origen,id_nodo_servicio,
      codMetrica,descMetrica,heapUtilization,
      indicador_metrica,des_indicador_metrica, fecha).toArray
    insertMetricasServicio(conn,
      owner_tablas,
      tableTargetMetricasPerformance,
      metricasArray,
      "APPEND")
    insertMetricasServicio(conn,
      owner_tablas,
      tableTargetMetricasPerformance+"_log",
      metricasArray,
      "APPEND")

    val processorLoadAverage=obtieneMetricasNifiGeneral(parsedSystemDiagnostics,"processorLoadAverage")
    logger.info(s"Carga promedio por procesador:${processorLoadAverage}")
    val processorLoadAveragePercentage=(processorLoadAverage.toDouble.round.toInt)
    val indicadorProcessorLoadAverage=obtieneIndicadorEvalValorMetrica(processorLoadAveragePercentage,
      rangoMaxNormal,
      rangoMaxWarning)
    indicador_metrica=indicadorProcessorLoadAverage(0)
    des_indicador_metrica=indicadorProcessorLoadAverage(1)
    logger.info(s"Indicador carga promedio por procesador:${des_indicador_metrica}")

    codMetrica=s"${servicio}_CARPRON_PROC_${id_nodo}".toUpperCase()
    descMetrica="Carga promedio por procesador"
    metricasArray=List(env,ip_origen,hostname_origen,id_nodo_servicio,
      codMetrica,descMetrica,processorLoadAveragePercentage,
      indicador_metrica,des_indicador_metrica, fecha).toArray
    insertMetricasServicio(conn,
      owner_tablas,
      tableTargetMetricasPerformance,
      metricasArray,
      "APPEND")
    insertMetricasServicio(conn,
      owner_tablas,
      tableTargetMetricasPerformance+"_log",
      metricasArray,
      "APPEND")
    val flowFileRepositoryStorageUtilization=obtieneMetricasNifiRepositoryStorageUsage(parsedSystemDiagnostics,
      "flowFile",
      "utilization").replace("%",
      "").toDouble.round.toInt
    logger.info(s"Porcentaje de uso de almacenamiento de Flow File Repository:${flowFileRepositoryStorageUtilization}")

    val indicadorFlowFileRepositoryStorageUtilization=obtieneIndicadorEvalValorMetrica(flowFileRepositoryStorageUtilization,
      rangoMaxNormal,
      rangoMaxWarning)
    indicador_metrica=indicadorFlowFileRepositoryStorageUtilization(0)
    des_indicador_metrica=indicadorFlowFileRepositoryStorageUtilization(1)
    logger.info(s"Indicador Porcentaje de uso de almacenamiento de Flow File Repository:${des_indicador_metrica}")

    codMetrica=s"${servicio}_PORUSOAL_FFR_${id_nodo}".toUpperCase()
    descMetrica="Porcentaje de uso de almacenamiento de Flow File Repository"
    metricasArray=List(env,ip_origen,hostname_origen,id_nodo_servicio,
      codMetrica,descMetrica,flowFileRepositoryStorageUtilization,
      indicador_metrica,des_indicador_metrica, fecha).toArray
    insertMetricasServicio(conn,
      owner_tablas,
      tableTargetMetricasPerformance,
      metricasArray,
      "APPEND")
    insertMetricasServicio(conn,
      owner_tablas,
      tableTargetMetricasPerformance+"_log",
      metricasArray,
      "APPEND")
    val contentRepositoryStorageUtilization=obtieneMetricasNifiRepositoryStorageUsage(parsedSystemDiagnostics,
      "content",
      "utilization").replace("%",
      "").toDouble.round.toInt
    logger.info(s"Porcentaje de uso de almacenamiento de Content Repository:${contentRepositoryStorageUtilization}")

    val indicadorContentRepositoryStorageUtilization=obtieneIndicadorEvalValorMetrica(contentRepositoryStorageUtilization,
      rangoMaxNormal,
      rangoMaxWarning)
    indicador_metrica=indicadorContentRepositoryStorageUtilization(0)
    des_indicador_metrica=indicadorContentRepositoryStorageUtilization(1)
    logger.info(s"Indicador Porcentaje de uso de almacenamiento de Content Repository:${des_indicador_metrica}")
    codMetrica=s"${servicio}_PORUSOAL_CR_${id_nodo}".toUpperCase()
    descMetrica="Porcentaje de uso de almacenamiento de Content Repository"
    metricasArray=List(env,ip_origen,hostname_origen,id_nodo_servicio,
      codMetrica,descMetrica,contentRepositoryStorageUtilization,
      indicador_metrica,des_indicador_metrica, fecha).toArray
    insertMetricasServicio(conn,
      owner_tablas,
      tableTargetMetricasPerformance,
      metricasArray,
      "APPEND")
    insertMetricasServicio(conn,
      owner_tablas,
      tableTargetMetricasPerformance+"_log",
      metricasArray,
      "APPEND")

    val provenanceRepositoryStorageUtilization=obtieneMetricasNifiRepositoryStorageUsage(parsedSystemDiagnostics,
      "provenance",
      "utilization").replace("%",
      "").toDouble.round.toInt
    logger.info(s"Porcentaje de uso de almacenamiento de Provenance Repository:${provenanceRepositoryStorageUtilization}")

    val indicadorProvenanceRepositoryStorageUtilization=obtieneIndicadorEvalValorMetrica(provenanceRepositoryStorageUtilization,
      rangoMaxNormal,
      rangoMaxWarning)
    indicador_metrica=indicadorProvenanceRepositoryStorageUtilization(0)
    des_indicador_metrica=indicadorProvenanceRepositoryStorageUtilization(1)
    logger.info(s"Indicador Porcentaje de uso de almacenamiento de Provenance Repository:${des_indicador_metrica}")

    codMetrica=s"${servicio}_PORUSOAL_PR_${id_nodo}".toUpperCase()
    descMetrica="Porcentaje de uso de almacenamiento de Provenance Repository"
    metricasArray=List(env,ip_origen,hostname_origen,id_nodo_servicio,
      codMetrica,descMetrica,provenanceRepositoryStorageUtilization,
      indicador_metrica,des_indicador_metrica, fecha).toArray
    insertMetricasServicio(conn,
      owner_tablas,
      tableTargetMetricasPerformance,
      metricasArray,
      "APPEND")
    insertMetricasServicio(conn,
      owner_tablas,
      tableTargetMetricasPerformance+"_log",
      metricasArray,
      "APPEND")
    logger.info(s"Se insertaron las metricas para el servicio:${id_nodo_servicio}")
  }

  /**
   * Ejecuta comandos unix en el servidor.
   * @example {{{
   *  val command=Seq("sh",RutaBinNifi+shellScript)
   *  val arguments=Seq("status")
   *  val msg_retorno=execShell(command,arguments)
   * }}}
   *
   * @param seqCommand Secuencia del tipo String con el comando de ejecución.
   * @param seqArguments Secuencia del tipo String con los argumentos de ejecución del comando

   * @return Retorna el mensaje de salida de la ejecución del comando.
   * @group util_funs
   */
  def ejecutaShell(seqCommand:Seq[String],seqArguments:Seq[String] ):String={
    val seqEjec=seqCommand++seqArguments
    val msg_retorno=seqEjec.!!
    msg_retorno
  }

  /**
   * Evalúa el mensaje de salida obtenido de la ejecución de nifi.sh status para determinar el estado actual de Apache Nifi.
   * @example {{{
   *  val command=Seq("sh",RutaBinNifi+shellScript)
   *  val arguments=Seq("status")
   *  val msg_retorno=execShell(command,arguments)
   * }}}
   *
   * @param msg_retorno Mensaje de salida para evaluación de estado del componente Apache Nifi.

   * @return Retorna el mensaje de salida de la ejecución del comando.
   * @group util_funs
   */
  def obtieneEstadoNifi(msg_retorno:String):Array[String]={
    val arrayStatusNifi= if(msg_retorno.toLowerCase().trim.contains("currently running")){
      val desc_estado_nifi="OK"
      val cod_msg_nifi="0"
      val des_msg_nifi="Apache Nifi en ejecucion."
      val listStatusNifi=List(desc_estado_nifi,cod_msg_nifi,des_msg_nifi)
      listStatusNifi.toArray
    } else {
      if(msg_retorno.toLowerCase().trim.contains("not running")){
        val desc_estado_nifi="ERROR"
        val cod_msg_nifi="1"
        val des_msg_nifi="Apache Nifi no se encuentra en ejecucion."
        val listStatusNifi=List(desc_estado_nifi,cod_msg_nifi,des_msg_nifi)
        listStatusNifi.toArray
      }
      else{
        val desc_estado_nifi="ERROR"
        val cod_msg_nifi="2"
        val des_msg_nifi="Apache Nifi se encuentra en ejecucion pero UI no se encuentra disponible."
        val listStatusNifi=List(desc_estado_nifi,cod_msg_nifi,des_msg_nifi)
        listStatusNifi.toArray}
    }
    arrayStatusNifi
  }


  /**
   * Obtiene metricas del tipo General del servicio Apache Nifi.
   * @example {{{
   *   val processorLoadAverage=obtieneMetricasNifiGeneral(parsedSystemDiagnostics,"processorLoadAverage")
   * }}}
   *
   * @param parsedSystemDiagnostics JSON Parse respuesta de consulta a traves Nifi API - System Diagnostics.
   * @param metrica Metrica a consultar.

   * @return Valor de metrica : String.
   * @group util_funs
   */
  def obtieneMetricasNifiGeneral(parsedSystemDiagnostics:JValue,metrica:String):String={
    implicit val formats = DefaultFormats
    def valor_metrica() : String = {
      (parsedSystemDiagnostics \ "systemDiagnostics" \ "aggregateSnapshot" \ s"${metrica}" ).extract[String]
    }
    valor_metrica
  }

  /**
   * Obtiene metricas de uso de almacenamiento por Repositorio del servicio Apache Nifi.
   *
   * @example {{{
   *  val provenanceRepositoryStorageUtilization=obtieneMetricasNifiRepositoryStorageUsage(parsedSystemDiagnostics,
   *       "provenance",
   *       "utilization").replace("%",
   *       "").toInt
   * }}}
   * @param parsedSystemDiagnostics JSON Parse respuesta de consulta a traves Nifi API - System Diagnostics.
   * @param typeRepository Tipo de repository (FlowFileRepository, ContentRepository, ProvenanceRepository)
   * @param metrica Metrica a consultar.
   * @return Valor de metrica : String.
   * @group util_funs
   */
  def obtieneMetricasNifiRepositoryStorageUsage(parsedSystemDiagnostics:JValue,typeRepository:String,metrica:String):String={
    implicit val formats = DefaultFormats
    def valor_metrica() : String = {
      (parsedSystemDiagnostics \ "systemDiagnostics" \ "aggregateSnapshot" \ s"${typeRepository}RepositoryStorageUsage" \ s"${metrica}" ).extract[String]
    }
    valor_metrica
  }

  /**
   * Obtiene indicador de evaluacion del valor de metrica segun los umbrales definidos para NORMAL,WARNING y CRITICAL.
   *
   * @example {{{
   *    val indicadorProvenanceRepositoryStorageUtilization=obtieneIndicadorEvalValorMetrica(provenanceRepositoryStorageUtilization,
   *       rangoMaxNormal,
   *       rangoMaxWarning)
   * }}}
   * @param valorMetrica Valor de metrica.
   * @param rangoMaxNormal Valor umbral maximo para la categorizacion NORMAL.
   * @param rangoMaxWarning Metrica a Valor umbral maximo para la categorizacion WARNING.
   * @return Resultado de evaluacion de valor de metrica (indicador metrica) : Array.
   * @group util_funs
   */
  def obtieneIndicadorEvalValorMetrica(valorMetrica:Int,rangoMaxNormal:Int,
                                       rangoMaxWarning:Int):Array[Any]={
    val arrayStatusNifi=if(valorMetrica<=rangoMaxNormal) {
      val indicador_metrica=10
      val des_indicador_metrica="NORMAL"
      val listIndicadorMetrica=List(indicador_metrica,des_indicador_metrica)
      listIndicadorMetrica.toArray
    } else {
    if(valorMetrica>rangoMaxNormal && valorMetrica<=rangoMaxWarning) {
      val indicador_metrica=20
      val des_indicador_metrica="WARNING"
      val listIndicadorMetrica=List(indicador_metrica,des_indicador_metrica)
      listIndicadorMetrica.toArray
    }
    else {
      val indicador_metrica=30
      val des_indicador_metrica="CRITICAL"
      val listIndicadorMetrica=List(indicador_metrica,des_indicador_metrica)
      listIndicadorMetrica.toArray
    }
    }
    arrayStatusNifi
  }

  /**
   * Realiza insercion de valores resultantes de la evaluacion del estado general del servicio.
   *
   * @example {{{
   * insertStatusGeneral(conn,user,tableTargetStatusGeneral,arrayValuesStatusGeneral,"APPEND")
   * }}}
   * @param conn Conexion Oracle.
   * @param user Usuario de conexion tablas Oracle (Owner tablas).
   * @param table Tabla destino de insercion.
   * @param arrayValues Valores a insertarse en la tabla de monitoreo de Estado del servicio de Ingesta.
   * @return Resultado de insercion en tabla Oracle : Unit.
   * @group util_funs
   */
  def insertStatusGeneral(conn:Connection,user:String,table:String,
                          arrayValues:Array[Any],modeInsert:String):Unit={
    val tableTarget=s"${user.toUpperCase()}.${table.toUpperCase()}"
    val sqlInsertTabla=
      s"""
         | INSERT INTO ${tableTarget}
         | VALUES('${arrayValues(0)}',
         | '${arrayValues(1)}',
         | '${arrayValues(2)}',
         | '${arrayValues(3)}',
         | '${arrayValues(4)}',
         | ${arrayValues(5)},
         | '${arrayValues(6)}',
         | '${arrayValues(7)}') """.stripMargin

    if(modeInsert.toUpperCase()=="OVERWRITE") OracleUtil.execInsertWithCommitOverwrite(conn,tableTarget,sqlInsertTabla)
    else OracleUtil.execInsertWithCommitAppend(conn,sqlInsertTabla)
  }
  /**
   * Realiza insercion de valores resultantes de la evaluacion de metricas del servicio.
   *
   * @example {{{
   * insertMetricasServicio(conn,user,tableTargetMetricasPerformance,metricasArray,"APPEND")
   * }}}
   * @param conn Conexion Oracle.
   * @param user Usuario de conexion tablas Oracle (Owner tablas).
   * @param table Tabla destino de insercion.
   * @param arrayValues Valores a insertarse en la tabla de monitoreo de Estado del servicio de Ingesta.
   * @return Resultado de insercion en tabla Oracle : Unit.
   * @group util_funs
   */
  def insertMetricasServicio(conn:Connection,user:String,table:String,
                             arrayValues:Array[Any],modeInsert:String):Unit={
    val tableTarget=s"${user.toUpperCase()}.${table.toUpperCase()}"
    val sqlInsertTabla=
      s"""
         | INSERT INTO ${tableTarget}
         | VALUES('${arrayValues(0)}',
         | '${arrayValues(1)}',
         | '${arrayValues(2)}',
         | '${arrayValues(3)}',
         | '${arrayValues(4)}',
         | '${arrayValues(5)}',
         |  ${arrayValues(6)},
         |  ${arrayValues(7)},
         | '${arrayValues(8)}',
         | '${arrayValues(9)}') """.stripMargin

    if(modeInsert.toUpperCase()=="OVERWRITE") OracleUtil.execInsertWithCommitOverwrite(conn,tableTarget,sqlInsertTabla)
    else OracleUtil.execInsertWithCommitAppend(conn,sqlInsertTabla)
  }


}
