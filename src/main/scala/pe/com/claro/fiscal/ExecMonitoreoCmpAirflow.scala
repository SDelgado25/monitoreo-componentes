package pe.com.claro.fiscal

import java.net.InetAddress
import java.sql.Connection
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import net.liftweb.json.{DefaultFormats, JValue, parse}
import org.apache.logging.log4j.Logger
import pe.com.claro.util.OracleUtil

import scala.sys.process._

/** [[pe.com.claro.fiscal.ExecMonitoreoCmpAirflow]] : Evalua estado del servicio Apache Airflow,
 * metricas de performance y almacena los valores obtenidos en tablas Oracle.
 *
 * @groupname main_method_funcs Metodo de ejecucion principal en el Objeto
 * @groupname util_funs Funciones de Obtencion de Configuración
 * @groupname select_cols_funcs Funciones de Seleccion de columnas en DF
 */

object ExecMonitoreoCmpAirflow {
  /** Obtiene metricas del estado y performance del Componente Apache Nifi.
   * @param ambienteActual: Ambiente actual
   * @param settings:Importa configuracion de clase Setting (Lectura archivo config)
   * @note ''Métricas divididas en dos grupos:
   *      1. Estado General del Componente por servidor de ingesta
   *      2. Métricas de performance del Componente por servidor de ingesta''
   * @author Susan Delgado
   * @version 1.0
   * @group main_method_funcs
   */
  def main(ambienteActual:String,settings: Settings,conn:Connection,logger:Logger): Unit = {
    logger.info("Inicia ejecucion de monitoreo de servicio Apache Airflow")
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

    logger.info("Obtiene *Parametros Servicio Apache Airflow")
    val ruta_shells_airflow = settings.rutaShellsAirflow
    val puertoAirflow=settings.puertoAirflow

    logger.info("Obtiene informacion de Servidor de ejecución actual")
    val localhost = InetAddress.getLocalHost
    val ip_origen = localhost.getHostAddress
    val hostname_origen=localhost.getCanonicalHostName
    val nodo=s"${ip_origen} (${hostname_origen})"
    val servicio="airflow"
    val id_nodo=ip_origen.split("\\.")(3)
    val id_nodo_servicio=s"${servicio}_${id_nodo}"

    logger.info("Obtiene fecha actual")
    val insFecha=LocalDateTime.now()
    val formatter=DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
    val fecha=formatter.format(insFecha)

    logger.info("1. Estado General del Componente por servidor de ingesta")
    logger.info(" Configuración de comando de ejecución y argumentos")
    val shellScript="airflow_status.sh"
    var command=Seq("sh",ruta_shells_airflow+shellScript)
    var arguments=Seq("")
    logger.info("Consulta estado de Apache Airflow")
    var msg_retorno=ejecutaShell(command,arguments)
    logger.info("Evalua estado de Apache Airflow")
    val arrayStatusAirflow=obtieneEstadoAirflow(msg_retorno)
    val estadoAirflow=arrayStatusAirflow(0)
    val codMsgAirflow=arrayStatusAirflow(1).toInt
    val desMsgAirflow=arrayStatusAirflow(2)
    logger.info(s"Estado del Componente Apache Airflow:${arrayStatusAirflow(0)}")
    logger.info(s"Codigo Estado del Componente Apache Airflow:${arrayStatusAirflow(1)}")
    logger.info(s"Descripcion detallada Estado del Componente Apache Airflow:${arrayStatusAirflow(2)}")

    logger.info("Insercion a tablas de Monitoreo en Oracle")
    val listValuesStatusGeneral=List(env,ip_origen,hostname_origen,
      id_nodo_servicio, estadoAirflow,codMsgAirflow,
      desMsgAirflow, fecha)
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
    logger.info("Obtiene Health de Airflow")
    command=Seq("curl","-H","'Content-Type: application/json'")
    val prefixURL="http://"
    val urlAirflow=prefixURL+ip_origen+":"+puertoAirflow+"/health"
    arguments=Seq(urlAirflow)
    println("arguments")
    msg_retorno=ejecutaShell(command,arguments)

    logger.info("Obtencion de valores desde del JSON Obtenido de la consulta de health Airflow")
    val parsedHealth = parse(msg_retorno)

    var arrayMetricaAirflow=obtieneMetricasAirflow(parsedHealth,"metadatabase")
    var valor_metrica=arrayMetricaAirflow(0)
    var indicador_metrica=arrayMetricaAirflow(1)
    var des_indicador_metrica=arrayMetricaAirflow(2)
    var des_estadoSubComp=arrayMetricaAirflow(3)

    logger.info(s"Estado de la BD de metadatos (metadatabase) Apache Airflow:${des_estadoSubComp}")
    var codMetrica=s"AIRFL_EST_METDB_${id_nodo}".toUpperCase()
    var descMetrica="Estado de la BD de metadatos (metadatabase) Apache Airflow"
    var metricasArray=List(env,ip_origen,hostname_origen,
      id_nodo_servicio, codMetrica,descMetrica,
      valor_metrica, indicador_metrica,des_indicador_metrica,
      fecha).toArray
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


    arrayMetricaAirflow=obtieneMetricasAirflow(parsedHealth,"scheduler")
    valor_metrica=arrayMetricaAirflow(0)
    indicador_metrica=arrayMetricaAirflow(1)
    des_indicador_metrica=arrayMetricaAirflow(2)
    des_estadoSubComp=arrayMetricaAirflow(3)

    logger.info(s"Estado del scheduler Apache Airflow:${des_estadoSubComp}")
    codMetrica=s"AIRFL_EST_SCHE_${id_nodo}".toUpperCase()
    descMetrica="Estado del scheduler Apache Airflow"
    metricasArray=List(env,ip_origen,hostname_origen,
      id_nodo_servicio, codMetrica,descMetrica,
      valor_metrica, indicador_metrica,des_indicador_metrica,
      fecha).toArray
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
  def obtieneEstadoAirflow(msg_retorno:String):Array[String]={
    val arrayStatusAirflow= if(msg_retorno.toLowerCase().trim.contains("pts")){
      val desc_estado_airflow="OK"
      val cod_msg_airflow="0"
      val des_msg_airflow="Apache Airflow en ejecucion."
      val listStatusAirflow=List(desc_estado_airflow,cod_msg_airflow,des_msg_airflow)
      listStatusAirflow.toArray
    } else {

        val desc_estado_airflow="ERROR"
        val cod_msg_airflow="1"
        val des_msg_airflow="Apache Airflow no se encuentra en ejecucion."
      val listStatusAirflow=List(desc_estado_airflow,cod_msg_airflow,des_msg_airflow)
      listStatusAirflow.toArray}
    arrayStatusAirflow
  }

  def obtieneMetricasAirflow(parsedHealth:JValue,componente:String):Array[Any]={
    implicit val formats = DefaultFormats
    def valor_estado() : String = {
      (parsedHealth \ componente \ "status" ).extract[String]
    }
    val arrayMetrica= if(valor_estado=="healthy"){
      val valor_metrica=0.00
      val indicador_metrica=10
      val des_indicador_metrica="NORMAL"
      val msg_retorno=s"${componente} de Apache Airflow activo y funcionando correctamente. Estado: Healthy"
      val listStatusComponente=List(valor_metrica,indicador_metrica,
        des_indicador_metrica,msg_retorno)
      listStatusComponente.toArray
    } else { val valor_metrica=1.00
      val indicador_metrica=30
      val des_indicador_metrica="CRITICAL"
      val msg_retorno=s"${componente} de Apache Airflow presenta problemas"
      val listStatusComponente=List(valor_metrica,indicador_metrica,
        des_indicador_metrica,msg_retorno)
      listStatusComponente.toArray
    }
    arrayMetrica
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
