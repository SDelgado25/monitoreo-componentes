package pe.com.claro.fiscal

import java.net.InetAddress
import java.sql.Connection

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.logging.log4j.{LogManager, ThreadContext}
import pe.com.claro.util.OracleUtil

object ExecMonitoreoCmpIngesta {

  val logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val env=args(0)
    val flgSecuredConnection=args(1)
    val flgExecMonitoreoServIngesta=args(2)
    val conf: Config = ConfigFactory.load()
    val settings: Settings = new Settings(conf)

    /* Parametros obtenidos de archivo de configuracion*/
    /*Parametros conexion Oracle*/
    val jdbcUrl=settings.jdbcUrl
    val user = settings.user
    val pass=settings.pass
    /*Parametros tabla Oracle destino*/
    var tableTargetStatusGeneral=settings.tableTargetStatusGeneral
    val owner_tablas=tableTargetStatusGeneral.split("\\.")(0)
    tableTargetStatusGeneral=tableTargetStatusGeneral.split("\\.")(1)
    var tableTargetMetricasPerformance=settings.tableTargetMetricasPerformance
    tableTargetMetricasPerformance=tableTargetMetricasPerformance.split("\\.")(1)
    /*Parametros Log*/
    val rutaLog=settings.rutaLog.replace("env",env)

    val localhost = InetAddress.getLocalHost
    val ip_origen = localhost.getHostAddress
    val hostname_origen=localhost.getCanonicalHostName
    val nodo=s"${ip_origen} (${hostname_origen})"
    val servicio_nifi="NIFI"
    val servicio_airflow="AIRFLOW"
    val id_nodo=ip_origen.split("\\.")(3)
    val id_nodo_servicio_nifi=s"${servicio_nifi}_${id_nodo}"
    val id_nodo_servicio_airflow=s"${servicio_airflow}_${id_nodo}"

    /*Configuracion Archivo Log*/
    val nombreFichero="cmp-common-monitoreo-componentes-scala"
    ThreadContext.put("logFileName", nombreFichero)
    ThreadContext.put("logPath", rutaLog)
    println(s"Se inicializo log: ${rutaLog}/${nombreFichero}_TRAZADO.log")

    /**3. Conexion a BD Oracle*/
    logger.info("Establece conexion con BD Oracle")
    OracleUtil.loadDriverClass
    val conn:Connection=OracleUtil.getConnection(jdbcUrl,user,pass)
    /**Crea tabla si no existe*/
    logger.info("Inicia creacion de tablas (si no existen)")
    creaTablasStatusGeneral(conn,
      owner_tablas,
      tableTargetStatusGeneral,
      "Proyecto Fiscal: Tabla de status general de servicios de Ingesta. (Ultima foto)")
    creaTablasStatusGeneral(conn,
      owner_tablas,
      tableTargetStatusGeneral+"_log",
      "Proyecto Fiscal: Tabla de status general de servicios de Ingesta. (Historico)")
    creaTablasMetricasServicio(conn,
      owner_tablas,
      tableTargetMetricasPerformance,
      "Proyecto Fiscal: Tabla de metricas de comportamiento de servicios de Ingesta. (Ultima foto)")
    creaTablasMetricasServicio(conn,
      owner_tablas,
      tableTargetMetricasPerformance+"_log",
      "Proyecto Fiscal: Tabla de metricas de comportamiento de servicios de Ingesta. (Historico)")

    logger.info("Evalua si Apache Nifi utiliza autenticacion AD si se ingreso usuario y pass")
    if(flgSecuredConnection=="S" && (settings.userADNifi.isEmpty || settings.passADNifi.isEmpty)){
      conn.close()
      logger.error("Se especifico que los servicios de Ingesta existen en Entorno Seguro (AD)," +
        " pero no se especificaron los parametros de autenticacion requeridos")
      System.exit(11)
    }
    else{
      if(flgExecMonitoreoServIngesta.toUpperCase=="ALL"){
        OracleUtil.execDeleteByService(conn,
          s"${owner_tablas}.${tableTargetStatusGeneral}".toUpperCase(),
          "servicio",id_nodo_servicio_nifi)
        OracleUtil.execDeleteByService(conn,
          s"${owner_tablas}.${tableTargetMetricasPerformance}".toUpperCase(),
          "des_servicio",id_nodo_servicio_nifi)
        OracleUtil.execDeleteByService(conn,
          s"${owner_tablas}.${tableTargetStatusGeneral}".toUpperCase(),
          "servicio",id_nodo_servicio_airflow)
        OracleUtil.execDeleteByService(conn,
          s"${owner_tablas}.${tableTargetMetricasPerformance}".toUpperCase(),
          "des_servicio",id_nodo_servicio_airflow)
        ExecMonitoreoCmpNifi.main(env,flgSecuredConnection,settings,conn,logger)
        ExecMonitoreoCmpAirflow.main(env,settings,conn,logger)
      }
      if(flgExecMonitoreoServIngesta.toUpperCase=="NIFI"){
        OracleUtil.execDeleteByService(conn,
          s"${owner_tablas}.${tableTargetStatusGeneral}".toUpperCase(),
          "servicio",id_nodo_servicio_nifi)
        OracleUtil.execDeleteByService(conn,
          s"${owner_tablas}.${tableTargetMetricasPerformance}".toUpperCase(),
          "des_servicio",id_nodo_servicio_nifi)
        ExecMonitoreoCmpNifi.main(env,flgSecuredConnection,settings,conn,logger)
      }
      if(flgExecMonitoreoServIngesta.toUpperCase=="AIRFLOW"){
        OracleUtil.execDeleteByService(conn,
          s"${owner_tablas}.${tableTargetStatusGeneral}".toUpperCase(),
          "servicio",id_nodo_servicio_airflow)
        OracleUtil.execDeleteByService(conn,
          s"${owner_tablas}.${tableTargetMetricasPerformance}".toUpperCase(),
          "des_servicio", id_nodo_servicio_airflow)
        ExecMonitoreoCmpAirflow.main(env,settings,conn,logger)
      }
      conn.close()
      logger.info("Finalizo ejecucion de monitoreo de servicios de Ingesta.")
      System.exit(0)
    }

  }

  def creaTablasStatusGeneral(conn:Connection,user:String,table:String,comentarioTable:String):Unit={
    println(s"Usuario:${user}")
    println(s"Tabla:${table}")
    val sqlCreateTabla=
      s"""
         | create table ${user}.${table}
         |    (
         |    ambiente varchar2(100) NOT NULL,
         |    nodo_ip varchar2(100) NOT NULL,
         |    nodo_host varchar2(100) NOT NULL,
         |    servicio varchar2(100) NOT NULL,
         |    estado varchar2(20) NOT NULL,
         |    cod_estado number NOT NULL,
         |    des_estado varchar2(100) NOT NULL,
         |    fecha varchar2(100) NOT NULL
         |    )
         |""".stripMargin
    OracleUtil.execCreateTable(conn,user,table,sqlCreateTabla,comentarioTable)
  /*  OracleUtil.execCommentColumnTable(conn,user,table,"ambiente","Ambiente al que pertenece el nodo de Ingesta")
    OracleUtil.execCommentColumnTable(conn,user,table,"nodo_ip","IP del Servidor en el cual se ejecutan los servicios Nifi y Airflow de Ingesta")
    OracleUtil.execCommentColumnTable(conn,user,table,"nodo_host","Hostname del Servidor en el cual se ejecutan los servicios Nifi y Airflow de Ingesta")
    OracleUtil.execCommentColumnTable(conn,user,table,"servicio","Identificador Servicio y nodo en el que se ejecuta: (Identificador de servicio)_(Identificador de nodo)")
    OracleUtil.execCommentColumnTable(conn,user,table,"estado","Estado actual del servicio")
    OracleUtil.execCommentColumnTable(conn,user,table,"cod_estado","Codigo de retorno de comprobacion de estado del servicio. Posibles valores:0,1,2")
    OracleUtil.execCommentColumnTable(conn,user,table,"des_estado","Mensaje de retorno de comprobacion de estado del servicio")
    OracleUtil.execCommentColumnTable(conn,user,table,"fecha","Fecha de comprobacion de estado del servicio")*/
    logger.info(s"Se creo correctamente tabla ${user}.${table}")
  }

  def creaTablasMetricasServicio(conn:Connection,user:String,table:String,comentarioTable:String):Unit={
    val sqlCreateTabla=
      s"""
         | create table ${user}.${table}
         |    (
         |    ambiente varchar2(100) NOT NULL,
         |    nodo_ip varchar2(100) NOT NULL,
         |    nodo_host varchar2(100) NOT NULL,
         |    des_servicio varchar2(100) NOT NULL,
         |    cod_metrica varchar2(100) NOT NULL,
         |    des_metrica varchar2(100) NOT NULL,
         |    valor_metrica number(16,2) NOT NULL,
         |    indicador_metrica number NOT NULL,
         |    des_indicador_metrica varchar2(100) NOT NULL,
         |    fecha varchar2(100) NOT NULL
         |    )
         |""".stripMargin
/*    OracleUtil.execCreateTable(conn,user,table,sqlCreateTabla,comentarioTable)
    OracleUtil.execCommentColumnTable(conn,user,table,"ambiente","Ambiente al que pertenece el nodo de Ingesta")
    OracleUtil.execCommentColumnTable(conn,user,table,"nodo_ip","IP del Servidor en el cual se ejecutan los servicios Nifi y Airflow de Ingesta")
    OracleUtil.execCommentColumnTable(conn,user,table,"nodo_host","Hostname del Servidor en el cual se ejecutan los servicios Nifi y Airflow de Ingesta")
    OracleUtil.execCommentColumnTable(conn,user,table,"des_servicio","Servicio de Ingesta: Apache Nifi/ Apache Airflow:(Identificador de servicio)_(Identificador de nodo)")
    OracleUtil.execCommentColumnTable(conn,user,table,"cod_metrica","Codigo identificador de la metrica: (Abreviatura servicio)_(Abreviatura Metrica)_(Identificador IP Nodo)")
    OracleUtil.execCommentColumnTable(conn,user,table,"des_metrica","Descripcion de metrica de comportamiento del servicio")
    OracleUtil.execCommentColumnTable(conn,user,table,"valor_metrica","Valor de metrica de comportamiento del servicio")
    OracleUtil.execCommentColumnTable(conn,user,table,"indicador_metrica","Indicador de metrica NORMAL:10, WARNING:20, CRITICAL:30")
    OracleUtil.execCommentColumnTable(conn,user,table,"des_indicador_metrica","Descripcion Indicador de metrica de comportamiento del servicio (NORMAL, WARNING,CRITICAL)")
    OracleUtil.execCommentColumnTable(conn,user,table,"fecha","Fecha de evaluacion de metrica de comportamiento del servicio")
   */ logger.info(s"Se creo correctamente tabla ${user}.${table}")
  }


}
