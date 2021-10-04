package pe.com.claro.fiscal

import com.typesafe.config.Config

class Settings(config: Config) extends Serializable {
  validaParamInput(config)

  /* Obtiene configuracion de archivo file */
  val rutaLog=config.getString("exec.rutaLog")

  val jdbcUrl = config.getString("oracle.jdbcURL")
  val user = config.getString("oracle.user")
  val pass = config.getString("oracle.pass")

  val tableTargetStatusGeneral = config.getString("target.tableTargetStatusGeneral")
  val tableTargetMetricasPerformance = config.getString("target.tableTargetMetricasPerformance")

  val rangoMaxNormalInput = config.getString("metricas.rangoMaxNormalInput")
  val rangoMaxWarningInput = config.getString("metricas.rangoMaxWarningInput")

  val rutaBinNifi = config.getString("nifi.rutaBin")
  val puertoNifi = config.getString("nifi.puerto")
  val userADNifi=config.getString("nifi.userad")
  val passADNifi=config.getString("nifi.passad")

  val rutaShellsAirflow = config.getString("airflow.rutaShells")
  val puertoAirflow = config.getString("airflow.puerto")

  def validaParamInput(config: Config):Unit={
    var cntParamsReqFalt=0
    if(config.getString("oracle.jdbcURL").isEmpty) {
      println("Se esperaba especif. cadena de conexion Oracle: oracle.jdbcURL Por favor completar este parametro")
      cntParamsReqFalt+=1
    }
    if(config.getString("oracle.user").isEmpty) {
      println("Se esperaba especif. usuario Oracle: oracle.user Por favor completar este parametro")
      cntParamsReqFalt+=1
    }
    if(config.getString("oracle.pass").isEmpty) {
      println("Se esperaba especif. password Oracle: oracle.pass Por favor completar este parametro")
      cntParamsReqFalt+=1
    }
    if(config.getString("target.tableTargetStatusGeneral").isEmpty) {
      println("Se esperaba especif. tabla Oracle Target Status General: target.tableTargetStatusGeneral Por favor completar este parametro")
      cntParamsReqFalt+=1
    }
    if(config.getString("target.tableTargetMetricasPerformance").isEmpty) {
      println("Se esperaba especif. tabla Oracle Target Metricas: target.tableTargetMetricasPerformance Por favor completar este parametro")
      cntParamsReqFalt+=1
    }
    if(config.getString("metricas.rangoMaxNormalInput").isEmpty) {
      println("Se esperaba especif. valor maximo para rango normal: metricas.rangoMaxNormalInput Por favor completar este parametro")
      cntParamsReqFalt+=1
    }
    if(config.getString("metricas.rangoMaxWarningInput").isEmpty) {
      println("Se esperaba especif. valor maximo para rango warning: metricas.rangoMaxWarningInput Por favor completar este parametro")
      cntParamsReqFalt+=1
    }
    if(config.getString("nifi.rutaBin").isEmpty) {
      println("Se esperaba especif. ruta bin de ejecucion nifi: nifi.rutaBin Por favor completar este parametro")
      cntParamsReqFalt+=1
    }
    if(config.getString("nifi.puerto").isEmpty) {
      println("Se esperaba especif. Puerto nifi: nifi.puerto Por favor completar este parametro")
      cntParamsReqFalt+=1
    }
    if(config.getString("airflow.rutaShells").isEmpty) {
      println("Se esperaba especif. Ruta shells Airflow: airflow.rutaShells Por favor completar este parametro")
      cntParamsReqFalt+=1
    }
    if(config.getString("airflow.puerto").isEmpty) {
      println("Se esperaba especif. Puerto Airflow: airflow.puerto Por favor completar este parametro")
      cntParamsReqFalt+=1
    }

    if(cntParamsReqFalt>0){
      println(s"Existen ${cntParamsReqFalt} parametros faltantes requeridos para la ejecucion.")
      System.exit(10)
    }


  }

}


