-- Databricks notebook source
USE lab.salud

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Calcular el total de estadía de los pacientes en hospitales de cada distrito.

-- COMMAND ----------

SELECT distrito, SUM(dias_internacion) AS estadia FROM internaciones 
INNER JOIN hospitales ON internaciones.id_hospital = hospitales.id_hospital
GROUP BY distrito
ORDER BY estadia DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Listar los nombres de los hospitales, distritos y la cantidad de camas disponibles que tuvieron internaciones durante el mes de enero del 2022. Incluir todos los hospitales, incluso aquellos sin internaciones registradas.
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT DISTINCT nombre, distrito, cantidad_camas 
FROM hospitales 
  LEFT JOIN internaciones ON hospitales.id_hospital = internaciones.id_hospital
WHERE fecha ILIKE '2022-01-%';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Mostrar el nombre y apellido de los pacientes (en un solo campo), el nombre del hospital en el que fueron internados, fecha de internacion, dias de internacion y tipo de egreso

-- COMMAND ----------

SELECT 
  concat(pacientes.nombre,' ', pacientes.apellido) AS nombre_completo, 
  hospitales.nombre AS hospital, 
  internaciones.fecha, 
  internaciones.dias_internacion, 
  internaciones.tipo_egreso 
FROM pacientes 
  INNER JOIN internaciones USING(id_paciente)
  INNER JOIN hospitales USING(id_hospital)
WHERE internaciones.id_hospital = hospitales.id_hospital;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Generar un reporte de las internaciones del año 2022 que detalle:
-- MAGIC - Nombre del hospital
-- MAGIC - Distrito
-- MAGIC - Diagnostico (id y nombre en un solo campo)
-- MAGIC - Servicio (nombre). En caso de no encontrarlo poner "Sin Especificar"
-- MAGIC - Identificador de internacion
-- MAGIC - Tipo de egreso
-- MAGIC - Dias de internación

-- COMMAND ----------

SELECT 
  hospitales.nombre AS hospital, 
  distrito,
  concat(internaciones.id_diagnostico,' ',coalesce(diagnosticos.diagnostico,'')) AS diagnostico, --cuando diagnostico es NULL entonces concatena un espacio vacio. Porque sino, aparece NULL
  coalesce(servicio, 'Sin Especificar') AS servicio,
  id_internacion,
  tipo_egreso,
  dias_internacion
FROM internaciones 
  LEFT JOIN diagnosticos ON internaciones.id_diagnostico = diagnosticos.id_diagnostico
  LEFT JOIN hospitales ON internaciones.id_hospital = hospitales.id_hospital
WHERE fecha ILIKE '2022%';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Continuando con el ejercicio anterior, incorporar la descripcion del sector pero en listado solo debe contener las intercaciones del año 2019

-- COMMAND ----------

SELECT 
  hospitales.nombre AS hospital, 
  distrito, 
  concat(internaciones.id_diagnostico,' ',coalesce(diagnosticos.diagnostico,'')) as diagnostico, --cuando diagnostico es NULL entonces concatena un espacio vacio. Porque sino, aparece NULL
  coalesce(servicio, 'Sin Especificar') AS servicio,
  id_internacion,
  tipo_egreso,
  dias_internacion,
  sector
FROM internaciones 
  LEFT JOIN diagnosticos USING(id_diagnostico)
  LEFT JOIN hospitales USING(id_hospital)
  LEFT JOIN sectores USING(id_sector)
WHERE internaciones.fecha ILIKE '2019%';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Obener la cantidad de internaciones, promedio de dias de internacion por sexo, distrito y tipo de egreso para los años 2021 y 2022. El listado debe estar ordenado por distrito, tipo de egreso y sexo

-- COMMAND ----------

SELECT 
  sexo,
  distrito,
  tipo_egreso,
  COUNT(id_internacion) AS cant_internaciones,
  ROUND(AVG(dias_internacion),0) AS prom_dias_internacion
FROM internaciones 
  INNER JOIN pacientes ON internaciones.id_paciente = pacientes.id_paciente
  INNER JOIN hospitales ON internaciones.id_hospital = hospitales.id_hospital
WHERE fecha LIKE '2021%' OR fecha LIKE '2022%'
GROUP BY sexo, distrito, tipo_egreso
ORDER BY distrito, tipo_egreso, sexo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Listar el nombre completo de los pacientes que sufrieron traslados. Además del nombre completo el listado debe mostrar:
-- MAGIC - fecha de internación
-- MAGIC - diagnostico
-- MAGIC - servicio
-- MAGIC - sector
-- MAGIC - dias de internacion
-- MAGIC *Normalizar todos los campos que considere*

-- COMMAND ----------

SELECT 
  initcap(concat(trim(pacientes.nombre),' ', trim(pacientes.apellido))) AS nombre_completo,
  fecha,
  initcap(coalesce(concat(internaciones.id_diagnostico,' ',coalesce(diagnosticos.diagnostico,'')),'Sin especificar')) as diagnostico, --cuando diagnostico es NULL entonces concatena un espacio vacio. Porque sino, aparece NULL
  coalesce(initcap(trim(servicio)), 'Sin especificar') AS servicio,
  coalesce(initcap(trim(sector)), 'Sin especificar') AS sector,
  dias_internacion
FROM internaciones
  INNER JOIN pacientes ON pacientes.id_paciente = internaciones.id_paciente
  LEFT JOIN diagnosticos ON diagnosticos.id_diagnostico = internaciones.id_diagnostico
  LEFT JOIN sectores ON sectores.id_sector = internaciones.id_sector
WHERE tipo_egreso ILIKE '%traslado%';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Calcular la cantidad de pacientes que se internaron en en los años 2020 o 2021 de nacionalidad Argentina y que la edad este entre los 25 y 35 años. Listar la cantidad de pacientes por tipo de egreso.

-- COMMAND ----------

SELECT tipo_egreso, COUNT(internaciones.id_paciente) AS cantidad_pacientes
FROM internaciones INNER JOIN pacientes USING(id_paciente)
WHERE (fecha LIKE '2020%' OR fecha LIKE '2021%') AND nacionalidad ILIKE '%argentina%' AND edad BETWEEN 25 AND 35
GROUP BY tipo_egreso
ORDER BY cantidad_pacientes DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. ¿A qué distrito pertenece el hospital que mayor cantidad de internaciones durante el mes de Agosto del 2021?

-- COMMAND ----------

SELECT internaciones.id_hospital, distrito, COUNT(id_internacion) 
FROM internaciones INNER JOIN hospitales ON internaciones.id_hospital = hospitales.id_hospital 
WHERE fecha LIKE '2021-08%'
GROUP BY internaciones.id_hospital, distrito
HAVING COUNT(id_internacion) = ( -- MAXIMA cantidad internaciones en Agosto del 2021
  SELECT COUNT(id_internacion) 
  FROM internaciones INNER JOIN hospitales ON internaciones.id_hospital = hospitales.id_hospital 
  WHERE fecha LIKE '2021-08%'
  GROUP BY internaciones.id_hospital, distrito
  ORDER BY COUNT(id_internacion) DESC
  LIMIT 1);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10. Obtener tasa de mortalidad durante el año 2022 considerando que la población estimada para el mismo año para la ciudad de Rosario es de 995.497 habitantes

-- COMMAND ----------

SELECT
  COUNT(id_paciente)*1000/995497 AS tasa_mortalidad_2022
FROM internaciones
WHERE tipo_egreso  ILIKE '%defuncion%' AND (fecha + CAST(dias_internacion AS INT) ILIKE '2022%')

-- COMMAND ----------

SELECT 
  (SELECT COUNT(id_paciente)
  FROM internaciones
  WHERE tipo_egreso  ILIKE '%defuncion%' AND (fecha + CAST(dias_internacion AS INT) ILIKE '2022%')) 
* 1000
/ 995497 AS tasa_mortalidad_2022;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11. Cual es el mayor motivo (diagnóstico) y servicio de las defunciones para todos los años excepto el 2020

-- COMMAND ----------

SELECT internaciones.id_diagnostico, diagnosticos.diagnostico, diagnosticos.servicio, COUNT(internaciones.id_internacion)
FROM internaciones LEFT JOIN diagnosticos USING(id_diagnostico)
WHERE fecha NOT LIKE '2020%' AND tipo_egreso ILIKE '%defuncion%'
GROUP BY internaciones.id_diagnostico, diagnosticos.diagnostico, diagnosticos.servicio
ORDER BY COUNT(internaciones.id_diagnostico) DESC
LIMIT 1;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12. Calcular el porcentaje de ocupación de camas del hospital *Centro de Salud Santa Lucia* para el dia 2022-05-23.
-- MAGIC - Porcentaje de ocupación = (cantidad de internaciones / cantidad camas disponibles) * 100

-- COMMAND ----------

SELECT
  COUNT(id_internacion)*100/(SELECT cantidad_camas FROM hospitales WHERE nombre ILIKE '%Centro de Salud Santa Lucia%') AS porcentaje_ocupacion
FROM internaciones 
  INNER JOIN hospitales USING(id_hospital)
WHERE fecha ILIKE '2022-05-23' AND nombre ILIKE '%Centro de Salud Santa Lucia%';

-- COMMAND ----------

SELECT
  (SELECT COUNT(id_internacion) 
  FROM internaciones 
    INNER JOIN hospitales ON internaciones.id_hospital = hospitales.id_hospital 
  WHERE fecha ILIKE '2022-05-23' AND nombre ILIKE '%Centro de Salud Santa Lucia%')
/
  (SELECT cantidad_camas 
  FROM hospitales 
  WHERE nombre ILIKE '%Centro de Salud Santa Lucia%')
*100
AS porcentaje_ocupacion;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 13. Contar la cantidad de diagnosticos distintos por cada servicio. Mostrar sólo aquellos servicios con más de 50 diagnosticos.

-- COMMAND ----------

SELECT servicio, COUNT(id_diagnostico) FROM diagnosticos
GROUP BY servicio
HAVING COUNT(id_diagnostico) > 50
ORDER BY COUNT(id_diagnostico) DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 14. ¿Cuál fue el año con mayor cantidad de internaciones?

-- COMMAND ----------

SELECT
  extract(year FROM fecha) AS anno,
  COUNT(id_internacion) AS cantidad_internaciones
FROM internaciones
GROUP BY anno
ORDER BY cantidad_internaciones DESC
LIMIT 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 15. Obtener la cantidad de dias de internacion por periodo (año y mes). No se deben tener en cuenta los tipos de egreso *Traslado*

-- COMMAND ----------

SELECT
  extract(year FROM fecha) AS anno,
  extract(month FROM fecha) AS mes,
  SUM(dias_internacion) as cantidad_dias_internacion
FROM internaciones
WHERE tipo_egreso NOT ILIKE '%traslado%'
GROUP BY anno, mes
ORDER BY anno, mes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 16. Obtener los distritos que tienen mas de 2000 camas 

-- COMMAND ----------

SELECT distrito, SUM(cantidad_camas) AS total_camas FROM hospitales
GROUP BY distrito
HAVING total_camas > 2000;
