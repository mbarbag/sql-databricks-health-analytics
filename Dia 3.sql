-- Databricks notebook source
use lab.salud

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Contar la cantidad de pacientes por grupo etario:
-- MAGIC - Menos de edad
-- MAGIC - Jovenes: entre 18 y 30 años
-- MAGIC - Adultos: entre 31 y 60 años
-- MAGIC - Adultos Mayores: > 60 años

-- COMMAND ----------

SELECT
  CASE
    WHEN edad BETWEEN 18 AND 30 THEN 'jovenes'
    WHEN edad BETWEEN 31 AND 60 THEN 'adultos'
    WHEN edad > 60 THEN 'adultos mayores'
    WHEN edad < 18 THEN 'menores de edad'
    ELSE 'Sin especificar'
  END AS grupo_etario,
  COUNT(id_paciente) as cantidad_pacientes
FROM pacientes
GROUP BY grupo_etario;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Mostrar la cantidad de camas disponbiles por distrito. La nueva columna debe tener la leyenda "Cantidad de camas disponibles para el distrito son cantidad_camas". Por ejemplo, *La cantidad de camas para el distrito Este son: 2000*
-- MAGIC - Nota: Tener en cuenta los tipos de datos

-- COMMAND ----------

SELECT 
  -- distrito, 
  -- SUM(cantidad_camas) AS total_camas,
  concat('Cantidad de camas disponibles para el distrito ',initcap(distrito),' son: ',SUM(cantidad_camas)) AS camas_x_distrito
FROM hospitales
GROUP BY distrito;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Generar un reporte que cuente la cantidad de internaciones y cantidad de dias de internación en 4 grupos de tipo de internacion.
-- MAGIC - Urgente: Son las internaciones donde el servicio contenga la palabra *EMERGENCIA*
-- MAGIC - Infantil: Son las internaciones donde el servicio contenga la palabra *PEDIATRIA*
-- MAGIC - Covid: Son las internaciones donde el servicio contenga la palabra *COVID*
-- MAGIC - Otros: El resto de las internaciones que no pertenezcan a las clasificaciones anteriores

-- COMMAND ----------

SELECT
  CASE
    WHEN servicio ILIKE '%EMERGENCIA%' THEN 'Urgente'
    WHEN servicio ILIKE '%PEDIATRIA%' THEN 'Infantil' 
    WHEN servicio ILIKE '%COVID%' THEN 'Covid'
    ELSE 'Otros'
  END AS tipo_internacion,
  COUNT(id_internacion) AS cant_internaciones,
  SUM(dias_internacion) AS total_dias_internacion
FROM internaciones 
  LEFT JOIN diagnosticos ON internaciones.id_diagnostico = diagnosticos.id_diagnostico
GROUP BY tipo_internacion;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Generar un reporte que liste los pacientes de nacionalidad Argentina: El reporte debe contener los campos
-- MAGIC - Nombre completo
-- MAGIC - sexo
-- MAGIC - edad
-- MAGIC - fecha de nacimiento

-- COMMAND ----------

SELECT
  initcap(concat(trim(nombre),' ',trim(apellido))) AS nombre_completo,
  sexo,
  edad,
  -- current_date() - CAST((edad*365) AS INT) AS fecha_nacimiento -- asumir que un año tiene 365 es un riesgo porque no cuenta con los años bisiestos
  current_date() - (INTERVAL '1 year' * edad) AS fecha_nacimiento
FROM pacientes
WHERE nacionalidad ILIKE '%argentina%';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Generar un informe de las internaciones que se realizaron en el 2020. El mismo debe tener:
-- MAGIC - id_internacion
-- MAGIC - nombre del hospital
-- MAGIC - servicio
-- MAGIC - sector
-- MAGIC - fecha de ingreso
-- MAGIC - fecha de egreso
-- MAGIC - cantidad de dias de internacion
-- MAGIC - tipo de egreso. Si no tiene un tipo de egreso asignado poner "Sin motivo"
-- MAGIC - Normalizar todos los campos que sean necesarios

-- COMMAND ----------

SELECT
  id_internacion,
  initcap(trim(hospitales.nombre)) AS hospital,
  coalesce(initcap(trim(servicio)),'Sin especificar') AS servicio,
  coalesce(initcap(trim(sector)),'Sin especificar') AS sector,
  fecha AS fecha_ingreso,
  fecha + CAST(dias_internacion AS INT) AS fecha_egreso,
  dias_internacion,
  coalesce(initcap(trim(tipo_egreso)),'Sin motivo') AS tipo_egreso
FROM internaciones
  LEFT JOIN hospitales ON internaciones.id_hospital = hospitales.id_hospital
  LEFT JOIN diagnosticos ON internaciones.id_diagnostico = diagnosticos.id_diagnostico
  LEFT JOIN sectores ON internaciones.id_sector = sectores.id_sector
WHERE fecha ILIKE('2020%');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Mostrar los nombres de los hospitales que tienen más camas disponibles que el promedio de camas en todos los hospitales. Ademas el reporte debe mostrar:
-- MAGIC - Distrito
-- MAGIC - Titular
-- MAGIC - Cantidad de camas

-- COMMAND ----------

SELECT AVG(cantidad_camas) FROM hospitales;

-- COMMAND ----------

SELECT nombre, distrito, titular, cantidad_camas FROM hospitales
WHERE cantidad_camas > (SELECT AVG(cantidad_camas) FROM hospitales);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Mostrar los hospitales junto con su distrito, cantidad total de camas y promedio de camas en aquellos hospitales que superen el promedio de camas de su respectivo distrito.

-- COMMAND ----------

SELECT distrito, AVG(cantidad_camas) FROM hospitales
GROUP BY distrito;

-- COMMAND ----------

SELECT h1.nombre, h1.distrito, h1.cantidad_camas, avg_distrito.promedio
FROM hospitales AS h1
INNER JOIN (
    SELECT distrito, AVG(cantidad_camas) as promedio
    FROM hospitales
    GROUP BY distrito
) AS avg_distrito ON h1.distrito = avg_distrito.distrito
WHERE h1.cantidad_camas > avg_distrito.promedio;

-- COMMAND ----------

SELECT 
  nombre, 
  distrito, 
  cantidad_camas,
  CASE 
    WHEN distrito = 'oeste' THEN (SELECT AVG(cantidad_camas) FROM hospitales GROUP BY distrito HAVING distrito = 'oeste')
    WHEN distrito = 'sur' THEN (SELECT AVG(cantidad_camas) FROM hospitales GROUP BY distrito HAVING distrito = 'sur')
    WHEN distrito = 'norte' THEN (SELECT AVG(cantidad_camas) FROM hospitales GROUP BY distrito HAVING distrito = 'norte')
    WHEN distrito = 'sudoeste' THEN (SELECT AVG(cantidad_camas) FROM hospitales GROUP BY distrito HAVING distrito = 'sudoeste')
    WHEN distrito = 'centro' THEN (SELECT AVG(cantidad_camas) FROM hospitales GROUP BY distrito HAVING distrito = 'centro')
    WHEN distrito = 'noroeste' THEN (SELECT AVG(cantidad_camas) FROM hospitales GROUP BY distrito HAVING distrito = 'noroeste')
  END AS avg_camas_x_distrito
FROM hospitales
WHERE
  CASE
    WHEN distrito = 'oeste' AND cantidad_camas > (SELECT AVG(cantidad_camas) FROM hospitales GROUP BY distrito HAVING distrito = 'oeste') THEN 1
    WHEN distrito = 'sur' AND cantidad_camas > (SELECT AVG(cantidad_camas) FROM hospitales GROUP BY distrito HAVING distrito = 'sur') THEN 1
    WHEN distrito = 'norte' AND cantidad_camas > (SELECT AVG(cantidad_camas) FROM hospitales GROUP BY distrito HAVING distrito = 'norte') THEN 1
    WHEN distrito = 'sudoeste' AND cantidad_camas > (SELECT AVG(cantidad_camas) FROM hospitales GROUP BY distrito HAVING distrito = 'sudoeste') THEN 1
    WHEN distrito = 'centro' AND cantidad_camas > (SELECT AVG(cantidad_camas) FROM hospitales GROUP BY distrito HAVING distrito = 'centro') THEN 1
    WHEN distrito = 'noroeste' AND cantidad_camas > (SELECT AVG(cantidad_camas) FROM hospitales GROUP BY distrito HAVING distrito = 'noroeste') THEN 1
    ELSE 0
  END = 1 -- Mostrar los resultados que cumplen con la condicion 1

-- COMMAND ----------

/* 
  SUBCONSULTA CORRELACIONADA
  El término en inglés es "Correlated Subqueries" o "Correlated Nested Queries".
  Una subconsulta correlacionada es una subconsulta que depende de valores de la consulta externa para su ejecución. A diferencia de las subconsultas independientes, estas no pueden ejecutarse de forma aislada porque necesitan información de la consulta "padre".
  Características clave
  - Dependencia externa: La subconsulta referencia columnas de la tabla externa:
    sql-- La subconsulta usa hospitales_padre.distrito WHERE hospitales_hijo2.distrito = hospitales_padre.distrito
  - Ejecución iterativa: Se ejecuta una vez por cada fila de la consulta externa, no una sola vez como las subconsultas independientes.
  - Contexto dinámico: El resultado de la subconsulta cambia según la fila actual que se esté procesando en la consulta externa.
  - Las subconsultas correlacionadas pueden ser menos eficientes porque:
  - Se ejecutan repetidamente (N veces para N filas)
*/
SELECT 
  nombre,
  distrito,
  cantidad_camas,
  (SELECT AVG(cantidad_camas) FROM hospitales AS hospitales_hijo1 WHERE hospitales_hijo1.distrito = hospitales_padre.distrito) AS avg_camas_x_distrito
FROM hospitales AS hospitales_padre
WHERE cantidad_camas > (
  SELECT AVG(cantidad_camas)
  FROM hospitales AS hospitales_hijo2
  WHERE hospitales_hijo2.distrito = hospitales_padre.distrito
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Crear una vista que muestre los nombres de los pacientes que hayan fallecido en los ultimos 18 meses (tomando en cuenta la fecha de egreso) y que han sido internados en hospitales de tipo "Municipal". Además, el reporte debe tener los siguientes campos:
-- MAGIC - Nombre completo
-- MAGIC - sexo
-- MAGIC - edad
-- MAGIC - fecha de nacimiento
-- MAGIC - hospital
-- MAGIC - distrito
-- MAGIC - diagnostico 
-- MAGIC - servicio
-- MAGIC - cantidad dias internacion
-- MAGIC - fecha ingreso
-- MAGIC - fecha egreso
-- MAGIC
-- MAGIC ###### Nota: Normalizar todos los campos que sean necesarios

-- COMMAND ----------

SELECT 
  concat(pacientes.nombre,' ',pacientes.apellido) AS nombre_completo,
  sexo,
  edad,
  current_date() - CAST((edad*365) AS INT) AS fecha_nacimiento,
  hospitales.nombre AS hospital,
  distrito,
  diagnostico,
  servicio,
  dias_internacion,
  fecha,
  fecha + CAST(dias_internacion AS INT) AS fecha_egreso
FROM pacientes 
  INNER JOIN internaciones ON pacientes.id_paciente = internaciones.id_paciente
  LEFT JOIN hospitales ON internaciones.id_hospital = hospitales.id_hospital
  LEFT JOIN diagnosticos ON internaciones.id_diagnostico = diagnosticos.id_diagnostico
WHERE tipo_egreso ILIKE '%DEFUNCION%' AND 
  (fecha + CAST(dias_internacion AS INT) > (current_date() - INTERVAL '18 months')) AND
  titular ILIKE '%municipal%'

-- COMMAND ----------

CREATE VIEW pacientes_fallecidos_ultimos_18_meses AS
SELECT 
  concat(pacientes.nombre,' ',pacientes.apellido) AS nombre_completo,
  sexo,
  edad,
  current_date() - CAST((edad*365) AS INT) AS fecha_nacimiento,
  hospitales.nombre AS hospital,
  distrito,
  diagnostico,
  servicio,
  dias_internacion,
  fecha,
  fecha + CAST(dias_internacion AS INT) AS fecha_egreso
FROM pacientes 
  INNER JOIN internaciones ON pacientes.id_paciente = internaciones.id_paciente
  LEFT JOIN hospitales ON internaciones.id_hospital = hospitales.id_hospital
  LEFT JOIN diagnosticos ON internaciones.id_diagnostico = diagnosticos.id_diagnostico
WHERE tipo_egreso ILIKE '%defuncion%' AND 
  (fecha + CAST(dias_internacion AS INT) > (current_date() - INTERVAL '18 months')) AND
  titular ILIKE '%municipal%'

-- COMMAND ----------

DROP VIEW IF EXISTS pacientes_fallecidos_ultimos_18_meses

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Crear una vista del enunciado numero 7

-- COMMAND ----------

CREATE VIEW hospitales_con_camas_mayor_al_promedio_segun_distrito AS
SELECT h1.nombre, h1.distrito, h1.cantidad_camas, avg_distrito.promedio
FROM hospitales AS h1
INNER JOIN (
    SELECT distrito, AVG(cantidad_camas) as promedio
    FROM hospitales
    GROUP BY distrito
) AS avg_distrito ON h1.distrito = avg_distrito.distrito
WHERE h1.cantidad_camas > avg_distrito.promedio;

-- COMMAND ----------

DROP VIEW IF EXISTS hospitales_con_camas_mayor_al_promedio_segun_distrito

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10. Listar los nombres de los hospitales y el número de internaciones por cada tipo de servicio realizadas en los últimos 2 años

-- COMMAND ----------

SELECT servicio, nombre, COUNT(id_internacion)
FROM diagnosticos 
  LEFT JOIN internaciones ON diagnosticos.id_diagnostico = internaciones.id_diagnostico
  LEFT JOIN hospitales ON internaciones.id_hospital = hospitales.id_hospital
WHERE fecha > (current_date()- INTERVAL '2 years')
GROUP BY servicio, nombre

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11. Obtener los nombres de los hospitales y el promedio de edad de los pacientes internados en cada hospital

-- COMMAND ----------

SELECT hospitales.nombre, ROUND(AVG(edad),0) AS promedio_edad
FROM pacientes
  INNER JOIN internaciones ON pacientes.id_paciente = internaciones.id_paciente
  INNER JOIN hospitales ON internaciones.id_hospital = hospitales.id_hospital
GROUP BY hospitales.nombre;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12. Obtner un listado de hospitales que muestre la cantidad minima, maxima y promedio de internacion por año. Ordenar el resultado por hospital y año de manera ascendente.

-- COMMAND ----------

SELECT
  nombre AS hospital,
  extract(YEAR from fecha) AS anio_internacion,
  min(dias_internacion) AS min_dias_internacion,
  max(dias_internacion) AS max_dias_internacion,
  avg(dias_internacion) AS prom_dias_internacion
FROM internaciones
INNER JOIN hospitales USING(id_hospital)
GROUP BY nombre,anio_internacion
ORDER BY nombre,anio_internacion ASC
