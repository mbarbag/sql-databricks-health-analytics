-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### A continuación se plantean un conjunto de ejercicios utilizando el modelo de Salud públicas de la Ciudad de Rosario (Santa Fé)
-- MAGIC #### [Link: Datos Rosario](https://datos.rosario.gob.ar/)
-- MAGIC ![Modelo Salud](/Workspace/Lab-Bicicletas/DER-Salud.jpg)
-- MAGIC
-- MAGIC ###### El modelo cuenta con las siguientes tablas:
-- MAGIC - Pacientes: Listado de pacientes internados en instituciones de salud de la ciudad de Rosario. Incluye id, nombre, apellido, edad, nacionalidady sexo.
-- MAGIC - Hospitales: Listado de instituciones de salud resgistradas en el sistema. Incluye identificador, nombre, dirección, distrito, titular y cantidad de camas.
-- MAGIC - Diagnosticos: Listado de los tipos de diagnosticos de los pacientes internados. Incluye identificador, nombre del diagnostico y servicio.
-- MAGIC - Sectores: Registro de los sectores de las instituciones.
-- MAGIC - Internaciones: Información de las internaciones realizadas entre 2019-2022. Incluye el identificador de internación, paciente, diagnostico, sector, fecha de internación, dias de internacion y motivo de egreso

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Listar el contenido de la tabla diagnosticos

-- COMMAND ----------

USE lab.salud

-- COMMAND ----------

SELECT * FROM diagnosticos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Listar el nombre, calle y altura de los hospitales que sean de tipo *Provincial*

-- COMMAND ----------

SELECT nombre, calle, altura
FROM hospitales
WHERE titular='Provincial';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Continuando con la consulta anterior generar un campo *dirección* que sea la calle y altura. Quitar "espacios" en caso de ser necesario

-- COMMAND ----------

SELECT nombre, concat(trim(calle), ' ',trim(altura)) AS direccion
FROM hospitales
WHERE titular='Provincial';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Listar todos los hospitales que sean de tipo *Municipal* y que pertenezcan al distrito *oeste* o *sur*

-- COMMAND ----------

SELECT nombre, distrito FROM hospitales
WHERE titular='Municipal' AND distrito IN ('oeste','sur');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. ¿y que tengan entre 100 y 200 camas?

-- COMMAND ----------

SELECT nombre, distrito, cantidad_camas FROM hospitales
WHERE titular='Municipal' AND distrito IN ('oeste','sur') AND cantidad_camas BETWEEN 100 AND 200;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. ¿Cuales son los tipos de egresos posibles de los pacientes?. Ordenar alfabeticamente

-- COMMAND ----------

SELECT DISTINCT tipo_egreso FROM internaciones
ORDER BY tipo_egreso;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Generar un reporte que muestre las internaciones del año 2022 que estuvieron al menos un mes internado y el tipo de egreso no sea *defuncion*. El reporte debe contener:
-- MAGIC - Fecha de internación
-- MAGIC - ID de internación
-- MAGIC - ID de hospital
-- MAGIC - ID del paciente
-- MAGIC - cantidad de dias internado
-- MAGIC - motivo de egreso
-- MAGIC - Primero se deben mostrar las internaciones más recientes y luego por tipo de egreso alfabeticamente

-- COMMAND ----------

SELECT fecha, id_internacion, id_hospital, id_paciente, dias_internacion, tipo_egreso FROM internaciones
WHERE tipo_egreso!='Defuncion'
AND dias_internacion>=30
AND fecha LIKE('2022%')
ORDER BY fecha DESC, tipo_egreso;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Obtener los nombres de los hospitales que no tienen la palabra "salud" en su nombre, ordenados por distrito

-- COMMAND ----------

SELECT nombre, distrito FROM hospitales
WHERE nombre NOT ILIKE('%salud%')
ORDER BY distrito;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Mostrar los pacientes que sean mayores de edad y del sexo femenino

-- COMMAND ----------

SELECT * FROM pacientes
WHERE edad>=18 AND sexo='F';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10. Listar los pacientes, fecha y cantidad de dias de internacion de los sectores que comiencen con "C" de las internaciones que no tengan motivo de egreso. En el listado completar el tipo de egreso por la descripción *Sin Especificar*

-- COMMAND ----------

SELECT internaciones.id_paciente, internaciones.fecha, internaciones.dias_internacion, sectores.id_sector, sectores.sector, 
  CASE
    WHEN internaciones.tipo_egreso IS NULL THEN 'Sin Especificar'
  END AS tipo_egreso
FROM internaciones
INNER JOIN sectores ON internaciones.id_sector = sectores.id_sector
WHERE internaciones.tipo_egreso IS NULL
AND sectores.id_sector ILIKE 'c%';
-- AND sectores.sector ILIKE ('C%');


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11. Existen pacientes que no sean de nacionalidad Argentina y que sean menores de edad o mayores a los 65 años? En caso de existir, listar el nombre completo (en un solo campo), edad y nacionalidad.

-- COMMAND ----------

SELECT concat(nombre,' ',apellido) AS nombre_completo, edad, nacionalidad FROM pacientes
WHERE nacionalidad != 'Argentina' AND (edad<18 OR edad>65);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12. Desde el sector de administración nos solicitan que aseguremos la calidad de los datos y normalizemos los datos de los hospitales. Por lo tango debemos generar un listado de los hospitales que contenga los siguientes campos:
-- MAGIC - Nombre del hospital, eliminar espacios innecesarios.
-- MAGIC - direccion, unificar y eliminar espacios innecesarios. Ademas, dejar sólo la primer letra en mayuscula
-- MAGIC - Distrito, eliminar espacios inncesarios y dejar solo primer letra en mayuscula
-- MAGIC - Cantidad de campas

-- COMMAND ----------

SELECT 
  trim(nombre) AS nombre, 
  initcap(concat(trim(calle),' ',trim(altura))) AS direccion,
  initcap(trim(distrito)) AS distrito,
  cantidad_camas
FROM hospitales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 13. Calcular la edad promedio de los pacientes por sexo

-- COMMAND ----------

SELECT sexo, ROUND(AVG(edad),0) FROM pacientes
GROUP BY sexo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 14. Calcular el promedio de días de internación de los pacientes por tipo de egreso para el año 2021. En caso que no este catalogado el tipo de egreso completar con "Sin tipo"

-- COMMAND ----------

SELECT 
  CASE 
    WHEN tipo_egreso IS NULL THEN 'Sin tipo'
    ELSE tipo_egreso
  END AS tipo_egreso, 
  ROUND(AVG(dias_internacion),0) AS media_dias_internacion 
FROM internaciones
WHERE fecha LIKE('2021%')
GROUP BY tipo_egreso;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 15. Contar la cantidad de internaciones por diagnóstico, pero solo para aquellos diagnósticos con más de 50 internaciones

-- COMMAND ----------

SELECT id_diagnostico, COUNT(id_diagnostico) FROM internaciones
GROUP BY id_diagnostico
HAVING COUNT(id_diagnostico)>50;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 16. Obtener un listado de hospitales que muestre la cantidad minima, maxima y promedio de internacion por año. Ordenar el resultado por hospital y año de manera ascendente.

-- COMMAND ----------

SELECT 
  id_hospital,
  EXTRACT(YEAR FROM fecha) AS anno,
  MIN(dias_internacion),
  MAX(dias_internacion),
  ROUND(AVG(dias_internacion),0)
FROM internaciones
GROUP BY id_hospital, anno
ORDER BY id_hospital, anno;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 17. Encontrar el día de la semana con más internaciones en promedio

-- COMMAND ----------

SELECT 
  DAYNAME(fecha) AS dia,
  ROUND(AVG(dias_internacion),0)
FROM internaciones
GROUP BY dia
ORDER BY AVG(dias_internacion) DESC
LIMIT 1;
