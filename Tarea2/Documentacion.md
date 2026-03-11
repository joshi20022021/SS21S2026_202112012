# Universidad de San Carlos de Guatemala
# Facultad de Ingenieria
# Escuela de Ciencias y Sistemas

## Nombre: Edgar Josías Cán Ajquejay
## Carnet: 202112012

# Documentacion - Tarea 2

# Descripcion del dataset
El dataset “Global AI Impact on Jobs (2010–2025)” contiene información sintética sobre 5000 ofertas de empleo a nivel global entre los años 2010 y 2025. Cada registro representa una vacante laboral e incluye datos sobre ubicación, empresa, industria, puesto, nivel de seniority, salario, además de variables relacionadas con el impacto de la inteligencia artificial, como mención de IA en la oferta, habilidades de IA requeridas, intensidad de uso de IA, riesgo de automatización, necesidad de reentrenamiento y riesgo de desplazamiento laboral. Su propósito es analizar cómo la IA ha influido en el mercado laboral, las habilidades demandadas y la evolución de los empleos en distintos sectores y regiones.

# Descripción de las transformaciones realizadas al dataset

Al dataset Global AI Impact on Jobs (2010–2025) se le aplicaron transformaciones en Power Query con el objetivo de limpiarlo, estandarizarlo y prepararlo para su análisis en Power BI. Primero, se realizó el renombramiento de columnas para usar nombres más cortos, claros y consistentes, facilitando su manejo dentro del modelo. Luego se corrigieron los tipos de datos, asignando formato numérico a campos como salario, puntajes de intensidad de IA y riesgo de automatización, y formato texto a columnas descriptivas como país, ciudad, industria y puesto.

También se efectuó una limpieza de datos de texto, aplicando procesos como quitar espacios innecesarios al inicio y al final, limpiar caracteres ocultos y estandarizar valores para evitar categorías duplicadas por diferencias de escritura. Además, se revisó la posible existencia de registros duplicados, especialmente en la columna identificadora de empleo, y se verificaron valores nulos o vacíos en campos importantes para el análisis.

# Descripcion de la creacion de Diagrama Estrella y Justificacion