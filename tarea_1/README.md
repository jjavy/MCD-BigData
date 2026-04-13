# Descripción del dataset
El dataset de **STRING** _(Search Tool for the Retrieval of Interacting Genes/Proteins)_ para Homo sapiens _(taxid: 9606)_ contiene redes de interacción proteína-proteína. Los archivos suelen incluir identificadores de proteínas y una serie de puntuaciones _(scores)_ que indican la confianza de la interacción basada en diferentes evidencias _(experimentos, bases de datos, text mining, etc.)_.

> Inicialmente se tenía contemplado utilizar el total del dataset, un conjunto de datos de casi 200 GB, sin embargo por limitaciones técnicas personales, se decidió utilizar únicamente las interacciones correspondientes al ser humando _(Homo sapiens)_.

# Justificación
- **Volumen y Escalabilidad**: STRING contiene cientos de miles de registros _(filas de interacciones)_, lo que justifica el uso de herramientas de Big Data como Spark frente a librerías tradicionales como Pandas.
- **Complejidad Estructural**: Permite realizar operaciones de filtrado complejas y cálculos sobre múltiples columnas de evidencia para obtener una puntuación ponderada.
- **Relevancia Científica**: Sigue siendo un estándar actual en bioinformática y análisis de redes biológicas, permitiendo aplicar técnicas de teoría de grafos en etapas posteriores.
