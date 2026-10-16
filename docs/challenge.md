# PRUEBA TÉCNICA - DATA ENGINEERING

---

## 1. Contexto

Formas parte del equipo de Data & Analytics dentro de **Apex Digital / M5**, área transversal que habilita productos analíticos y datos gobernados para múltiples unidades de negocio de una corporación multinacional de bebidas y alimentos (CBC, Beliv, BIA, entre otras).

La organización está consolidando una plataforma de datos empresarial sobre **Databricks + ADLS Gen2 + Unity Catalog**, con el objetivo de habilitar productos analíticos y, en una segunda fase, agentes de IA conectados a los datos core del negocio.

El proyecto **SAAS** define una metodología de ingesta y procesamiento aplicable de forma multi-empresa / multi-tenant: cada unidad de negocio (tenant) puede onboarding-arse al pipeline con mínima fricción, manteniendo aislamiento lógico de datos, trazabilidad y consistencia de estándares.

La arquitectura del proyecto ya está definida y se documenta en la **sección 5** de este documento. El objetivo de la prueba es **implementar el pipeline sobre esa arquitectura, no diseñarla desde cero**.

---

## 2. Alcance y filosofía de la prueba

La prueba está dimensionada para **3 días de trabajo** (aproximadamente 12-15 horas en total). Recibes una arquitectura ya definida y un conjunto de datos de entrada. Tu tarea es implementar el pipeline siguiendo las convenciones provistas.

### Consideraciones para la entrega:
* Un MVP funcional bien razonado se valora más que un alcance amplio mal terminado.
* Documenta en el `README` qué decidiste dejar fuera y la razón.
* El archivo `docs/observations.md` es **obligatorio**. Allí registras:
  * Decisiones de la arquitectura provista con las que no estás de acuerdo (con tu propuesta alternativa).
  * Áreas con ambigüedad y cómo las resolviste.
  * Mejoras tecnológicas que propondrías a futuro.
  * *Mínimo 3 observaciones sustantivas*. No cambies la arquitectura unilateralmente: si discrepas, documéntalo aquí y se discute en la sustentación.
* Cada decisión de implementación debe poder ser explicada en la sustentación.
* El uso de asistentes de IA está permitido. El código que no puedas explicar afecta negativamente la evaluación.

### Distribución de tiempo sugerida

| Día | Foco | Entregables del día |
| :--- | :--- | :--- |
| **Día 1 (~4 h)** | Setup + Bronze | Lectura de la arquitectura, estructura del repo, configuración base, capa Bronze funcionando. |
| **Día 2 (~5 h)** | Silver + DQ | Capa Silver con SCD Type 2, validaciones de calidad, manejo de anomalías. |
| **Día 3 (~5 h)** | Gold, CI, mentoría, doc | Capa Gold, GitHub Actions, ejercicio de code review, README, observaciones. |

---

## 3. Stack tecnológico

El stack objetivo del proyecto incluye los siguientes componentes. La implementación se realiza con un subconjunto; el resto está descrito en la arquitectura provista:

| Componente | Rol en la plataforma | En esta prueba |
| :--- | :--- | :--- |
| **Databricks (PySpark)** | Motor de procesamiento Bronze/Silver/Gold | Implementación |
| **Delta Lake** | Formato de almacenamiento con ACID | Implementación |
| **Unity Catalog** | Gobierno, aislamiento por tenant | Provisto en arquitectura |
| **ADLS Gen2** | Almacenamiento por capas | Simulado con rutas locales |
| **MongoDB / Couchbase** | Fuentes operacionales / serving | Provisto en arquitectura |
| **Streaming (Auto Loader)** | Ingesta incremental | Provisto en arquitectura |
| **Terraform** | Infra como código | Snippet ilustrativo |
| **GitHub + GitHub Actions** | Versionado y CI | Implementación |

> **Nota de ejecución:** La implementación puede ejecutarse con Spark localmente o sobre Databricks Community/trial. El código debe ser compatible con Databricks en ambos casos.

### Versiones mínimas requeridas (alineadas con Databricks Runtime 15.x LTS):
* Python 3.11 o superior.
* PySpark 3.5.x.
* Delta Lake 3.x (paquete `delta-spark`).
* El `README` debe especificar las versiones exactas utilizadas y un `Makefile` o script equivalente para levantar el entorno (`venv`, `poetry`, `uv` u otro).

---

## 4. Datasets de entrada

### 4.1 `global_mobility_data_entrega_productos.csv`
Tabla transaccional con ~3,100 registros de entregas de producto en 6 tenants (países) a lo largo de Q1-Q2 2025.

| Columna | Tipo lógico | Descripción |
| :--- | :--- | :--- |
| `pais` | string | Código de país. Para esta prueba se interpreta como `tenant_id`. |
| `fecha_proceso` | string (YYYYMMDD) | Fecha en que se generó el registro. Clave de partición. |
| `transporte` | long | Identificador del vehículo / unidad de transporte. |
| `ruta` | long | Identificador de la ruta de entrega. |
| `tipo_entrega` | string | `ZPRE`/`ZVE1` = rutina; `Z04`/`Z05` = bonificación; otros = descartar. |
| `material` | string | SKU del producto entregado. FK lógica al catálogo (4.2). |
| `precio` | decimal | Precio unitario al momento de la transacción. |
| `cantidad` | decimal | Cantidad entregada. |
| `unidad` | string | `CS` = caja (20 unidades); `ST` = unidad individual. |

> **Anomalías:** Contiene anomalías intencionales (~4% del total): fechas nulas o inválidas, cantidades negativas y en cero, duplicados exactos, materiales que no existen en el catálogo, y valores de `tipo_entrega` fuera de las 4 categorías válidas. La estrategia de manejo se especifica en la sección 5.6.

### 4.2 `materials_catalog.csv`
Catálogo maestro de materiales (SKUs) con estructura **SCD Type 2**: algunos materiales tienen múltiples registros que reflejan cambios históricos en precio, categoría o descripción.

| Columna | Tipo lógico | Descripción |
| :--- | :--- | :--- |
| `material` | string | SKU del producto. No es PK por sí solo (hay múltiples versiones). |
| `descripcion` | string | Nombre comercial del producto en esa versión. |
| `categoria` | string | Categoría de producto en esa versión. |
| `precio_base` | decimal | Precio base vigente en el intervalo [`valid_from`, `valid_to`]. |
| `valid_from` | date (YYYY-MM-DD) | Inicio de vigencia del registro. |
| `valid_to` | date (YYYY-MM-DD) | Fin de vigencia. `9999-12-31` = vigente actual. |
| `is_current` | boolean | Flag de versión actual. Útil pero no fuente de verdad única. |

---

## 5. Arquitectura provista

### 5.1 Visión general
La plataforma sigue el patrón **Medallion** con tres capas lógicas. Cada capa cumple un rol específico y solo lee de la inmediata anterior:
* **RAW:** Archivos crudos tal como llegan de la fuente, sin transformación. Solo lectura.
* **Bronze:** Datos ingestados en formato Delta, preservando esquema original más columnas técnicas. Particionado por fecha y tenant.
* **Silver:** Datos limpios, normalizados, enriquecidos con dimensiones (SCD Type 2). Es la zona de verdad para consumo analítico downstream.
* **Gold:** Agregaciones y métricas de negocio. Modelado dimensional ligero.

### 5.2 Estrategia de aislamiento multi-tenant
Aislamiento por **schema** dentro de un catálogo único por ambiente:
* Onboarding ágil de nuevos tenants (no requiere crear catálogos, solo schemas).
* Gobierno centralizado de grants y políticas a nivel de catálogo.
* Compatibilidad con escenarios de análisis cross-tenant futuros (vistas en gold).

#### Naming en Unity Catalog:
```text
saas_<env>.bronze_<tenant>.<table>
saas_<env>.silver_<tenant>.<table>
saas_<env>.gold_<tenant>.<table>
```
*Ejemplos:*
* `saas_dev.bronze_sv.deliveries`
* `saas_dev.silver_sv.fact_deliveries`
* `saas_dev.silver_sv.dim_materials`
* `saas_dev.gold_sv.daily_metrics_by_delivery_type`

#### Mapeo a rutas locales (para esta prueba):
```text
data/<layer>/<tenant>/<table>/fecha_proceso=YYYYMMDD/
data/<layer>_quarantine/<tenant>/<table>/
data/shared/quality_logs/
```
*Ejemplos:*
* `data/bronze/sv/deliveries/fecha_proceso=20250314/` *(por tenant)*
* `data/silver/sv/fact_deliveries/fecha_proceso=20250314/` *(por tenant)*
* `data/silver_quarantine/sv/fact_deliveries/` *(cuarentena)*
* `data/shared/quality_logs/` *(cross-tenant)*

Todas las tablas se escriben como Delta.

### 5.3 Convenciones de naming
* **Tablas y columnas:** `snake_case` en todas las capas.
* **Silver hechos:** Prefijo `fact_`.
* **Silver dimensiones:** Prefijo `dim_`.
* **Gold:** Nombre descriptivo del agregado sin prefijo (ej: `daily_metrics_by_delivery_type`).
* **Columnas técnicas:** Prefijo guion bajo (`_ingestion_timestamp`, `_source_file`, `_tenant_id`, `_batch_id`).
* **Flags booleanos:** Prefijo `is_` (ej: `is_routine_delivery`, `is_bonus_delivery`).
* **Fechas técnicas:** `timestamp` UTC; en columnas de negocio: tipo `date`.
* **Códigos tenant:** Minúscula en paths, schemas y en `_tenant_id`. Normalizar la columna `pais` a minúscula al ingresar a Bronze.

### 5.4 Particionado
* **Bronze:** Particionado por `fecha_proceso` (`yyyymmdd`) y `tenant_id`.
* **Silver `fact_deliveries`:** Particionado por `fecha_proceso`. (El tenant ya está aislado por schema/carpeta).
* **Silver `dim_materials`:** No particionado (cardinalidad baja).
* **Gold:** Particionado por fecha si la cardinalidad lo amerita.

### 5.5 Idempotencia y reprocesos
* **Bronze:** Overwrite por partición (`replaceWhere` o equivalente sobre `fecha_proceso` + `tenant_id`).
* **Silver `fact_deliveries`:** `MERGE INTO` con clave de negocio compuesta:
  $$\text{Clave} = (\text{tenant\_id}, \text{fecha\_proceso}, \text{transporte}, \text{ruta}, \text{material}, \text{tipo\_entrega})$$
  Actualiza si existe, inserta si no.
* **Silver `dim_materials`:** `MERGE INTO` con lógica SCD Type 2 sobre la clave `material` + `valid_from`.
* **Gold:** Recomputo completo por partición de fecha. Las tablas Gold son derivadas, no autoritativas.

### 5.6 Manejo de anomalías

| Tipo de anomalía | Acción | Justificación |
| :--- | :--- | :--- |
| `fecha_proceso` nula o inválida | **Cuarentena** | Sin fecha no puede asignarse a partición. Revisión manual posterior. |
| `cantidad` nula, negativa o cero | **Cuarentena** | Posible error de origen. No descartar silenciosamente. |
| `material` no presente en el catálogo | **Cuarentena** | Romper integridad referencial debe ser visible y auditable. |
| `tipo_entrega` fuera de las 4 válidas | **Descarte** | Regla de negocio: COBR y Z99 no pertenecen al alcance analítico. |
| Duplicado exacto (todas las columnas) | **Deduplicar** | Conservar una sola copia. No tratar como anomalía visible. |
| `precio` nulo | **Cuarentena** | Sin precio las métricas de ingreso no son calculables. |

* Las filas en cuarentena se escriben en `<layer>_quarantine_<tenant>.<table>` con la columna técnica `_quarantine_reason`.
* Los descartes se contabilizan en métricas pero no se persisten.

### 5.7 SCD Type 2 sobre `dim_materials`
* **Clave de negocio:** `material` (SKU).
* **Atributos versionados:** `descripcion`, `categoria`, `precio_base`.
* **Columnas de control:** `valid_from`, `valid_to`, `is_current`.
* Solo una fila puede tener `is_current = true` por cada SKU.
* **Enriquecimiento temporal:** Las transacciones Silver deben enriquecerse con la versión vigente a la fecha del proceso:
  ```sql
  fact_deliveries.fecha_proceso BETWEEN dim_materials.valid_from AND dim_materials.valid_to
  ```

### 5.8 Configuración (OmegaConf)
Estructura jerárquica:
```text
config/base.yaml             # Defaults compartidos
config/env/<env>.yaml        # Overrides por ambiente (dev/qa/main)
config/tenants/<tenant>.yaml # Overrides por tenant
```
Parámetros mínimos a soportar:
* `paths.bronze`, `paths.silver`, `paths.gold`
* `paths.quarantine_root`
* `paths.quality_logs`
* `execution.start_date`, `execution.end_date` (formato `YYYY-MM-DD`)
* `execution.tenant` (código de tenant o `"all"`)
* `execution.fail_fast` (bool, default `false`)
* `quality.fail_on_critical` (bool, default `false`)

### 5.9 Tabla de logs de calidad
Persistencia Delta en `data/shared/quality_logs/` (o `<env>.shared.quality_logs`):

| Campo | Tipo | Descripción |
| :--- | :--- | :--- |
| `_run_id` | string | Identificador de ejecución |
| `_batch_id` | string | Identificador del lote |
| `tenant_id` | string | Código del tenant |
| `layer` | string | `bronze` \| `silver` \| `gold` |
| `table_name` | string | Nombre de la tabla validada |
| `check_name` | string | Identificador del check |
| `check_severity` | string | `critical` \| `warning` \| `info` |
| `records_checked` | long | Registros evaluados |
| `records_failed` | long | Registros con fallos |
| `check_passed` | boolean | `true` si pasó el check |
| `executed_at` | timestamp | Timestamp UTC de ejecución |

### 5.10 Diagrama de arquitectura
```text
                  RAW (CSV)
                      │
                      ▼ (overwrite por partición)
          BRONZE: <tenant>.deliveries (Delta, particionado fecha+tenant)
                      │
                      ▼ (MERGE INTO por clave de negocio)
          SILVER: <tenant>.fact_deliveries
                  <tenant>.dim_materials (SCD2)
                      │
                      ├──► QUARANTINE: quarantine_<table>
                      ├──► SHARED: quality_logs
                      │
                      ▼ (recompute por partición)
          GOLD:   <tenant>.daily_metrics_*
```

---

## 6. Implementación requerida

### 6.1 Configuración (obligatorio)
1. Parámetros vía YAML con OmegaConf (estructura jerárquica de 5.8).
2. Flag `--tenant sv` o `--tenant all`.
3. Rango de fechas `--start-date` y `--end-date` (`fecha_proceso`).
4. Configuración por ambiente: `dev`/`qa`/`main` (cambio de paths).

### 6.2 Capa Bronze
* Ingesta de CSV a Delta preservando esquema original.
* Particionado por `fecha_proceso` y `tenant_id`.
* Idempotencia por overwrite de partición.
* Columnas técnicas: `_ingestion_timestamp`, `_source_file`, `_tenant_id`, `_batch_id`.

### 6.3 Capa Silver
* `fact_deliveries` con `MERGE INTO`.
* Normalización de unidades: convertir `CS` a `ST` ($1\text{ CS} = 20\text{ ST}$).
* Filtrado de `tipo_entrega`: conservar solo `ZPRE`, `ZVE1`, `Z04`, `Z05`.
* Flags `is_routine_delivery` (`ZPRE`, `ZVE1`) e `is_bonus_delivery` (`Z04`, `Z05`).
* Manejo de anomalías (cuarentena y descarte según 5.6).
* `dim_materials` como SCD Type 2.
* Enriquecimiento temporal con `dim_materials` (`BETWEEN valid_from AND valid_to`).

### 6.4 Capa Gold
Construir la tabla `daily_metrics_by_delivery_type` por tenant:
* **Granularidad:** `(tenant_id, fecha_proceso, tipo_entrega)`
* **Métricas:**
  * `total_units`: Suma de cantidad normalizada a `ST`.
  * `total_revenue`: Suma de $\text{cantidad\_normalizada\_st} \times \text{precio\_transaccion}$.
  * `active_routes`: `count(distinct ruta)`.
  * `active_transports`: `count(distinct transporte)`.

### 6.5 Calidad de datos
* Mínimo 3 validaciones sobre Silver.
* Registro en `quality_logs` según esquema 5.9.
* Declaración de severidad (`critical`, `warning`, `info`).
* Abortar antes de Gold si `quality.fail_on_critical = true` y falla un check crítico.

---

## 7. CI/CD e infraestructura

### 7.1 GitHub Actions (obligatorio)
Workflow en `push` y `pull_request`:
* Linter Python (`ruff`, `flake8`) bajo PEP8.
* Tests unitarios con `pytest`: mínimo 3 pruebas significativas (conversión de unidades, filtrado, anomalías, SCD2).
* Test de carga y parseo de archivos YAML de configuración.

### 7.2 Terraform (snippet)
En `docs/infra.md`:
* Descripción de recursos para onboarding de nuevo tenant (schemas en Unity Catalog, paths ADLS, secretos).
* Snippet ilustrativo (~30-50 líneas) del módulo principal.

---

## 8. Mentoría y revisión de código

En la carpeta `/mentoring/`:
1. `bad_code.py`: Contiene el código provisto en el **Anexo A** (sin modificar).
2. `code_review.md`: Mínimo 4 observaciones accionables (qué está mal, por qué importa, cómo se corrige).
3. `good_code.py`: Versión refactorizada.
4. Nota breve (~1/3 de página): *"Cómo se lo explicaría al junior"*, detallando estilo de feedback pedagógico y temas a investigar.

---

## 9. Documentación y sustentación

### 9.1 README
* Estructura del repositorio.
* Instrucciones reproducibles de instalación y ejecución.
* Cómo ejecutar tests y linter.
* Guía de onboarding para nuevos tenants.
* Sección *"Qué dejé fuera y por qué"*.

### 9.2 Observaciones a la arquitectura (`docs/observations.md`)
Mínimo 3 observaciones sustantivas:
1. Desacuerdos de arquitectura con propuesta alternativa y trade-offs.
2. Ambigüedades detectadas y solución implementada.
3. Propuestas de mejora tecnológica (Horizonte 2-3).

### 9.3 Sustentación (30 min)
* Demo funcional del pipeline (10 min).
* Walkthrough técnico de decisiones (10 min).
* Discusión de `observations.md` (5 min).
* Q&A (5 min).

---

## 10. Bonus opcionales
* Segunda tabla Gold (ej. top materiales por tenant y mes).
* Ingesta Bronze con Auto Loader / Structured Streaming.
* Dashboard simple (Databricks SQL / Streamlit).
* Pre-commit hooks configurados.
* Módulo Terraform completamente ejecutable (`terraform validate` / `plan`).

---

## 11. Criterios de evaluación

| Eje | Peso | Aspectos Clave |
| :--- | :---: | :--- |
| **Implementación del pipeline** | 40% | Funcionalidad, adherencia arquitectónica, idempotencia, join temporal SCD2, separación de capas, OmegaConf. |
| **Calidad de datos** | 15% | Validaciones relevantes, registro en `quality_logs`, manejo de cuarentena. |
| **CI + Terraform** | 10% | GitHub Actions funcional, unit tests significativos, IaC coherente. |
| **Mentoría / Code review** | 15% | Feedback accionable, refactorización limpia, nota constructiva. |
| **Documentación y código** | 10% | Reproducibilidad del README, PEP8, historial de commits limpio. |
| **Criterio y sustentación** | 10% | Argumentación en `observations.md`, defensa técnica en vivo. |

### Penalizaciones directas:
* Rutas o datos hardcodeados.
* Multi-tenant resuelto como un simple `WHERE pais = X`.
* SCD Type 2 unido solo con `is_current = true`.
* Pérdida silenciosa de registros sin catálogo (sin cuarentena).
* Validaciones no registradas en `quality_logs`.
* Commits tipo `"fix all"` o repositorios con commit único.
* Cambios arquitectónicos no explicados en `observations.md`.

---

## 12. Entregables y reglas
* **Repositorio:** GitHub público con commits incrementales y al menos un PR visible.
* **Plazo:** 3 días corridos desde la entrega de la prueba.
* **Idiomas:** Código y variables en **inglés**; documentación y comentarios en **español o inglés** consistente.
* **IA:** Asistentes permitidos siempre que el candidato pueda explicar todo el código.
* **Sustentación:** Llamada de 30 minutos (Google Meet) en los 5 días hábiles posteriores a la entrega.

---

## Anexo A: Código para revisar (`bad_code.py`)

```python
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def process(file_path, country):
    df = pd.read_csv(file_path)
    df = df[df["pais"] == country]
    result = []
    for i, row in df.iterrows():
        if row["tipo_entrega"] == "ZPRE" or row["tipo_entrega"] == "ZVE1":
            if row["unidad"] == "CS":
                qty = row["cantidad"] * 20
            else:
                qty = row["cantidad"]
            result.append({
                "pais": row["pais"],
                "fecha": row["fecha_proceso"],
                "material": row["material"],
                "cantidad_st": qty,
                "total": qty * row["precio"]
            })
    out = pd.DataFrame(result)
    sdf = spark.createDataFrame(out)
    sdf.write.mode("overwrite").parquet("/tmp/output/" + country)
    print("done")
    return out

process("data.csv", "GT")
```

---

## Anexo B: Estructura de repositorio sugerida

```text
saas-data-platform/
├── .github/
│   └── workflows/
│       └── ci.yml
├── config/
│   ├── base.yaml
│   ├── env/
│   │   ├── dev.yaml
│   │   ├── qa.yaml
│   │   └── main.yaml
│   └── tenants/
│       └── sv.yaml
├── data/                               # Creado al ejecutar; no versionar contenido
│   ├── bronze/<tenant>/<table>/fecha_proceso=YYYYMMDD/
│   ├── silver/<tenant>/<table>/fecha_proceso=YYYYMMDD/
│   ├── gold/<tenant>/<table>/
│   ├── bronze_quarantine/...
│   ├── silver_quarantine/...
│   └── shared/
│       └── quality_logs/
├── docs/
│   ├── infra.md                        # Descripción Terraform + snippet
│   ├── observations.md                 # Obligatorio, mínimo 3 observaciones
│   └── onboarding-tenant.md
├── mentoring/
│   ├── bad_code.py
│   ├── good_code.py
│   └── code_review.md
├── src/
│   └── saas_pipeline/
│       ├── __init__.py
│       ├── bronze.py
│       ├── silver.py
│       ├── gold.py
│       ├── quality.py
│       ├── config.py
│       └── cli.py
├── tests/
│   ├── test_silver_transforms.py
│   └── test_quality.py
├── Makefile                            # Opcional, recomendado para reproducibilidad
├── pyproject.toml                      # O requirements.txt
└── README.md
```