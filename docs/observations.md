# SAAS PIPELINE

## ESTADO ACTUAL

| Area | Current state | Assessment |
| --- | --- | --- |
| Config | OmegaConf por env, tenants y runtime | Implementado |
| Bronze | Ingesta con pySpark Structured Streaming | Implementado |
| Silver | Parseo, Dedup, logica de negocio, quarantena, SCD2 MERGE INTO, temporal JOIN | Implementado |
| Gold | Daily metrics by delivery-type | Implementado |
| DQ | Reglas de DQ vector, severidad, audit log tabla Delta, fail on critical | Implementado |
| Portabilidad | Pensado para desarrollo en local modo --filesystem y validar y deployar a Databricks+UC | Diseñado con este objetivo en mente solo implementado modo local |
| IaC | No Implementado Terraform | - |
| CI | Ruff + pytest en GH runner | - |

## MEJORAS PREVISTAS

### BRONZE
- cambiar a **AutoLoader**, definir el schema en un path y utilizar `rescued-data` column para mejorar el schema evolution como esta actualmente
- si bien logea registros incorrectos en `quality_logs` no se implemento quarentena como en capa silver
- no se considero mantenimiento (`VACUUM` con retencion y `OPTIMIZE`)
- 🟡 se implemento una tabla bronze por tenant, quizas en bronze una misma tabla consolidada *(con RLS en caso de ser preciso)* podria funcionar mejor
- 🔴 reevaluar particionado: `fecha_proceso` con este bajo volumen de registros deja excesivas particiones y parquets pequeños. Quizas con el volumen de datos en produccion si amerita `tenant`+`fecha_proceso`
- 🔴 `batch_id` se genera por invocacion del pipeline, como `run_id`, no identifica el batch de origen. Podria utilizar `epoch_id` del micro batch

### SILVER
- 🔴 resolver el filtro start/end, comente la ventana que usaba para filtrar, porque las fechas erroneas no se logeaban en `quality_log`, pero ahora esta procesando bronze completa
- 🟡 reevaluar si hace falta el dedup costoso si ya antes estamos haciendo el JOIN temporal con la dimension catalogo con SCD2
- 🟡 junto punto al anterior, validar que no haya intervalos de vigencia superpuestos por SKU antes del merge
- 🟡 la quarentena hoy hace overwrite, sirve para re procesar pero borra evidencia previa

### GOLD
- 🔴 procesarla incremental, reprocesar agregaciones solo de la ventana de tiempo en vez de reprocesar y sobreescribir la tabla completa
- 🟡 reevaluar si hace falta rellenar con 0 aquellas fechas sin entregas
- 🟡 reevaluar si es mejor pivotear por `delivery_type` 

### DQ
- Definir si continuar con un enfoque pySpark native o valerse de un framework tipo **GX** o **Soda**
- Evaluar si mantener versiones de reglas y logear bajo que versiones se hizo el pipeline run tanto en `quality_log` como en `silver_quarantine`

### PLATAFORMA

**pendientes**
- 🔴 Terraform IaC
- 🔴 CD a Databricks
- 🔴 CI sumar 
    - buildear `whl` artifact
    - validar Terraform
- 🔴 Orquestacion 
    - Databricks Job/Workflow 
    - en local mantenemos CLIck o Dagster
- 🟡 Run Metadata, en local quizas no es necesario y en produccion tendriamos el monitoreo y audit resuelto
