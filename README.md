# Spark Streaming — Danger Zone Alerts

Job de **Spark Structured Streaming** escrito en **Scala** (programación funcional sobre DataFrames) que consume `topic.geolocation`, marca con probabilidad configurable (**10%** por default) cada punto GPS como "zona peligrosa", y publica la alerta a `topic.alert`.

Es la capa de **procesamiento online** del MVP. Corre junto al [rides_simulator](../rides_simulator/) (Go) que produce los eventos GPS.

---

## Flujo

```
┌────────────────────┐     ┌─────────────────────────────┐     ┌──────────────┐
│ topic.geolocation  │────▶│  Spark Structured Streaming │────▶│ topic.alert  │
│ (GPS cada 2s)      │     │  rand() < RISK_THRESHOLD    │     │ (10% del     │
│                    │     │  enriquece metadata         │     │  tráfico GPS)│
└────────────────────┘     └─────────────────────────────┘     └──────────────┘
```

1. **Read**: `readStream.format("kafka")` sobre `topic.geolocation`, parsea el JSON con schema tipado.
2. **Detect**: agrega una columna `risk_score = rand()` y filtra `risk_score < 0.10` → ~10% de los eventos.
3. **Enrich**: renombra `event_id → source_event_id`, genera un `event_id` nuevo (UUID), añade `alert_timestamp_utc`, `risk_alert = true`, `reason = "random_zone_risk_10pct"`.
4. **Write**: escribe a `topic.alert` con `trip_id` como key (preserva la correlación con los otros tópicos).

Toda la lógica está en [`DangerZoneDetector.detectAlerts`](src/main/scala/com/ridesim/streaming/logic/DangerZoneDetector.scala) — una transformación pura `DataFrame => DataFrame` testeable fuera del stream.

---

## Estructura

```
spark_streaming_alerts/
├── build.sbt
├── project/
│   ├── build.properties
│   └── plugins.sbt
├── src/
│   ├── main/scala/com/ridesim/streaming/
│   │   ├── Main.scala                    # entry point
│   │   ├── config/AppConfig.scala        # env vars → case class
│   │   ├── domain/Schemas.scala          # schema de topic.geolocation
│   │   ├── logic/DangerZoneDetector.scala   # transformación pura
│   │   └── pipeline/AlertPipeline.scala  # read → transform → write
│   └── test/scala/com/ridesim/streaming/
│       ├── config/AppConfigSpec.scala
│       └── logic/DangerZoneDetectorSpec.scala
├── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## Evento de salida (`topic.alert`)

**Kafka key**: `trip_id` (hash determinístico → misma partición que los otros tópicos del mismo viaje).

```json
{
  "event_id":             "c1a9f8ea-...",
  "source_event_id":      "45ced94a-b468-4609-a1a1-44887868200e",
  "source_timestamp_utc": "2026-04-19T23:01:18.605Z",
  "alert_timestamp_utc":  "2026-04-19T23:01:28.112Z",
  "trip_id":              "b5585e01-e54d-4e16-9386-495d85cde228",
  "user_id":              "user_019563",
  "driver_id":            "driver_044177",
  "lat":                  1.331,
  "lon":                  103.793,
  "risk_alert":           true,
  "risk_score":           0.0734,
  "reason":               "random_zone_risk_10pct",
  "schema_version":       "1.0"
}
```

Campos clave:

| Campo                  | Descripción                                                          |
|------------------------|----------------------------------------------------------------------|
| `event_id`             | UUID nuevo, identifica **esta alerta**                              |
| `source_event_id`      | `event_id` del evento de `topic.geolocation` que la originó         |
| `source_timestamp_utc` | Cuándo fue emitido el punto GPS                                     |
| `alert_timestamp_utc`  | Cuándo Spark procesó la alerta                                      |
| `risk_score`           | Valor aleatorio en `[0, 1)`; la alerta existe solo si `< threshold` |
| `reason`               | Razón (extensible a geofencing real, fraud, etc.)                   |

---

## Configuración (env vars)

| Variable            | Default                          | Descripción                                    |
|---------------------|----------------------------------|------------------------------------------------|
| `KAFKA_BROKERS`     | `localhost:9092`                 | Brokers Kafka                                  |
| `TOPIC_GEOLOCATION` | `topic.geolocation`              | Tópico de entrada (GPS)                        |
| `TOPIC_ALERT`       | `topic.alert`                    | Tópico de salida (alertas)                     |
| `RISK_THRESHOLD`    | `0.10`                           | Probabilidad de marcar como zona peligrosa    |
| `STARTING_OFFSETS`  | `latest`                         | `earliest` para reprocesar histórico          |
| `TRIGGER_INTERVAL`  | `10 seconds`                     | Micro-batch interval                           |
| `CHECKPOINT_DIR`    | `/tmp/spark-checkpoints/alerts`  | Directorio de checkpoint del stream            |
| `SPARK_MASTER`      | `local[*]`                       | Para cluster real: `spark://...` o `yarn`     |

---

## Correr con Docker (junto al rides_simulator)

El `docker-compose.yml` se conecta a la red `rides_simulator_default` que crea el `docker-compose up` del otro proyecto.

### Paso 1: asegúrate de que `rides_simulator` esté corriendo

```bash
cd ../rides_simulator
docker-compose up -d
```

Verifica la red:

```bash
docker network ls | grep rides
# debe aparecer: rides_simulator_default
```

### Paso 2: arranca el job de Spark

```bash
cd ../spark_streaming_alerts
docker-compose up --build
```

La primera corrida tarda más porque:

1. sbt descarga todas las deps (cacheadas en `./data/ivy-cache` para siguientes builds)
2. Spark descarga `spark-sql-kafka-0-10` con `--packages`

### Paso 3: observa las alertas

```bash
# consume topic.alert desde CLI
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic topic.alert \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | "
```

O en Kafka UI: http://localhost:8081 → Topics → `topic.alert` → Messages.

Con `RIDES_PER_MINUTE=1` en el simulator y `RISK_THRESHOLD=0.10`, deberías ver ~1 alerta cada ~3-4 minutos (1 viaje de 3 GPS eventos × 10% = 0.3 alertas por viaje).

Para ver más alertas rápido, sube el ritmo:

```bash
# desde host
curl -X POST "http://localhost:8080/rides/burst?n=100"
```

---

## Correr localmente (sin Docker)

Requisitos: **JDK 17** + **sbt 1.9+**. Kafka debe estar expuesto en `localhost:29092` (lo hace el `rides_simulator/docker-compose.yml`).

```bash
# tests
sbt test

# run
export KAFKA_BROKERS=localhost:29092
export TOPIC_GEOLOCATION=topic.geolocation
export TOPIC_ALERT=topic.alert
export RISK_THRESHOLD=0.10
export CHECKPOINT_DIR=/tmp/spark-checkpoints/alerts
sbt "run"
```

`sbt run` usa `fork := true` con los `--add-opens` necesarios para Java 17 + Spark.

---

## Tests

```bash
sbt test
```

Cobertura:

- **`DangerZoneDetectorSpec`** (7 tests)
  - threshold=0 → 0 alertas
  - threshold=1 → todas son alertas
  - threshold=0.10 con seed → ~100 alertas sobre 1000 rows (bounds estadísticos)
  - correlación `trip_id`/`user_id`/`driver_id` preservada
  - `event_id` nuevo distinto de `source_event_id`
  - `risk_alert=true` y `reason` correcto
  - `risk_score ∈ [0,1)` y `alert_timestamp_utc` no vacío
- **`AppConfigSpec`** (3 tests): defaults, overrides, parsing numérico

---

## Programación funcional — nota de diseño

- `DangerZoneDetector.detectAlerts` es una **función pura** `DataFrame => DataFrame` con parámetros explícitos. No tiene side effects. La única fuente de no-determinismo (`rand()`) es inyectable vía `seed` para tests.
- `AppConfig` es un `case class` inmutable construido desde un `Map[String, String]` puro (`fromEnv`), lo que permite testear la config sin tocar `sys.env`.
- El pipeline (`AlertPipeline.run`) compone transformaciones vía `flatMap`-style chaining (`withColumn`, `select`, `filter`) — no hay loops imperativos ni vars.
- Errores: Spark Streaming maneja fallos vía checkpoint + reintentos; no hay `try/catch` silenciosos que escondan problemas.

---

## Próximos pasos

- Reemplazar la detección random por **geofencing real**: cargar un DataFrame de polígonos peligrosos y hacer `ST_Contains` (via Apache Sedona) contra cada GPS.
- Enviar las alertas a **Amazon SNS** (o el `SEND ALERT` del diagrama) además de a Kafka.
- Agregar un segundo stream que haga **stream-stream join** entre `topic.geolocation` y `topic.payment` para detectar fraude (pagos desde zonas donde el driver nunca estuvo).
