import json
import os
import threading
import logging
import time

import joblib
import numpy as np
import redis
import uvicorn
from fastapi import FastAPI
from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

from opentelemetry import trace
from opentelemetry.propagate import extract
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ml-predictor")

# OpenTelemetry setup
otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4318")
resource = Resource(attributes={"service.name": "ml-predictor"})
provider = TracerProvider(resource=resource)
exporter = OTLPSpanExporter(endpoint=f"{otlp_endpoint}/v1/traces")
processor = BatchSpanProcessor(exporter)
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("ml-predictor")

# Prometheus metrics
predictions_total = Counter("ml_predictor_predictions_total", "Total predictions", ["model_type"])
prediction_latency = Histogram("ml_predictor_prediction_duration_seconds", "Prediction latency", ["model_type"])
xg_value = Gauge("ml_predictor_last_xg", "Last xG prediction", ["match_id"])
home_win_prob = Gauge("ml_predictor_home_win_prob", "Home win probability", ["match_id"])
kafka_messages_processed = Counter("ml_predictor_kafka_messages_total", "Kafka messages processed")

app = FastAPI(title="ml-predictor")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "localhost:6379")
MODEL_DIR = os.getenv("MODEL_DIR", "/app/models")

try:
    xg_model = joblib.load(f"{MODEL_DIR}/xg_model.pkl")
    xg_features = joblib.load(f"{MODEL_DIR}/xg_features.pkl")
    logger.info("Loaded xG model")
except Exception as e:
    logger.warning(f"Could not load xG model: {e}")
    xg_model = None
    xg_features = None

try:
    win_model = joblib.load(f"{MODEL_DIR}/win_prob_model.pkl")
    win_features = joblib.load(f"{MODEL_DIR}/win_prob_features.pkl")
    logger.info(f"Loaded win probability model (classes: {win_model.classes_})")
except Exception as e:
    logger.warning(f"Could not load win probability model: {e}")
    win_model = None
    win_features = None

rdb = redis.Redis(host=REDIS_URL.split(":")[0], port=int(REDIS_URL.split(":")[1]), decode_responses=True)


def predict_xg(event):
    if xg_model is None:
        return None

    import math
    x = event.get("x", 80)
    y = event.get("y", 50)
    distance = math.sqrt((100 - x) ** 2 + (50 - y) ** 2)
    angle = math.degrees(math.atan2(7.0, distance * (105 / 100)))

    detail = event.get("detail", "")
    shot_type = "foot"
    if "header" in detail:
        shot_type = "header"
    elif "freekick" in detail:
        shot_type = "freekick"
    elif "penalty" in detail:
        shot_type = "penalty"

    features = {
        "x": x, "y": y, "distance": distance, "angle": angle,
        "shot_type_foot": 1 if shot_type == "foot" else 0,
        "shot_type_header": 1 if shot_type == "header" else 0,
        "shot_type_freekick": 1 if shot_type == "freekick" else 0,
        "shot_type_penalty": 1 if shot_type == "penalty" else 0,
        "defenders": 2,
        "is_strong_foot": 1 if shot_type == "foot" else 0,
    }

    X = np.array([[features[f] for f in xg_features]])
    xg = float(xg_model.predict_proba(X)[0][1])
    return round(xg, 4)


def predict_win_probability(stats):
    if win_model is None:
        return None

    features = {
        "minute": stats.get("minute", 0),
        "home_goals": stats.get("home_goals", 0),
        "away_goals": stats.get("away_goals", 0),
        "goal_diff": stats.get("home_goals", 0) - stats.get("away_goals", 0),
        "home_shots": stats.get("home_shots", 0),
        "away_shots": stats.get("away_shots", 0),
        "home_shots_on_target": stats.get("home_shots_on_target", 0),
        "away_shots_on_target": stats.get("away_shots_on_target", 0),
        "shot_diff": stats.get("home_shots", 0) - stats.get("away_shots", 0),
        "home_corners": stats.get("home_corners", 0),
        "away_corners": stats.get("away_corners", 0),
        "home_fouls": stats.get("home_fouls", 0),
        "away_fouls": stats.get("away_fouls", 0),
        "home_yellow": stats.get("home_yellow_cards", 0),
        "away_yellow": stats.get("away_yellow_cards", 0),
        "home_red": stats.get("home_red_cards", 0),
        "away_red": stats.get("away_red_cards", 0),
    }

    X = np.array([[features[f] for f in win_features]])
    proba = win_model.predict_proba(X)[0]
    classes = list(win_model.classes_)

    result = {}
    for cls, prob in zip(classes, proba):
        result[cls] = round(float(prob), 4)

    return result


def kafka_consumer_loop():
    logger.info(f"Connecting to Kafka at {KAFKA_BROKER}...")

    consumer = None
    for attempt in range(30):
        try:
            consumer = KafkaConsumer(
                "match-stats",
                bootstrap_servers=KAFKA_BROKER,
                group_id="ml-predictor",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_interval_ms=300000,
            )
            logger.info("Connected to Kafka")
            break
        except Exception as e:
            logger.warning(f"Kafka not ready (attempt {attempt + 1}): {e}")
            time.sleep(2)

    if consumer is None:
        logger.error("Failed to connect to Kafka after 30 attempts")
        return

    for message in consumer:
        # Extract trace context from Kafka headers
        carrier = {}
        if message.headers:
            for key, value in message.headers:
                carrier[key] = value.decode("utf-8") if isinstance(value, bytes) else value

        ctx = extract(carrier)

        with tracer.start_as_current_span("process-match-stats", context=ctx) as span:
            try:
                stats = message.value
                match_id = stats.get("match_id", "unknown")
                last_event = stats.get("last_event", {})
                kafka_messages_processed.inc()

                span.set_attribute("match.id", match_id)
                span.set_attribute("match.minute", stats.get("minute", 0))

                predictions = {
                    "match_id": match_id,
                    "minute": stats.get("minute", 0),
                    "home_team": stats.get("home_team", ""),
                    "away_team": stats.get("away_team", ""),
                    "score": f"{stats.get('home_goals', 0)} - {stats.get('away_goals', 0)}",
                }

                if last_event.get("event_type") in ["shot", "goal"]:
                    with tracer.start_as_current_span("predict-xg"):
                        try:
                            start = time.time()
                            xg = predict_xg(last_event)
                            prediction_latency.labels(model_type="xg").observe(time.time() - start)

                            if xg is not None:
                                predictions["last_shot_xg"] = xg
                                predictions_total.labels(model_type="xg").inc()
                                xg_value.labels(match_id=match_id).set(xg)
                                logger.info(f"[{match_id}] min:{stats.get('minute')} xG={xg:.3f} ({last_event.get('player')})")

                                try:
                                    current = rdb.get(f"match:{match_id}:predictions")
                                    if current:
                                        current = json.loads(current)
                                        home_xg = current.get("home_xg", 0)
                                        away_xg = current.get("away_xg", 0)
                                    else:
                                        home_xg = 0
                                        away_xg = 0

                                    if last_event.get("team") == stats.get("home_team"):
                                        home_xg += xg
                                    else:
                                        away_xg += xg

                                    predictions["home_xg"] = round(home_xg, 3)
                                    predictions["away_xg"] = round(away_xg, 3)
                                except Exception as e:
                                    logger.error(f"Error accumulating xG: {e}")

                        except Exception as e:
                            logger.error(f"Error in xG prediction: {e}")

                with tracer.start_as_current_span("predict-win-probability"):
                    try:
                        start = time.time()
                        win_prob = predict_win_probability(stats)
                        prediction_latency.labels(model_type="win_prob").observe(time.time() - start)

                        if win_prob:
                            predictions["win_probability"] = win_prob
                            predictions_total.labels(model_type="win_prob").inc()
                            home_win_prob.labels(match_id=match_id).set(win_prob.get("home_win", 0))
                            logger.info(
                                f"[{match_id}] min:{stats.get('minute')} "
                                f"Win%: H={win_prob.get('home_win', 0):.1%} "
                                f"D={win_prob.get('draw', 0):.1%} "
                                f"A={win_prob.get('away_win', 0):.1%}"
                            )
                    except Exception as e:
                        logger.error(f"Error in win prob prediction: {e}")

                with tracer.start_as_current_span("redis-write-predictions"):
                    try:
                        rdb.set(f"match:{match_id}:predictions", json.dumps(predictions))
                    except Exception as e:
                        logger.error(f"Error storing predictions in Redis: {e}")

            except Exception as e:
                logger.error(f"Error processing message: {e}")


@app.on_event("startup")
def startup():
    thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    thread.start()
    logger.info("Kafka consumer thread started")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "ml-predictor",
        "xg_model_loaded": xg_model is not None,
        "win_model_loaded": win_model is not None,
    }


@app.get("/predict/xg")
def predict_xg_endpoint(x: float = 85, y: float = 50, shot_type: str = "foot"):
    event = {"x": x, "y": y, "detail": shot_type}
    xg = predict_xg(event)
    return {"xg": xg, "x": x, "y": y, "shot_type": shot_type}


@app.get("/metrics")
def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8084)# trigger build
# trigger build
