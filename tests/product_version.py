"""Protocol authority for standalone test scenarios."""
import json
from pathlib import Path

MODEL = Path(__file__).resolve().parents[1] / "metadata/product.json"
PRODUCT = json.loads(MODEL.read_text(encoding="utf-8"))["product"]
PROTOCOL_VERSION = PRODUCT["protocolVersion"]
CATALOG_REVISION = PRODUCT["catalogRevision"]
