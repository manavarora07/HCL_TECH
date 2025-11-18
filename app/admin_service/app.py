from flask import Flask, jsonify, request, send_file
from pathlib import Path

app = Flask(__name__)
ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

@app.route("/staged", methods=["GET"])
def staged():
    f = DATA_DIR / "staged.csv"
    if not f.exists():
        return jsonify({"exists": False}), 404
    return send_file(str(f), mimetype="text/csv")

@app.route("/validate", methods=["POST"])
def validate_endpoint():
    """
    POST -> run validation on staged CSV and return JSON result summary.
    """
    staged = DATA_DIR / "staged.csv"
    if not staged.exists():
        return jsonify({"error": "no staged file"}), 400

    try:
        from etl import validate
        ok = validate.run_validation(str(staged))
        return jsonify({"validation_ok": bool(ok)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
