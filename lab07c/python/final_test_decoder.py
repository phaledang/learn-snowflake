import base64
import pickle
import pickletools
import json
import sys
from typing import Any

# ============================================================
# Utility helpers
# ============================================================

def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def print_header(title: str):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90 + "\n")


def disassemble(b64: str):
    """Show pickletools disassembly from base64 string."""
    raw = base64.b64decode(b64)
    print(pickletools.dis(raw))


def try_unpickle(b64: str):
    """Try to unpickle. If missing classes, show error but continue."""
    raw = base64.b64decode(b64)
    try:
        return pickle.loads(raw)
    except Exception as e:
        print(f"⚠️ Cannot unpickle (classes missing): {e}\n")
        return None


def safe_repr(obj):
    """Best effort pretty printing for LangChain message objects."""
    if obj is None:
        return None

    # LangChain message-like objects
    try:
        if hasattr(obj, "content"):
            return {
                "type": obj.__class__.__name__,
                "content": obj.content,
                "additional_kwargs": getattr(obj, "additional_kwargs", {}),
                "response_metadata": getattr(obj, "response_metadata", {}),
                "id": getattr(obj, "id", None)
            }
    except:
        pass

    # Generic fallback
    try:
        return json.loads(json.dumps(obj, default=str))
    except:
        return str(obj)


# ============================================================
# Main function
# ============================================================

def decode_checkpoint_json(json_path: str="test_data/checkpoint_example.json"):
    doc = load_json(json_path)

    print_header("1️⃣  Plain top-level fields (non-encoded)")
    non_encoded = {
        k: v for k, v in doc.items()
        if k not in ("checkpoint_data", "writes")
    }
    print(json.dumps(non_encoded, indent=4, default=str))

    # -----------------------------------------------------
    # CHECKPOINT_DATA
    # -----------------------------------------------------
    print_header("2️⃣  checkpoint_data → BASE64 → pickletools → (attempt unpickle)")
    cp = doc.get("checkpoint_data")

    print_header("2.1 Raw pickletools disassembly")
    disassemble(cp)

    print_header("2.2 Attempting to unpickle")
    decoded = try_unpickle(cp)
    print("Decoded object (raw):")
    print(decoded)

    print_header("2.3 Human-readable reconstruction")

    if isinstance(decoded, dict):
        pretty = {}
        for k, v in decoded.items():
            if k == "channel_values":
                # look for messages in standard LangGraph location
                msgs = (
                    v.get("messages")
                    or v.get("__start__", {}).get("messages")
                )
                pretty["messages"] = [safe_repr(m) for m in msgs] if msgs else None
            else:
                pretty[k] = v
        print(json.dumps(pretty, indent=4, default=str))
    else:
        print("❌ Could not reconstruct due to missing classes.")

    # -----------------------------------------------------
    # WRITES[*].value
    # -----------------------------------------------------
    print_header("3️⃣  Decoding writes[*].value")

    for i, w in enumerate(doc.get("writes", [])):
        print_header(f"Write #{i}")

        print("Metadata:")
        print(json.dumps({
            "task_id": w["task_id"],
            "idx": w["idx"],
            "channel": w["channel"],
            "type": w["type"],
            "created_at": w["created_at"]
        }, indent=4, default=str))

        val_b64 = w["value"]

        print_header("3.1 Pickletools disassembly")
        disassemble(val_b64)

        print_header("3.2 Attempt to unpickle")
        val_obj = try_unpickle(val_b64)

        print("Human-readable:")
        print(safe_repr(val_obj))


# ============================================================
# CLI Entry point
# ============================================================

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python final_test_decoder.py <checkpoint.json>")
        sys.exit(1)

    decode_checkpoint_json(sys.argv[1])
