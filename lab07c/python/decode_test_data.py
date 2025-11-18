"""
Decode test_data checkpoint blobs from base64/msgpack to readable JSON format.
Output structure: test_data_output/{folder_name}/{_id}/blob/binary/1.json
"""

import json
import base64
import msgpack
from pathlib import Path
from typing import Any, Dict, List


def convert_to_json_serializable(obj: Any) -> Any:
    """Convert objects to JSON-serializable format."""
    if isinstance(obj, bytes):
        # Try to decode as msgpack first (for nested msgpack data)
        try:
            nested = msgpack.unpackb(obj, raw=False, strict_map_key=False)
            return convert_to_json_serializable(nested)
        except Exception:
            pass
        
        # Try to decode as UTF-8
        try:
            return obj.decode('utf-8')
        except UnicodeDecodeError:
            return f"<bytes: {obj.hex()}>"
    elif isinstance(obj, dict):
        return {convert_to_json_serializable(k): convert_to_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_json_serializable(item) for item in obj]
    else:
        return obj


def decode_msgpack_blob(base64_string: str) -> Any:
    """Decode a base64-encoded msgpack blob to a readable Python object."""
    if not base64_string:
        return None
    
    try:
        # Decode base64
        binary_data = base64.b64decode(base64_string)
        # Decode msgpack
        decoded_data = msgpack.unpackb(binary_data, raw=False, strict_map_key=False)
        # Convert to JSON-serializable format
        return convert_to_json_serializable(decoded_data)
    except Exception as e:
        return f"Error decoding: {str(e)}"


def process_json_file(input_file: Path, output_base: Path, folder_name: str) -> None:
    """Process a JSON file containing checkpoint data."""
    print(f"\nProcessing: {input_file}")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"  ⚠ Skipping - Invalid JSON format: {e}")
        return
    
    # Handle both single objects and arrays
    if isinstance(data, dict):
        data = [data]
    
    processed_count = 0
    for item in data:
        if not isinstance(item, dict):
            continue
        
        # Extract _id
        _id = None
        if '_id' in item:
            if isinstance(item['_id'], dict) and '$oid' in item['_id']:
                _id = item['_id']['$oid']
            elif isinstance(item['_id'], str):
                _id = item['_id']
        
        if not _id:
            print(f"  Skipping item without valid _id: {item.get('_id')}")
            continue
        
        # Extract blob data
        blob_data = None
        if 'blob' in item and isinstance(item['blob'], dict):
            if '$binary' in item['blob']:
                binary_obj = item['blob']['$binary']
                if isinstance(binary_obj, dict) and 'base64' in binary_obj:
                    base64_string = binary_obj['base64']
                    if base64_string:
                        blob_data = decode_msgpack_blob(base64_string)
        
        # Create output directory structure
        output_dir = output_base / folder_name / _id / "blob" / "binary"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create output file
        output_file = output_dir / "1.json"
        
        # Prepare output data
        output_data = {
            "metadata": {
                "_id": _id,
                "thread_id": item.get("thread_id"),
                "checkpoint_ns": item.get("checkpoint_ns"),
                "checkpoint_id": item.get("checkpoint_id"),
                "task_id": item.get("task_id"),
                "idx": item.get("idx"),
                "channel": item.get("channel"),
                "version": item.get("version"),
                "type": item.get("type"),
                "created_at": item.get("created_at"),
                "parent_checkpoint_id": item.get("parent_checkpoint_id"),
            },
            "decoded_blob": blob_data
        }
        
        # Remove None values from metadata
        output_data["metadata"] = {k: v for k, v in output_data["metadata"].items() if v is not None}
        
        # Write to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        processed_count += 1
        print(f"  ✓ Created: {output_file.relative_to(output_base.parent)}")
    
    print(f"  Processed {processed_count} items from {input_file.name}")


def main():
    """Main function to process all test_data files."""
    # Setup paths
    script_dir = Path(__file__).parent
    test_data_dir = script_dir / "test_data"
    output_dir = script_dir / "test_data_output"
    
    if not test_data_dir.exists():
        print(f"Error: test_data directory not found at {test_data_dir}")
        return
    
    # Clear output directory if it exists
    if output_dir.exists():
        import shutil
        shutil.rmtree(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("Decoding test_data checkpoint blobs")
    print("=" * 80)
    
    # Process each subdirectory
    folders = ["checkpoint_blobs", "checkpoints", "checkpoints_writes"]
    
    for folder_name in folders:
        folder_path = test_data_dir / folder_name
        if not folder_path.exists():
            print(f"\nSkipping {folder_name} - directory not found")
            continue
        
        print(f"\n{'='*80}")
        print(f"Processing folder: {folder_name}")
        print(f"{'='*80}")
        
        # Process all JSON files in the folder
        json_files = list(folder_path.glob("*.json"))
        if not json_files:
            print(f"  No JSON files found in {folder_name}")
            continue
        
        for json_file in json_files:
            process_json_file(json_file, output_dir, folder_name)
    
    print(f"\n{'='*80}")
    print(f"✓ Decoding complete!")
    print(f"Output directory: {output_dir}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
