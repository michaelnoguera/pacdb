import json
import glob
import os

def merge_benchmark_files():
    # Pattern to match all the benchmark files
    pattern = "benchmarks/q*-customer.sql.json"
    
    # Find all matching files
    files = glob.glob(pattern)
    
    # Sort files to ensure consistent ordering (optional)
    files.sort(key=lambda x: int(x.split('q')[1].split('-')[0]))
    
    all_data = []
    
    print(f"Found {len(files)} files to merge:")
    
    for file_path in files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                all_data.append(data)
                print(f"  ✓ {file_path}")
        except FileNotFoundError:
            print(f"  ✗ File not found: {file_path}")
        except json.JSONDecodeError as e:
            print(f"  ✗ Invalid JSON in {file_path}: {e}")
        except Exception as e:
            print(f"  ✗ Error reading {file_path}: {e}")
    
    # Create output directory if it doesn't exist
    os.makedirs("benchmarks", exist_ok=True)
    
    # Write merged data to all.json
    output_path = "benchmarks/all.json"
    try:
        with open(output_path, 'w') as f:
            json.dump(all_data, f, indent=2)
        print(f"\n✓ Successfully created {output_path} with {len(all_data)} entries")
    except Exception as e:
        print(f"\n✗ Error writing to {output_path}: {e}")

if __name__ == "__main__":
    merge_benchmark_files()