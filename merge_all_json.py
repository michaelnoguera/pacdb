import glob
import json
import os
import pathlib
import re

def merge_benchmark_files(benchmark_folder):
    # Pattern to match all the benchmark files
    pattern = str(benchmark_folder / "q*-customer.sql.json")
    
    # Find all matching files
    files = glob.glob(pattern)
    
    # Sort files to ensure consistent ordering (optional)
    def extract_query_number(filename):
        match = re.search(r'q(\d+)', filename)
        return int(match.group(1)) if match else float('inf')
    files.sort(key=extract_query_number)
    
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
    benchmark_folder.mkdir(exist_ok=True)
    
    # Write merged data to all.json
    output_path = benchmark_folder / "all.json"
    try:
        with open(output_path, 'w') as f:
            json.dump(all_data, f, indent=2)
        print(f"\n✓ Successfully created {output_path} with {len(all_data)} entries")
    except Exception as e:
        print(f"\n✗ Error writing to {output_path}: {e}")

    # Write TSV for gnuplot
    tsv_output_path = benchmark_folder / "all.tsv"
    try:
        with open(tsv_output_path, 'w') as f:
            f.write("Query\tStep1\tStep2\tStep3\tTotal\n")
            for entry in all_data:
                # rename "q1-customer.sql" to "Q1" for the query column
                query = entry.get("query", "unknown").replace("-customer.sql", "").upper()
                step1 = entry.get("step1", "NA")
                step2 = entry.get("step2", "NA")
                step3 = entry.get("step3", "NA")
                total = entry.get("step1", 0) + entry.get("step2", 0) + entry.get("step3", 0)
                f.write(f"{query}\t{step1}\t{step2}\t{step3}\t{total}\n")
        print(f"✓ Successfully created {tsv_output_path}")
    except Exception as e:
        print(f"✗ Error writing to {tsv_output_path}: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Merge benchmark JSON files into a single file.")
    parser.add_argument(
        "benchmark_folder",
        nargs="?",
        default="./benchmarks",
        type=str,
        help="Path to the benchmark folder containing individual benchmark JSON files (default: ./benchmarks)."
    )
    args = parser.parse_args()
    benchmark_folder = pathlib.Path(args.benchmark_folder)

    merge_benchmark_files(benchmark_folder)