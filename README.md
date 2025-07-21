# Project Jungle

Project Jungle is a disk usage analytics toolkit for large-scale filesystem environments, particularly designed for parsing `ls -lR`-style outputs on clusters (e.g., GPFS). It produces interactive HTML visualizations and CSV reports to assist administrators and researchers in identifying large, aging, or unused files.

---

## Features

- Hierarchical tree visualization of directory structure, file sizes, and age.
- Per-user file usage summaries and largest-file listings.
- Aggregation of usage by configurable time bins (e.g., last accessed within 2.5 years).
- Efficient, scriptable processing using `pandas` and `argparse`.
- Output in both CSV and HTML formats.

---

## Directory Structure

| File            | Description |
|-----------------|-------------|
| `main.py`       | Generates hierarchical tree, visualization (`dash_complete.html`), and `flattened.csv`. |
| `stats.py`      | User-level summary: total storage in different age bins. |
| `summary.py`    | Global summary: file count and storage across age bins. |
| `users.py`      | Per-user reports: top N files, storage summary, biggest file. |
| `templates/dash.html` | HTML template for inserting JSON tree into interactive dashboard. |
| `config.json`   | Thresholds and parameters for size and age filtering. |

---

## Input Format

All scripts take as input a `.list` file from `ls -lR` or equivalent. The required fields and expected column positions are:

| Column Index | Meaning |
|--------------|---------|
| 4            | Owner username |
| 6            | File size (bytes) |
| 8            | Last access time (UNIX epoch) |
| 11           | Full path |

Example line:
```
-rw------- 1 jsmith proj1 4376 8 1586037253.064382 1316204530.000000 /gpfs4/data/project/foo.txt
```

---

## Usage

### Step 1: Configure Thresholds

Create/update `config.json` file:

```json
{
    "analysis_parameter": {
        "years": 10,
        "levels": 5,
        "gb_threshold": 10
    },
    "color_mapping": {
        "bins": [
            "less than 2.5",
            "between 2.5 and 5",
            "between 5 and 7.5",
            "between 7.5 and 10",
            "10 years or more"
        ],
        "colors": {
            "less than 2.5": "rgb(255,127,0)",
            "between 2.5 and 5": "rgb(55,126,184",
            "between 5 and 7.5": "rgb(77,175,74)",
            "between 7.5 and 10": "rgb(152,78,163)",
            "10 years or more": "rgb(228,26,28)"
        }
        
    }
}
```

- `analysis_parameter.gb_threshold`: Minimum directory size (in GB) to include in output.
- `analysis_parameter.years`: Maximum age (in years) to consider for binning.
- `analysis_parameter.levels`: Maximum depth of directory levels to analyze.
- `color_mapping.bins`: Ordered labels for time-based access bins.
- `color_mapping.colors`: RGB color mapping for each bin in the HTML visualization.

---

### Step 2: Generate Tree + Flattened CSV + HTML

```bash
python main.py -f your_data.list -o output_dir/
```

This produces:
- `flattened.csv`: Tabular file-level breakdown (path, size, age bin).
- `dash_complete.html`: Interactive treemap visualization.

---

### Step 3: Per-User Summary

```bash
python users.py -f your_data.list -o output_dir/ -n 10
```

Generates:
- `top_files_per_user.csv`: Top N largest files per user.
- `user_storage_summary.csv`: Total storage and file count per user.
- `biggest_file_per_user.csv`: Each user’s single largest file.

---

### Step 4: Summary by User and File Age

```bash
python stats.py -i your_data.list -o output_dir/stats_summary.csv
```

Produces a CSV with total storage per user split into time bins such as:

| owner   | Size-GB-less than 2.5 | Size-GB-between 2.5 and 5.0 | ... |
|---------|------------------------|------------------------------|-----|
| John Cluster   | 120.54                 | 15.32                        | ... |
| Jane Computing     |  43.19                 |  3.01                        | ... |

---

### Step 5: Global Aging Summary (All Users Combined)

```bash
python summary.py -i your_data.list -o output_dir/global_summary.csv
```

Outputs:

| Age (years)     | N        | size (GB) |
|-----------------|----------|-----------|
| < 2.5           | 10,446,739 | 634,217   |
| 2.5–5           | 14,653,281 | 19,271    |
| ...             | ...        | ...       |

---

## Output Format Descriptions

### `flattened.csv`
| Column         | Description |
|----------------|-------------|
| Path           | File or directory path |
| File_Size_GB   | Size in GB |
| Type           | `file` or `directory` |
| Age_Bin        | One of: `<2.5`, `2.5–5`, `5–7.5`, `7.5–10`, `>10` years |

### `dash_complete.html`
A full-page HTML file with a collapsible tree (treemap) showing directory size and access age.

---

## Notes

- Files owned by `root` are excluded by default.
- Paths are cleaned to remove `/gpfs4` prefix.
- Time bins can be modified via `summary.py` and `stats.py`.
- Interactive tree visualization is based on D3.js with a templated insertion of hierarchical JSON data.

---

## License

Open Source

\-Reetom Gangopadhyay
