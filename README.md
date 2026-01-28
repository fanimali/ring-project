# Cassandra Ring Analyzer

A comprehensive Python toolkit to analyze and visualize Cassandra token ring distribution with advanced features for multi-datacenter support, historical analysis, and rebalancing recommendations.

## Overview

This tool parses Cassandra `nodetool ring` output files and generates a circular visualization showing:
- Token distribution across nodes (color-coded by node)
- Token ranges owned by each node
- Gaps or holes in the token ring (shown in white)
- Load statistics and balance metrics

## Project Structure

```
ring-project/
├── README.md                           # This file
├── ADVANCED_FEATURES.md                # Advanced features guide
├── requirements.txt                    # Python dependencies
├── cassandra_ring_analyzer.py          # Main analyzer script
├── multi_dc_analyzer.py                # Multi-datacenter support
├── interactive_visualizer.py           # Interactive HTML visualizations
├── historical_analyzer.py              # Historical trend analysis
├── rebalancing_advisor.py              # Rebalancing recommendations
├── docs/
│   └── cassandra_ring_analyzer_spec.md # Technical specification
└── example-output/                     # Example visualizations
```

## Installation

1. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Basic usage:
```bash
python cassandra_ring_analyzer.py ring
```

With options:
```bash
python cassandra_ring_analyzer.py ring -o output.png --dpi 300 --show
```

### Command-Line Options

```
positional arguments:
  ring_file              Path to nodetool ring output file

optional arguments:
  -h, --help            Show this help message and exit
  -o, --output FILE     Output image file (default: ring_visualization.png)
  --format FORMAT       Output format: png, pdf, svg (default: png)
  --dpi DPI             Image resolution (default: 300)
  --size WxH            Figure size in inches (default: 12x10)
  --show                Display plot interactively
  --stats-only          Print statistics without visualization
  -v, --verbose         Verbose output
```

## Example Output

### Visualization
The tool generates a circular ring diagram with:
- Each node represented by a unique color
- Token ranges shown as colored segments
- Gaps/holes shown in white
- Legend with node information and statistics

### Statistics
```
Cassandra Ring Analysis Summary
================================
Datacenter: dc3
Total Nodes: 3
Total Tokens: 48

Node Distribution:
  10.142.94.18: 16 tokens (33.3%) - Load: 3.37 TiB
  10.142.94.19: 16 tokens (33.3%) - Load: 1.31 TiB
  10.142.94.20: 16 tokens (33.3%) - Load: 1.26 TiB

Token Space Coverage:
  Owned: 95.2%
  Gaps: 4.8% (3 gaps detected)

Balance Score: 0.92 (1.0 = perfect balance)
```

## Features

### Core Features
- **Token Distribution Analysis**: Calculate and visualize how tokens are distributed across nodes
- **Gap Detection**: Identify missing token ranges in the ring
- **Color-Coded Visualization**: Each node gets a unique color for easy identification
- **Statistics**: Comprehensive metrics including balance score, coverage, and load distribution
- **Multiple Output Formats**: Save as PNG, PDF, or SVG
- **Interactive Mode**: View the visualization interactively with matplotlib

### Advanced Features ✨ NEW
- **Multi-Datacenter Support**: Analyze and compare multiple datacenters in a single view
- **Interactive Visualizations**: Create zoomable, hoverable HTML dashboards with Plotly
- **Historical Analysis**: Track ring changes over time and identify trends
- **Rebalancing Recommendations**: Get intelligent suggestions for improving ring balance

See [`ADVANCED_FEATURES.md`](ADVANCED_FEATURES.md) for detailed documentation.

## Requirements

- Python 3.7+
- matplotlib >= 3.5.0
- numpy >= 1.21.0
- plotly >= 5.0.0 (for interactive features)
- pandas >= 1.3.0 (for advanced analysis)

## Quick Start Examples

### Basic Analysis
```bash
python cassandra_ring_analyzer.py ring.txt -o visualization.png
```

### Multi-Datacenter Analysis
```bash
python multi_dc_analyzer.py ring.txt --comparison -o dc_comparison.png
```

### Interactive Dashboard
```bash
python interactive_visualizer.py ring.txt --dashboard --show
```

### Historical Comparison
```bash
python historical_analyzer.py ring_before.txt ring_after.txt
```

### Rebalancing Recommendations
```bash
python rebalancing_advisor.py ring.txt --export-json rebalancing_plan.json
```

## Documentation

- [`ADVANCED_FEATURES.md`](ADVANCED_FEATURES.md) - Complete guide to advanced features
- [`docs/cassandra_ring_analyzer_spec.md`](docs/cassandra_ring_analyzer_spec.md) - Technical specification

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.