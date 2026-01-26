# Cassandra Ring Analyzer

A Python tool to analyze and visualize Cassandra token ring distribution with color-coded node ownership and gap detection.

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
├── requirements.txt                    # Python dependencies
├── cassandra_ring_analyzer.py          # Main script
├── ring                                # Sample ring file
├── docs/
│   ├── cassandra_ring_analyzer_spec.md # Technical specification
│   ├── architecture_diagram.md         # Architecture diagrams
│   └── implementation_guide.md         # Implementation details
└── examples/
    └── ring_visualization.png          # Example output
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

- **Token Distribution Analysis**: Calculate and visualize how tokens are distributed across nodes
- **Gap Detection**: Identify missing token ranges in the ring
- **Color-Coded Visualization**: Each node gets a unique color for easy identification
- **Statistics**: Comprehensive metrics including balance score, coverage, and load distribution
- **Multiple Output Formats**: Save as PNG, PDF, or SVG
- **Interactive Mode**: View the visualization interactively with matplotlib

## Requirements

- Python 3.7+
- matplotlib >= 3.5.0
- numpy >= 1.21.0

## Documentation

See the `docs/` directory for detailed technical documentation:
- [`cassandra_ring_analyzer_spec.md`](docs/cassandra_ring_analyzer_spec.md) - Complete technical specification
- [`architecture_diagram.md`](docs/architecture_diagram.md) - System architecture and flow diagrams
- [`implementation_guide.md`](docs/implementation_guide.md) - Implementation details and algorithms

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.