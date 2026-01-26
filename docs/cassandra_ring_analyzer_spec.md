# Cassandra Ring Analyzer - Technical Specification

## Overview
A Python script to visualize Cassandra token ring distribution with color-coded node ownership and gap detection.

## Objectives
- Parse Cassandra nodetool ring output files
- Calculate token ranges and ownership
- Detect gaps/holes in the token ring
- Generate circular ring visualization with color-coded segments
- Provide summary statistics and analysis

## Input Format
The script will parse nodetool ring output with the following structure:
```
Datacenter: dc3
==========
Address       Rack        Status State   Load            Owns                Token
10.142.94.20  rack1       Up     Normal  1.26 TiB        ?                   -9192732180794298069
...
```

## Data Model

### Token Entry
```python
{
    'address': str,      # Node IP address
    'rack': str,         # Rack identifier
    'status': str,       # Up/Down
    'state': str,        # Normal/Leaving/Joining/Moving
    'load': str,         # Data load (e.g., "1.26 TiB")
    'owns': str,         # Ownership percentage
    'token': int         # Token value (signed 64-bit integer)
}
```

### Token Range
```python
{
    'start_token': int,  # Range start (inclusive)
    'end_token': int,    # Range end (exclusive)
    'owner': str,        # Node IP address
    'size': int,         # Range size
    'is_gap': bool       # True if no owner (gap/hole)
}
```

## Architecture

### Module Structure
```
cassandra_ring_analyzer.py
├── RingParser          # Parse ring file
├── TokenAnalyzer       # Calculate ranges and detect gaps
├── RingVisualizer      # Generate matplotlib visualization
└── CLI                 # Command-line interface
```

## Core Components

### 1. RingParser
**Purpose**: Parse the nodetool ring output file

**Key Methods**:
- `parse_file(filepath)` → List[TokenEntry]
  - Skip header lines (Datacenter, separator)
  - Parse column-based format
  - Handle the orphaned token at the top
  - Convert token strings to integers

**Implementation Notes**:
- Use regex or split() for column parsing
- Handle variable whitespace
- Validate token format (signed 64-bit integers)

### 2. TokenAnalyzer
**Purpose**: Calculate token ranges and detect gaps

**Key Methods**:
- `calculate_ranges(tokens)` → List[TokenRange]
  - Sort tokens by value
  - Calculate ranges between consecutive tokens
  - Handle ring wrap-around (max token → min token)
  - Detect gaps where consecutive tokens have different owners

**Token Range Calculation Logic**:
```
Cassandra uses a 64-bit token space: -2^63 to 2^63-1

For sorted tokens [t1, t2, t3, ..., tn]:
- Range 1: t1 to t2 (owned by node at t2)
- Range 2: t2 to t3 (owned by node at t3)
- ...
- Range n: tn to t1 (wrap-around, owned by node at t1)

Gap Detection:
- If token[i] and token[i+1] have different owners → potential gap
- Verify by checking if there should be intermediate tokens
```

**Key Calculations**:
- Total token space: 2^64 = 18,446,744,073,709,551,616
- Range size: (end_token - start_token) % total_space
- Coverage percentage per node
- Gap identification

### 3. RingVisualizer
**Purpose**: Create circular ring visualization using matplotlib

**Key Methods**:
- `create_ring_plot(ranges, nodes_info)` → matplotlib.Figure
  - Draw circular ring with colored segments
  - Map token ranges to angular positions
  - Apply node-specific colors
  - Highlight gaps in white
  - Add legend and statistics

**Visualization Design**:
```
┌─────────────────────────────────────┐
│  Cassandra Token Ring Distribution  │
│                                     │
│         ╭─────────────╮            │
│      ╭──┤   Node 1    ├──╮         │
│    ╭─┤  │   (Red)     │  ├─╮       │
│   ╭┤ │  ╰─────────────╯  │ ├╮      │
│  ╭┤│ │                    │ │├╮     │
│  ││├─┤     Token Ring     ├─┤││     │
│  ││││ │                    │ ││││    │
│  ╰┤││ │   Node 2 (Blue)   │ ││├╯    │
│   ╰┤│ │   Node 3 (Yellow) │ ├╯      │
│    ╰─┤   Gap (White)      ├─╯       │
│      ╰──┤                ├──╯        │
│         ╰─────────────╯            │
│                                     │
│  Legend:                            │
│  ■ 10.142.94.18 - 3.37 TiB (45%)   │
│  ■ 10.142.94.19 - 1.31 TiB (25%)   │
│  ■ 10.142.94.20 - 1.26 TiB (25%)   │
│  □ Gaps - 5%                        │
└─────────────────────────────────────┘
```

**Color Assignment**:
- Use matplotlib's tab10/tab20 colormap for distinct colors
- Assign colors deterministically based on sorted node IPs
- Reserve white (#FFFFFF) for gaps
- Ensure sufficient contrast between adjacent segments

**Angular Mapping**:
```python
# Map token value to angle (0-360 degrees)
def token_to_angle(token_value):
    min_token = -2^63
    max_token = 2^63 - 1
    normalized = (token_value - min_token) / (max_token - min_token)
    return normalized * 360
```

**Ring Drawing**:
- Use `matplotlib.patches.Wedge` for each segment
- Inner radius: 0.6, Outer radius: 1.0
- Start angle and end angle from token values
- Add radial lines for major divisions

### 4. CLI (Command-Line Interface)
**Purpose**: Provide user-friendly command-line interface

**Arguments**:
```bash
python cassandra_ring_analyzer.py <ring_file> [options]

Required:
  ring_file              Path to nodetool ring output file

Options:
  -o, --output FILE      Output image file (default: ring_visualization.png)
  --format FORMAT        Output format: png, pdf, svg (default: png)
  --dpi DPI              Image resolution (default: 300)
  --size WxH             Figure size in inches (default: 12x10)
  --show                 Display plot interactively
  --stats-only           Print statistics without visualization
  -v, --verbose          Verbose output
```

## Statistics Output

### Console Summary
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

Largest Gap: 2.1% of token space
Smallest Range: 0.8% of token space
Average Range: 2.0% of token space

Balance Score: 0.92 (1.0 = perfect balance)
```

### Balance Metrics
- **Balance Score**: Standard deviation of range sizes (lower = better)
- **Coverage**: Percentage of token space with owners
- **Gap Analysis**: Number, size, and location of gaps

## Implementation Details

### Dependencies
```python
# requirements.txt
matplotlib>=3.5.0
numpy>=1.21.0
```

### Error Handling
- Invalid file format → Clear error message
- Missing tokens → Warning and continue
- Malformed token values → Skip with warning
- Empty ring file → Error and exit

### Performance Considerations
- Expected input: 100-1000 tokens
- Parsing: O(n) where n = number of lines
- Sorting: O(n log n) where n = number of tokens
- Visualization: O(n) for drawing segments
- Memory: Minimal, all data structures fit in RAM

## Testing Strategy

### Unit Tests
- Parse valid ring file
- Parse file with gaps
- Calculate ranges correctly
- Handle wrap-around
- Detect gaps accurately
- Color assignment consistency

### Integration Tests
- End-to-end with sample file
- Multiple datacenters (if applicable)
- Edge cases: single node, all gaps, no gaps

### Test Data
Use the provided sample ring file with:
- 3 unique nodes
- 48 tokens
- Known distribution pattern

## Future Enhancements
- Support for multiple datacenters in one visualization
- Interactive HTML output with plotly
- Historical comparison (multiple ring files)
- Rebalancing recommendations
- Export statistics to JSON/CSV
- Animated visualization of token movements

## Implementation Order
1. RingParser - Parse and validate input
2. TokenAnalyzer - Calculate ranges and gaps
3. Basic statistics output (console)
4. RingVisualizer - Create matplotlib plot
5. CLI - Command-line interface
6. Testing and refinement
7. Documentation and examples