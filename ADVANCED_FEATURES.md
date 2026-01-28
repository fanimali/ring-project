# Advanced Features Guide

This document describes the advanced features added to the Cassandra Ring Analyzer project.

## Overview

The ring analyzer has been extended with four powerful advanced features:

1. **Multi-Datacenter Support** - Analyze and compare multiple datacenters
2. **Interactive Visualization** - Create interactive HTML visualizations with Plotly
3. **Historical Analysis** - Track changes over time and identify trends
4. **Rebalancing Recommendations** - Get intelligent suggestions for improving ring balance

## Table of Contents

- [Multi-Datacenter Support](#multi-datacenter-support)
- [Interactive Visualization](#interactive-visualization)
- [Historical Analysis](#historical-analysis)
- [Rebalancing Recommendations](#rebalancing-recommendations)
- [Installation](#installation)
- [Examples](#examples)

---

## Multi-Datacenter Support

### Overview
The [`multi_dc_analyzer.py`](multi_dc_analyzer.py:1) module extends the basic analyzer to handle ring files containing multiple datacenters, providing comparative analysis and visualization.

### Features
- Parse ring files with multiple datacenters
- Analyze each datacenter independently
- Compare metrics across datacenters
- Generate side-by-side visualizations
- Create comparison charts for balance, gaps, and node distribution

### Usage

#### Basic Multi-DC Analysis
```bash
python multi_dc_analyzer.py ring_file.txt
```

#### Generate Comparison Plot
```bash
python multi_dc_analyzer.py ring_file.txt --comparison -o multi_dc_comparison.png
```

#### Interactive Display
```bash
python multi_dc_analyzer.py ring_file.txt --show
```

### Output
- **Ring Visualization**: Shows all datacenters in separate polar plots
- **Comparison Charts**: Bar charts comparing key metrics across DCs
- **Statistics Report**: Detailed per-datacenter statistics

### Example Output
```
Parsed 2 datacenter(s):
  - dc1: 48 tokens
  - dc2: 48 tokens

MULTI-DATACENTER RING ANALYSIS
======================================================================
Total Datacenters: 2

──────────────────────────────────────────────────────────────────────
Datacenter: dc1
──────────────────────────────────────────────────────────────────────
Total Nodes: 3
Total Tokens: 48
Balance Score: 0.923 (1.0 = perfect balance)
```

---

## Interactive Visualization

### Overview
The [`interactive_visualizer.py`](interactive_visualizer.py:1) module creates interactive HTML visualizations using Plotly, allowing users to zoom, pan, and hover for detailed information.

### Features
- Interactive ring visualization with hover tooltips
- Zoomable and pannable interface
- Statistics dashboard with gauges and charts
- Export to standalone HTML files
- No server required - works offline

### Usage

#### Create Interactive Ring
```bash
python interactive_visualizer.py ring_file.txt -o ring_interactive.html
```

#### Generate Statistics Dashboard
```bash
python interactive_visualizer.py ring_file.txt --dashboard -o dashboard.html
```

#### Auto-open in Browser
```bash
python interactive_visualizer.py ring_file.txt --show
```

### Interactive Features
- **Hover Information**: Detailed token range info on hover
- **Zoom/Pan**: Explore specific ring sections
- **Legend Toggle**: Click legend items to show/hide nodes
- **Responsive**: Works on desktop and mobile browsers

### Dashboard Metrics
1. Token distribution by node (bar chart)
2. Coverage percentage by node (bar chart)
3. Balance score gauge (with color-coded zones)
4. Node loads comparison (bar chart)

---

## Historical Analysis

### Overview
The [`historical_analyzer.py`](historical_analyzer.py:1) module tracks ring topology changes over time, comparing multiple snapshots to identify trends and movements.

### Features
- Compare multiple ring snapshots
- Track node additions/removals
- Monitor balance score trends
- Identify token movements
- Export comparison reports to JSON
- Visualize trends over time

### Usage

#### Compare Two Ring Files
```bash
python historical_analyzer.py ring_snapshot1.txt ring_snapshot2.txt
```

#### Analyze Trends (3+ snapshots)
```bash
python historical_analyzer.py ring_day1.txt ring_day2.txt ring_day3.txt --trends
```

#### Export Comparison to JSON
```bash
python historical_analyzer.py ring_before.txt ring_after.txt --export-json comparison.json
```

### Comparison Report
The tool generates a detailed report showing:
- **Node Changes**: Added, removed, and unchanged nodes
- **Token Changes**: Per-node token count changes
- **Balance Metrics**: Before/after balance scores
- **Gap Analysis**: Changes in gap count and percentage
- **Time Delta**: Duration between snapshots

### Example Report
```
RING COMPARISON REPORT
======================================================================
Time Period: 2026-01-27 10:00 → 2026-01-28 10:00
Duration: 1 day, 0:00:00

NODE CHANGES:
  Added: 1 nodes
    + 10.142.94.21
  Removed: 0 nodes
  Unchanged: 3 nodes

TOKEN CHANGES:
  Before: 48 tokens
  After: 64 tokens
  Change: +16

BALANCE METRICS:
  Balance Score Before: 0.923
  Balance Score After: 0.945
  Change: +0.022
  ✓ Balance IMPROVED
```

### Trend Analysis
When analyzing 3+ snapshots with `--trends`, the tool generates:
- Token count over time (line chart)
- Balance score over time (line chart)
- Node count over time (line chart)
- Gap percentage over time (line chart)

---

## Rebalancing Recommendations

### Overview
The [`rebalancing_advisor.py`](rebalancing_advisor.py:1) module analyzes ring imbalances and provides intelligent recommendations for token rebalancing.

### Features
- Comprehensive balance analysis
- Node-by-node deviation calculation
- Prioritized rebalancing recommendations
- Specific token movement suggestions
- Cost estimation (data movement, time)
- Impact scoring for each movement

### Usage

#### Generate Rebalancing Report
```bash
python rebalancing_advisor.py ring_file.txt
```

#### Suggest More Movements
```bash
python rebalancing_advisor.py ring_file.txt --max-movements 20
```

#### Export to JSON
```bash
python rebalancing_advisor.py ring_file.txt --export-json rebalancing_plan.json
```

### Analysis Components

#### 1. Balance Analysis
- Overall balance score
- Imbalance severity classification
- Per-node deviation from ideal
- Status classification (balanced, imbalanced, severely imbalanced)

#### 2. Recommendations
Each recommendation includes:
- **Node**: Target node address
- **Current Tokens**: Current token count
- **Recommended Tokens**: Ideal token count
- **Change**: Number of tokens to add/remove
- **Priority**: High, Medium, or Low
- **Reason**: Detailed explanation

#### 3. Token Movements
Specific suggestions for which tokens to move:
- **Token Value**: The token to move
- **From Node**: Source node
- **To Node**: Destination node
- **Impact Score**: Expected balance improvement

#### 4. Cost Estimation
- Number of movements required
- Estimated data movement (GB)
- Estimated time (minutes)
- Expected balance improvement

### Example Report
```
REBALANCING ANALYSIS REPORT
======================================================================

OVERALL BALANCE:
  Balance Score: 0.856
  Status: FAIR
  Balanced: ✗ NO

NODE ANALYSIS:
  ✗ 10.142.94.18:
      Current: 20 tokens
      Ideal: 16.0 tokens
      Deviation: +4.0 (+25.0%)
      Status: Severely Imbalanced
  
  ✓ 10.142.94.19:
      Current: 16 tokens
      Ideal: 16.0 tokens
      Deviation: +0.0 (+0.0%)
      Status: Balanced

RECOMMENDATIONS (2):

  1. 🔴 10.142.94.18 [HIGH PRIORITY]
      Current: 20 tokens
      Recommended: 16 tokens
      Change: -4 tokens
      Reason: Node has 4.0 more tokens than ideal (25.0% over). Consider removing tokens.

SUGGESTED TOKEN MOVEMENTS (Top 5):

  1. Move token -8234567890123456789
      From: 10.142.94.18
      To: 10.142.94.20
      Impact Score: 12.45

ESTIMATED REBALANCING COST:
  Movements: 5
  Data Movement: ~125.3 GB
  Estimated Time: ~75.2 minutes
  Current Balance: 0.856
  Expected Balance: 0.932
  Improvement: +0.076 (+8.9%)
```

---

## Installation

### Update Dependencies
```bash
pip install -r requirements.txt
```

### New Dependencies
The advanced features require additional packages:
- `plotly>=5.0.0` - Interactive visualizations
- `pandas>=1.3.0` - Data manipulation (optional, for future enhancements)

### Verify Installation
```bash
python -c "import plotly; import pandas; print('All dependencies installed!')"
```

---

## Examples

### Example 1: Complete Multi-DC Analysis
```bash
# Analyze multi-DC ring
python multi_dc_analyzer.py production_ring.txt -o multi_dc_viz.png

# Generate comparison charts
python multi_dc_analyzer.py production_ring.txt --comparison -o comparison.png
```

### Example 2: Interactive Dashboard
```bash
# Create interactive ring
python interactive_visualizer.py ring.txt -o ring.html --show

# Create dashboard
python interactive_visualizer.py ring.txt --dashboard -o dashboard.html --show
```

### Example 3: Historical Tracking
```bash
# Compare two snapshots
python historical_analyzer.py ring_monday.txt ring_friday.txt

# Track weekly trends
python historical_analyzer.py ring_week*.txt --trends -o weekly_trends.png

# Export comparison
python historical_analyzer.py ring_before.txt ring_after.txt --export-json changes.json
```

### Example 4: Rebalancing Workflow
```bash
# 1. Analyze current state
python rebalancing_advisor.py current_ring.txt

# 2. Export recommendations
python rebalancing_advisor.py current_ring.txt --export-json rebalancing_plan.json

# 3. After rebalancing, verify improvement
python historical_analyzer.py before_rebalance.txt after_rebalance.txt
```

### Example 5: Complete Analysis Pipeline
```bash
# 1. Basic analysis
python cassandra_ring_analyzer.py ring.txt -o basic_viz.png

# 2. Interactive exploration
python interactive_visualizer.py ring.txt --dashboard -o dashboard.html --show

# 3. Check if rebalancing needed
python rebalancing_advisor.py ring.txt

# 4. If multi-DC, compare datacenters
python multi_dc_analyzer.py ring.txt --comparison -o dc_comparison.png
```

---

## Integration with Existing Tools

### Using with Original Analyzer
All advanced features work alongside the original [`cassandra_ring_analyzer.py`](cassandra_ring_analyzer.py:1):

```bash
# Original static visualization
python cassandra_ring_analyzer.py ring.txt -o static.png

# Interactive version
python interactive_visualizer.py ring.txt -o interactive.html

# Both provide complementary views
```

### Programmatic Usage
```python
from cassandra_ring_analyzer import RingParser, TokenAnalyzer
from rebalancing_advisor import RebalancingAdvisor
from interactive_visualizer import InteractiveRingVisualizer

# Parse ring
parser = RingParser('ring.txt')
tokens = parser.parse_file()

# Analyze
analyzer = TokenAnalyzer(tokens)
ranges = analyzer.calculate_ranges()
ranges = analyzer.detect_gaps(ranges)
stats = analyzer.calculate_statistics(ranges)

# Get rebalancing advice
advisor = RebalancingAdvisor(tokens, ranges, stats)
recommendations = advisor.generate_recommendations()

# Create interactive visualization
visualizer = InteractiveRingVisualizer()
fig = visualizer.create_interactive_ring(ranges, stats)
visualizer.save_html(fig, 'output.html')
```

---

## Best Practices

### 1. Regular Monitoring
- Take ring snapshots daily or weekly
- Use historical analysis to track trends
- Set up alerts for balance score drops

### 2. Rebalancing Strategy
- Review recommendations before implementing
- Start with high-priority movements
- Verify improvements with historical comparison
- Monitor during and after rebalancing

### 3. Multi-DC Environments
- Analyze each datacenter independently
- Compare balance scores across DCs
- Ensure consistent token distribution
- Monitor cross-DC replication impact

### 4. Visualization Choice
- Use static plots for reports and documentation
- Use interactive visualizations for exploration
- Use dashboards for monitoring and presentations
- Use historical plots for trend analysis

---

## Troubleshooting

### Import Errors
If you see import errors for plotly or pandas:
```bash
pip install --upgrade plotly pandas
```

### Large Ring Files
For rings with 1000+ tokens:
- Use `--stats-only` for quick analysis
- Increase figure size: `--size 20x18`
- Consider sampling for visualization

### Memory Issues
For very large historical analyses:
- Analyze fewer snapshots at once
- Use `--export-json` to save intermediate results
- Process datacenters separately

---

## Future Enhancements

Potential additions to the advanced features:

1. **Real-time Monitoring**: Live ring state updates
2. **Automated Rebalancing**: Execute recommended movements
3. **Machine Learning**: Predict optimal token distribution
4. **Integration**: Connect to Cassandra directly via JMX
5. **Web Interface**: Full-featured web dashboard
6. **Alerting**: Automated notifications for imbalances

---

## Contributing

To contribute new features or improvements:

1. Follow the existing code structure
2. Add comprehensive docstrings
3. Include usage examples
4. Update this documentation
5. Add tests for new functionality

---

## License

Same as the main project (MIT License)

---

*Last Updated: 2026-01-28*