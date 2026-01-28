#!/usr/bin/env python3
"""
Historical Ring Analyzer
Compare multiple ring snapshots over time to track changes and trends.
"""

from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field
from datetime import datetime
import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from cassandra_ring_analyzer import TokenEntry, TokenRange, RingParser, TokenAnalyzer


@dataclass
class RingSnapshot:
    """Represents a ring state at a specific point in time."""
    timestamp: datetime
    filepath: str
    datacenter: str
    tokens: List[TokenEntry] = field(default_factory=list)
    ranges: List[TokenRange] = field(default_factory=list)
    stats: Dict = field(default_factory=dict)
    
    def __post_init__(self):
        """Parse and analyze the ring file."""
        parser = RingParser(self.filepath)
        self.tokens = parser.parse_file()
        self.datacenter = parser.datacenter
        
        analyzer = TokenAnalyzer(self.tokens)
        self.ranges = analyzer.calculate_ranges()
        self.ranges = analyzer.detect_gaps(self.ranges)
        self.stats = analyzer.calculate_statistics(self.ranges)
        self.stats['datacenter'] = self.datacenter


class HistoricalAnalyzer:
    """Analyzes changes in ring topology over time."""
    
    def __init__(self):
        self.snapshots: List[RingSnapshot] = []
    
    def add_snapshot(self, filepath: str, timestamp: Optional[datetime] = None):
        """
        Add a ring snapshot for analysis.
        
        Args:
            filepath: Path to ring file
            timestamp: Timestamp of snapshot (defaults to now)
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        snapshot = RingSnapshot(
            timestamp=timestamp,
            filepath=filepath,
            datacenter=""  # Will be set during parsing
        )
        
        self.snapshots.append(snapshot)
        self.snapshots.sort(key=lambda s: s.timestamp)
        
        print(f"Added snapshot from {filepath} at {timestamp}")
    
    def compare_snapshots(self, idx1: int, idx2: int) -> Dict:
        """
        Compare two snapshots and identify changes.
        
        Args:
            idx1: Index of first snapshot
            idx2: Index of second snapshot
            
        Returns:
            Dictionary containing comparison results
        """
        if idx1 >= len(self.snapshots) or idx2 >= len(self.snapshots):
            raise IndexError("Snapshot index out of range")
        
        snap1 = self.snapshots[idx1]
        snap2 = self.snapshots[idx2]
        
        # Extract node sets
        nodes1 = set(t.address for t in snap1.tokens)
        nodes2 = set(t.address for t in snap2.tokens)
        
        # Identify changes
        added_nodes = nodes2 - nodes1
        removed_nodes = nodes1 - nodes2
        common_nodes = nodes1 & nodes2
        
        # Token changes per node
        token_changes = {}
        for node in common_nodes:
            tokens1 = len([t for t in snap1.tokens if t.address == node])
            tokens2 = len([t for t in snap2.tokens if t.address == node])
            if tokens1 != tokens2:
                token_changes[node] = {
                    'before': tokens1,
                    'after': tokens2,
                    'change': tokens2 - tokens1
                }
        
        # Balance score change
        balance_change = snap2.stats['balance_score'] - snap1.stats['balance_score']
        
        # Gap changes
        gap_change = snap2.stats['gap_count'] - snap1.stats['gap_count']
        gap_pct_change = snap2.stats['gap_percentage'] - snap1.stats['gap_percentage']
        
        comparison = {
            'timestamp1': snap1.timestamp,
            'timestamp2': snap2.timestamp,
            'time_delta': snap2.timestamp - snap1.timestamp,
            'nodes_added': list(added_nodes),
            'nodes_removed': list(removed_nodes),
            'nodes_unchanged': list(common_nodes),
            'token_changes': token_changes,
            'total_tokens_before': len(snap1.tokens),
            'total_tokens_after': len(snap2.tokens),
            'balance_score_before': snap1.stats['balance_score'],
            'balance_score_after': snap2.stats['balance_score'],
            'balance_change': balance_change,
            'gaps_before': snap1.stats['gap_count'],
            'gaps_after': snap2.stats['gap_count'],
            'gap_change': gap_change,
            'gap_pct_change': gap_pct_change
        }
        
        return comparison
    
    def detect_trends(self) -> Dict:
        """
        Analyze trends across all snapshots.
        
        Returns:
            Dictionary containing trend analysis
        """
        if len(self.snapshots) < 2:
            return {'error': 'Need at least 2 snapshots for trend analysis'}
        
        trends = {
            'timestamps': [s.timestamp for s in self.snapshots],
            'total_tokens': [len(s.tokens) for s in self.snapshots],
            'node_counts': [len(s.stats.get('nodes', {})) for s in self.snapshots],
            'balance_scores': [s.stats['balance_score'] for s in self.snapshots],
            'gap_counts': [s.stats['gap_count'] for s in self.snapshots],
            'gap_percentages': [s.stats['gap_percentage'] for s in self.snapshots]
        }
        
        # Calculate trends
        trends['token_trend'] = 'increasing' if trends['total_tokens'][-1] > trends['total_tokens'][0] else 'decreasing' if trends['total_tokens'][-1] < trends['total_tokens'][0] else 'stable'
        trends['balance_trend'] = 'improving' if trends['balance_scores'][-1] > trends['balance_scores'][0] else 'degrading' if trends['balance_scores'][-1] < trends['balance_scores'][0] else 'stable'
        trends['gap_trend'] = 'increasing' if trends['gap_counts'][-1] > trends['gap_counts'][0] else 'decreasing' if trends['gap_counts'][-1] < trends['gap_counts'][0] else 'stable'
        
        return trends
    
    def export_comparison(self, comparison: Dict, filepath: str):
        """Export comparison results to JSON file."""
        # Convert datetime objects to strings for JSON serialization
        export_data = comparison.copy()
        export_data['timestamp1'] = comparison['timestamp1'].isoformat()
        export_data['timestamp2'] = comparison['timestamp2'].isoformat()
        export_data['time_delta'] = str(comparison['time_delta'])
        
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        print(f"Comparison exported to: {filepath}")


class HistoricalVisualizer:
    """Visualizes historical trends and comparisons."""
    
    def create_trend_plot(self, trends: Dict, figsize: Tuple[int, int] = (14, 10)) -> plt.Figure:
        """
        Create a multi-panel trend visualization.
        
        Args:
            trends: Trends dictionary from HistoricalAnalyzer
            figsize: Figure size in inches
            
        Returns:
            matplotlib Figure
        """
        fig, axes = plt.subplots(2, 2, figsize=figsize)
        
        timestamps = trends['timestamps']
        time_labels = [t.strftime('%Y-%m-%d %H:%M') for t in timestamps]
        
        # 1. Token count over time
        ax1 = axes[0, 0]
        ax1.plot(time_labels, trends['total_tokens'], marker='o', linewidth=2, color='steelblue')
        ax1.set_title('Total Tokens Over Time', fontweight='bold', fontsize=12)
        ax1.set_ylabel('Token Count')
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='x', rotation=45)
        
        # 2. Balance score over time
        ax2 = axes[0, 1]
        ax2.plot(time_labels, trends['balance_scores'], marker='o', linewidth=2, color='mediumseagreen')
        ax2.axhline(y=0.9, color='green', linestyle='--', alpha=0.5, label='Good (>0.9)')
        ax2.axhline(y=0.7, color='orange', linestyle='--', alpha=0.5, label='Fair (>0.7)')
        ax2.set_title('Balance Score Over Time', fontweight='bold', fontsize=12)
        ax2.set_ylabel('Balance Score')
        ax2.set_ylim(0, 1.1)
        ax2.legend(fontsize=8)
        ax2.grid(True, alpha=0.3)
        ax2.tick_params(axis='x', rotation=45)
        
        # 3. Node count over time
        ax3 = axes[1, 0]
        ax3.plot(time_labels, trends['node_counts'], marker='o', linewidth=2, color='coral')
        ax3.set_title('Node Count Over Time', fontweight='bold', fontsize=12)
        ax3.set_ylabel('Number of Nodes')
        ax3.grid(True, alpha=0.3)
        ax3.tick_params(axis='x', rotation=45)
        
        # 4. Gap percentage over time
        ax4 = axes[1, 1]
        ax4.plot(time_labels, trends['gap_percentages'], marker='o', linewidth=2, color='crimson')
        ax4.set_title('Gap Percentage Over Time', fontweight='bold', fontsize=12)
        ax4.set_ylabel('Gap Percentage (%)')
        ax4.grid(True, alpha=0.3)
        ax4.tick_params(axis='x', rotation=45)
        
        fig.suptitle('Ring Topology Trends', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        return fig
    
    def create_comparison_plot(self, comparison: Dict, figsize: Tuple[int, int] = (12, 8)) -> plt.Figure:
        """
        Create a visualization comparing two snapshots.
        
        Args:
            comparison: Comparison dictionary from HistoricalAnalyzer
            figsize: Figure size in inches
            
        Returns:
            matplotlib Figure
        """
        fig, axes = plt.subplots(2, 2, figsize=figsize)
        
        # 1. Node changes
        ax1 = axes[0, 0]
        categories = ['Added', 'Removed', 'Unchanged']
        counts = [
            len(comparison['nodes_added']),
            len(comparison['nodes_removed']),
            len(comparison['nodes_unchanged'])
        ]
        colors = ['green', 'red', 'gray']
        ax1.bar(categories, counts, color=colors, alpha=0.7)
        ax1.set_title('Node Changes', fontweight='bold')
        ax1.set_ylabel('Count')
        ax1.grid(axis='y', alpha=0.3)
        
        # 2. Token count change
        ax2 = axes[0, 1]
        ax2.bar(['Before', 'After'], 
               [comparison['total_tokens_before'], comparison['total_tokens_after']],
               color=['steelblue', 'darkblue'], alpha=0.7)
        ax2.set_title('Total Token Count', fontweight='bold')
        ax2.set_ylabel('Tokens')
        ax2.grid(axis='y', alpha=0.3)
        
        # 3. Balance score change
        ax3 = axes[1, 0]
        balance_before = comparison['balance_score_before']
        balance_after = comparison['balance_score_after']
        balance_change = comparison['balance_change']
        
        bars = ax3.bar(['Before', 'After'], [balance_before, balance_after],
                      color=['orange' if balance_before < 0.9 else 'green',
                            'orange' if balance_after < 0.9 else 'green'],
                      alpha=0.7)
        ax3.axhline(y=0.9, color='green', linestyle='--', alpha=0.5)
        ax3.set_title(f'Balance Score (Δ: {balance_change:+.3f})', fontweight='bold')
        ax3.set_ylabel('Score')
        ax3.set_ylim(0, 1.1)
        ax3.grid(axis='y', alpha=0.3)
        
        # 4. Gap changes
        ax4 = axes[1, 1]
        ax4.bar(['Before', 'After'],
               [comparison['gaps_before'], comparison['gaps_after']],
               color=['coral', 'crimson'], alpha=0.7)
        ax4.set_title(f'Gap Count (Δ: {comparison["gap_change"]:+d})', fontweight='bold')
        ax4.set_ylabel('Gaps')
        ax4.grid(axis='y', alpha=0.3)
        
        # Add overall title with time information
        time_delta = comparison['time_delta']
        fig.suptitle(f'Ring Comparison\n{comparison["timestamp1"].strftime("%Y-%m-%d %H:%M")} → '
                    f'{comparison["timestamp2"].strftime("%Y-%m-%d %H:%M")} (Δ: {time_delta})',
                    fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        return fig


def print_comparison_report(comparison: Dict):
    """Print a detailed comparison report."""
    print("\n" + "="*70)
    print("RING COMPARISON REPORT")
    print("="*70)
    print(f"Time Period: {comparison['timestamp1'].strftime('%Y-%m-%d %H:%M')} → "
          f"{comparison['timestamp2'].strftime('%Y-%m-%d %H:%M')}")
    print(f"Duration: {comparison['time_delta']}")
    print()
    
    print("NODE CHANGES:")
    print(f"  Added: {len(comparison['nodes_added'])} nodes")
    if comparison['nodes_added']:
        for node in comparison['nodes_added']:
            print(f"    + {node}")
    
    print(f"  Removed: {len(comparison['nodes_removed'])} nodes")
    if comparison['nodes_removed']:
        for node in comparison['nodes_removed']:
            print(f"    - {node}")
    
    print(f"  Unchanged: {len(comparison['nodes_unchanged'])} nodes")
    print()
    
    print("TOKEN CHANGES:")
    print(f"  Before: {comparison['total_tokens_before']} tokens")
    print(f"  After: {comparison['total_tokens_after']} tokens")
    print(f"  Change: {comparison['total_tokens_after'] - comparison['total_tokens_before']:+d}")
    
    if comparison['token_changes']:
        print("\n  Per-Node Token Changes:")
        for node, changes in comparison['token_changes'].items():
            print(f"    {node}: {changes['before']} → {changes['after']} ({changes['change']:+d})")
    print()
    
    print("BALANCE METRICS:")
    print(f"  Balance Score Before: {comparison['balance_score_before']:.3f}")
    print(f"  Balance Score After: {comparison['balance_score_after']:.3f}")
    print(f"  Change: {comparison['balance_change']:+.3f}")
    
    if comparison['balance_change'] > 0:
        print("  ✓ Balance IMPROVED")
    elif comparison['balance_change'] < 0:
        print("  ✗ Balance DEGRADED")
    else:
        print("  = Balance UNCHANGED")
    print()
    
    print("GAP ANALYSIS:")
    print(f"  Gaps Before: {comparison['gaps_before']}")
    print(f"  Gaps After: {comparison['gaps_after']}")
    print(f"  Change: {comparison['gap_change']:+d}")
    print(f"  Gap % Change: {comparison['gap_pct_change']:+.2f}%")
    print("="*70 + "\n")


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Analyze historical changes in Cassandra ring topology'
    )
    parser.add_argument('ring_files', nargs='+', help='Ring files to compare (in chronological order)')
    parser.add_argument('-o', '--output', default='historical_analysis.png',
                       help='Output image file')
    parser.add_argument('--export-json', help='Export comparison to JSON file')
    parser.add_argument('--trends', action='store_true',
                       help='Generate trends plot instead of comparison')
    parser.add_argument('--dpi', type=int, default=300,
                       help='Image resolution')
    
    args = parser.parse_args()
    
    if len(args.ring_files) < 2:
        print("Error: Need at least 2 ring files for comparison")
        exit(1)
    
    # Create analyzer and add snapshots
    analyzer = HistoricalAnalyzer()
    
    for i, filepath in enumerate(args.ring_files):
        # Use file modification time or sequential timestamps
        timestamp = datetime.now().replace(hour=i, minute=0, second=0, microsecond=0)
        analyzer.add_snapshot(filepath, timestamp)
    
    # Generate visualization
    visualizer = HistoricalVisualizer()
    
    if args.trends:
        # Trend analysis
        trends = analyzer.detect_trends()
        fig = visualizer.create_trend_plot(trends)
        print(f"\nTrend Analysis:")
        print(f"  Token Trend: {trends['token_trend']}")
        print(f"  Balance Trend: {trends['balance_trend']}")
        print(f"  Gap Trend: {trends['gap_trend']}")
    else:
        # Compare first and last snapshots
        comparison = analyzer.compare_snapshots(0, len(analyzer.snapshots) - 1)
        print_comparison_report(comparison)
        fig = visualizer.create_comparison_plot(comparison)
        
        # Export if requested
        if args.export_json:
            analyzer.export_comparison(comparison, args.export_json)
    
    # Save figure
    fig.savefig(args.output, dpi=args.dpi, bbox_inches='tight', facecolor='white')
    print(f"Visualization saved to: {args.output}")

# Made with Bob
