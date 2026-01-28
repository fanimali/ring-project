#!/usr/bin/env python3
"""
Multi-Datacenter Ring Analyzer
Extends the basic ring analyzer to support multiple datacenters.
"""

from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from cassandra_ring_analyzer import TokenEntry, TokenRange, RingParser, TokenAnalyzer


@dataclass
class DatacenterInfo:
    """Information about a single datacenter."""
    name: str
    tokens: List[TokenEntry] = field(default_factory=list)
    ranges: List[TokenRange] = field(default_factory=list)
    stats: Dict = field(default_factory=dict)


class MultiDCRingParser:
    """Parses ring files that may contain multiple datacenters."""
    
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.datacenters: Dict[str, DatacenterInfo] = {}
    
    def parse_file(self) -> Dict[str, DatacenterInfo]:
        """
        Parse ring file and return dictionary of datacenter information.
        
        Returns:
            Dict mapping datacenter names to DatacenterInfo objects
        """
        try:
            with open(self.filepath, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            raise FileNotFoundError(f"Ring file '{self.filepath}' not found")
        except Exception as e:
            raise Exception(f"Error reading file: {e}")
        
        current_dc = None
        i = 0
        
        while i < len(lines):
            line = lines[i].strip()
            
            # Detect datacenter header
            if line.startswith('Datacenter:'):
                dc_name = line.split(':')[1].strip()
                current_dc = dc_name
                
                if dc_name not in self.datacenters:
                    self.datacenters[dc_name] = DatacenterInfo(name=dc_name)
                
                # Find the Address header for this DC
                i += 1
                while i < len(lines) and not lines[i].strip().startswith('Address'):
                    i += 1
                
                # Skip the Address header line
                i += 1
                
                # Parse tokens for this datacenter
                while i < len(lines):
                    line = lines[i].strip()
                    
                    # Stop if we hit another datacenter or end of file
                    if not line or line.startswith('Datacenter:'):
                        break
                    
                    # Parse token line
                    parts = line.split()
                    
                    # Skip orphaned tokens
                    if len(parts) == 1:
                        i += 1
                        continue
                    
                    if len(parts) >= 7:
                        try:
                            address = parts[0]
                            rack = parts[1]
                            status = parts[2]
                            state = parts[3]
                            load = f"{parts[4]} {parts[5]}"
                            owns = parts[6]
                            
                            if len(parts) >= 8:
                                token = int(parts[7])
                            else:
                                i += 1
                                continue
                            
                            token_entry = TokenEntry(
                                address=address,
                                rack=rack,
                                status=status,
                                state=state,
                                load=load,
                                owns=owns,
                                token=token
                            )
                            self.datacenters[current_dc].tokens.append(token_entry)
                        except (ValueError, IndexError):
                            pass
                    
                    i += 1
                continue
            
            i += 1
        
        if not self.datacenters:
            raise ValueError("No datacenters found in ring file")
        
        print(f"Parsed {len(self.datacenters)} datacenter(s):")
        for dc_name, dc_info in self.datacenters.items():
            print(f"  - {dc_name}: {len(dc_info.tokens)} tokens")
        
        return self.datacenters
    
    def analyze_all_datacenters(self) -> Dict[str, DatacenterInfo]:
        """
        Analyze token distribution for all datacenters.
        
        Returns:
            Updated datacenters dictionary with ranges and statistics
        """
        for dc_name, dc_info in self.datacenters.items():
            if not dc_info.tokens:
                continue
            
            # Analyze this datacenter
            analyzer = TokenAnalyzer(dc_info.tokens)
            dc_info.ranges = analyzer.calculate_ranges()
            dc_info.ranges = analyzer.detect_gaps(dc_info.ranges)
            dc_info.stats = analyzer.calculate_statistics(dc_info.ranges)
            dc_info.stats['datacenter'] = dc_name
        
        return self.datacenters


class MultiDCVisualizer:
    """Visualizes multiple datacenters in a single view."""
    
    TOKEN_SPACE = 2**64
    MIN_TOKEN = -(2**63)
    MAX_TOKEN = 2**63 - 1
    
    def __init__(self):
        self.color_map = {}
    
    def token_to_angle(self, token_value: int) -> float:
        """Convert token value to angle in degrees (0-360)."""
        normalized = (token_value - self.MIN_TOKEN) / (self.MAX_TOKEN - self.MIN_TOKEN)
        return normalized * 360
    
    def create_multi_dc_plot(self, datacenters: Dict[str, DatacenterInfo], 
                            figsize: Tuple[int, int] = (16, 12)) -> plt.Figure:
        """
        Create visualization showing all datacenters.
        
        Args:
            datacenters: Dictionary of datacenter information
            figsize: Figure size in inches
            
        Returns:
            matplotlib Figure object
        """
        n_dcs = len(datacenters)
        
        # Create subplots - one for each datacenter
        if n_dcs == 1:
            fig, axes = plt.subplots(1, 1, figsize=figsize, subplot_kw=dict(projection='polar'))
            axes = [axes]
        elif n_dcs == 2:
            fig, axes = plt.subplots(1, 2, figsize=figsize, subplot_kw=dict(projection='polar'))
        elif n_dcs <= 4:
            fig, axes = plt.subplots(2, 2, figsize=figsize, subplot_kw=dict(projection='polar'))
            axes = axes.flatten()
        else:
            rows = (n_dcs + 2) // 3
            fig, axes = plt.subplots(rows, 3, figsize=figsize, subplot_kw=dict(projection='polar'))
            axes = axes.flatten()
        
        # Assign colors globally across all DCs
        all_nodes = set()
        for dc_info in datacenters.values():
            for token in dc_info.tokens:
                all_nodes.add(token.address)
        
        self._assign_colors(sorted(all_nodes))
        
        # Plot each datacenter
        for idx, (dc_name, dc_info) in enumerate(datacenters.items()):
            if idx >= len(axes):
                break
            
            ax = axes[idx]
            self._plot_single_dc(ax, dc_info)
        
        # Hide unused subplots
        for idx in range(len(datacenters), len(axes)):
            axes[idx].set_visible(False)
        
        fig.suptitle('Multi-Datacenter Token Ring Distribution', 
                    fontsize=18, fontweight='bold', y=0.98)
        
        # Add global legend
        self._add_global_legend(fig, datacenters)
        
        plt.tight_layout()
        return fig
    
    def _assign_colors(self, nodes: List[str]):
        """Assign distinct colors to nodes."""
        n_nodes = len(nodes)
        
        if n_nodes <= 10:
            colors = plt.cm.tab10.colors
        elif n_nodes <= 20:
            colors = plt.cm.tab20.colors
        else:
            colors = plt.cm.hsv(np.linspace(0, 1, n_nodes))
        
        for i, node in enumerate(nodes):
            self.color_map[node] = colors[i % len(colors)]
        
        self.color_map['GAP'] = (1.0, 1.0, 1.0)
    
    def _plot_single_dc(self, ax, dc_info: DatacenterInfo):
        """Plot a single datacenter on the given axis."""
        ax.set_theta_zero_location('N')
        ax.set_theta_direction(-1)
        ax.set_ylim(0, 1)
        ax.set_yticks([])
        ax.set_xticks([])
        ax.spines['polar'].set_visible(False)
        
        # Draw ranges
        for range_obj in dc_info.ranges:
            start_angle = self.token_to_angle(range_obj.start_token)
            end_angle = self.token_to_angle(range_obj.end_token)
            
            if end_angle < start_angle:
                end_angle += 360
            
            start_rad = np.deg2rad(start_angle)
            end_rad = np.deg2rad(end_angle)
            
            if range_obj.is_gap:
                color = self.color_map['GAP']
                alpha = 0.5
                edgecolor = 'red'
                linewidth = 2
            else:
                color = self.color_map[range_obj.owner]
                alpha = 0.8
                edgecolor = 'black'
                linewidth = 0.5
            
            theta = np.linspace(start_rad, end_rad, 100)
            r_inner = 0.6
            r_outer = 1.0
            
            ax.fill_between(theta, r_inner, r_outer, 
                          color=color, alpha=alpha,
                          edgecolor=edgecolor, linewidth=linewidth)
        
        # Add title with stats
        stats = dc_info.stats
        title = f"{dc_info.name}\n{len(stats.get('nodes', {}))} nodes, {stats.get('total_tokens', 0)} tokens"
        ax.set_title(title, fontsize=12, fontweight='bold', pad=15)
    
    def _add_global_legend(self, fig, datacenters: Dict[str, DatacenterInfo]):
        """Add a global legend for all nodes."""
        legend_elements = []
        
        # Collect all unique nodes
        all_nodes = {}
        for dc_info in datacenters.values():
            for node, node_stats in dc_info.stats.get('nodes', {}).items():
                if node not in all_nodes:
                    all_nodes[node] = {
                        'token_count': 0,
                        'load': node_stats['load'],
                        'dcs': []
                    }
                all_nodes[node]['token_count'] += node_stats['token_count']
                all_nodes[node]['dcs'].append(dc_info.name)
        
        # Create legend entries
        for node in sorted(all_nodes.keys()):
            node_info = all_nodes[node]
            color = self.color_map[node]
            dcs_str = ', '.join(node_info['dcs'])
            label = f"{node} ({dcs_str}): {node_info['token_count']} tokens"
            legend_elements.append(
                mpatches.Patch(facecolor=color, edgecolor='black', label=label)
            )
        
        fig.legend(handles=legend_elements, loc='center left',
                  bbox_to_anchor=(1.0, 0.5), fontsize=9, frameon=True)
    
    def create_comparison_plot(self, datacenters: Dict[str, DatacenterInfo],
                              figsize: Tuple[int, int] = (14, 10)) -> plt.Figure:
        """
        Create a comparison plot showing balance metrics across datacenters.
        
        Args:
            datacenters: Dictionary of datacenter information
            figsize: Figure size in inches
            
        Returns:
            matplotlib Figure object
        """
        fig, axes = plt.subplots(2, 2, figsize=figsize)
        
        dc_names = list(datacenters.keys())
        
        # 1. Token distribution per DC
        ax1 = axes[0, 0]
        token_counts = [len(dc.tokens) for dc in datacenters.values()]
        ax1.bar(dc_names, token_counts, color='steelblue', alpha=0.7)
        ax1.set_title('Token Count per Datacenter', fontweight='bold')
        ax1.set_ylabel('Number of Tokens')
        ax1.grid(axis='y', alpha=0.3)
        
        # 2. Balance scores
        ax2 = axes[0, 1]
        balance_scores = [dc.stats.get('balance_score', 0) for dc in datacenters.values()]
        colors = ['green' if s > 0.9 else 'orange' if s > 0.7 else 'red' for s in balance_scores]
        ax2.bar(dc_names, balance_scores, color=colors, alpha=0.7)
        ax2.set_title('Balance Score per Datacenter', fontweight='bold')
        ax2.set_ylabel('Balance Score (1.0 = perfect)')
        ax2.set_ylim(0, 1.1)
        ax2.axhline(y=0.9, color='green', linestyle='--', alpha=0.5, label='Good (>0.9)')
        ax2.axhline(y=0.7, color='orange', linestyle='--', alpha=0.5, label='Fair (>0.7)')
        ax2.legend(fontsize=8)
        ax2.grid(axis='y', alpha=0.3)
        
        # 3. Gap percentage
        ax3 = axes[1, 0]
        gap_percentages = [dc.stats.get('gap_percentage', 0) for dc in datacenters.values()]
        ax3.bar(dc_names, gap_percentages, color='coral', alpha=0.7)
        ax3.set_title('Gap Percentage per Datacenter', fontweight='bold')
        ax3.set_ylabel('Gap Percentage (%)')
        ax3.grid(axis='y', alpha=0.3)
        
        # 4. Node count per DC
        ax4 = axes[1, 1]
        node_counts = [len(dc.stats.get('nodes', {})) for dc in datacenters.values()]
        ax4.bar(dc_names, node_counts, color='mediumseagreen', alpha=0.7)
        ax4.set_title('Node Count per Datacenter', fontweight='bold')
        ax4.set_ylabel('Number of Nodes')
        ax4.grid(axis='y', alpha=0.3)
        
        fig.suptitle('Multi-Datacenter Comparison', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        return fig


def print_multi_dc_statistics(datacenters: Dict[str, DatacenterInfo]):
    """Print comprehensive statistics for all datacenters."""
    print("\n" + "="*70)
    print("MULTI-DATACENTER RING ANALYSIS")
    print("="*70)
    print(f"Total Datacenters: {len(datacenters)}\n")
    
    for dc_name, dc_info in datacenters.items():
        stats = dc_info.stats
        print(f"\n{'─'*70}")
        print(f"Datacenter: {dc_name}")
        print(f"{'─'*70}")
        print(f"Total Nodes: {len(stats.get('nodes', {}))}")
        print(f"Total Tokens: {stats.get('total_tokens', 0)}")
        
        print("\nNode Distribution:")
        for node in sorted(stats.get('nodes', {}).keys()):
            node_stats = stats['nodes'][node]
            print(f"  {node}: {node_stats['token_count']} tokens "
                  f"({node_stats['coverage_percentage']:.2f}%) - Load: {node_stats['load']}")
        
        print(f"\nToken Space Coverage:")
        total_coverage = sum(n['coverage_percentage'] for n in stats.get('nodes', {}).values())
        print(f"  Owned: {total_coverage:.2f}%")
        print(f"  Gaps: {stats.get('gap_percentage', 0):.2f}% "
              f"({stats.get('gap_count', 0)} gaps detected)")
        
        print(f"\nBalance Score: {stats.get('balance_score', 0):.3f} (1.0 = perfect balance)")
    
    print("\n" + "="*70 + "\n")


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Analyze and visualize multi-datacenter Cassandra token rings'
    )
    parser.add_argument('ring_file', help='Path to nodetool ring output file')
    parser.add_argument('-o', '--output', default='multi_dc_visualization.png',
                       help='Output image file (default: multi_dc_visualization.png)')
    parser.add_argument('--comparison', action='store_true',
                       help='Generate comparison plot')
    parser.add_argument('--dpi', type=int, default=300,
                       help='Image resolution (default: 300)')
    parser.add_argument('--show', action='store_true',
                       help='Display plot interactively')
    
    args = parser.parse_args()
    
    # Parse and analyze
    parser = MultiDCRingParser(args.ring_file)
    datacenters = parser.parse_file()
    datacenters = parser.analyze_all_datacenters()
    
    # Print statistics
    print_multi_dc_statistics(datacenters)
    
    # Generate visualization
    visualizer = MultiDCVisualizer()
    
    if args.comparison:
        fig = visualizer.create_comparison_plot(datacenters)
        output_file = args.output.replace('.png', '_comparison.png')
    else:
        fig = visualizer.create_multi_dc_plot(datacenters)
        output_file = args.output
    
    if args.show:
        plt.show()
    
    fig.savefig(output_file, dpi=args.dpi, bbox_inches='tight', facecolor='white')
    print(f"Visualization saved to: {output_file}")

# Made with Bob
