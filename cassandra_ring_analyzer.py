#!/usr/bin/env python3
"""
Cassandra Ring Analyzer
Analyzes and visualizes Cassandra token ring distribution with gap detection.
"""

import argparse
import sys
import re
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np


@dataclass
class TokenEntry:
    """Represents a single token entry from the ring file."""
    address: str
    rack: str
    status: str
    state: str
    load: str
    owns: str
    token: int


@dataclass
class TokenRange:
    """Represents a token range in the ring."""
    start_token: int
    end_token: int
    owner: str
    size: int
    is_gap: bool = False


class RingParser:
    """Parses Cassandra nodetool ring output files."""
    
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.datacenter = None
    
    def parse_file(self) -> List[TokenEntry]:
        """Parse the ring file and return list of token entries."""
        tokens = []
        
        try:
            with open(self.filepath, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            print(f"Error: File '{self.filepath}' not found.")
            sys.exit(1)
        except Exception as e:
            print(f"Error reading file: {e}")
            sys.exit(1)
        
        # Find datacenter
        for line in lines:
            if line.strip().startswith('Datacenter:'):
                self.datacenter = line.split(':')[1].strip()
                break
        
        # Find where data starts (after "Address" header line)
        data_start = 0
        for i, line in enumerate(lines):
            if line.strip().startswith('Address'):
                data_start = i + 1
                break
        
        if data_start == 0:
            print("Error: Could not find 'Address' header in ring file.")
            sys.exit(1)
        
        # Parse token lines
        for line_num, line in enumerate(lines[data_start:], start=data_start + 1):
            line = line.strip()
            if not line:
                continue
            
            # Split by whitespace
            parts = line.split()
            
            # Handle orphaned token (line with only token, no address)
            if len(parts) == 1:
                try:
                    token_value = int(parts[0])
                    # Skip orphaned tokens - they don't have an owner
                    continue
                except ValueError:
                    continue
            
            # Normal token line should have at least 7 parts
            if len(parts) >= 7:
                try:
                    # Handle load which might be "1.46 TiB" (2 parts)
                    address = parts[0]
                    rack = parts[1]
                    status = parts[2]
                    state = parts[3]
                    
                    # Load is parts[4] and parts[5] combined
                    load = f"{parts[4]} {parts[5]}"
                    owns = parts[6]
                    
                    # Token is the last part
                    if len(parts) >= 8:
                        token = int(parts[7])
                    else:
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
                    tokens.append(token_entry)
                    
                except (ValueError, IndexError) as e:
                    print(f"Warning: Skipping malformed line {line_num}: {line}")
                    continue
        
        if not tokens:
            print("Error: No valid tokens found in ring file.")
            sys.exit(1)
        
        print(f"Parsed {len(tokens)} tokens from datacenter '{self.datacenter}'")
        return tokens


class TokenAnalyzer:
    """Analyzes token distribution and detects gaps."""
    
    TOKEN_SPACE = 2**64  # Total Cassandra token space
    MIN_TOKEN = -(2**63)
    MAX_TOKEN = 2**63 - 1
    
    def __init__(self, tokens: List[TokenEntry]):
        self.tokens = tokens
        self.sorted_tokens = sorted(tokens, key=lambda t: t.token)
    
    def calculate_ranges(self) -> List[TokenRange]:
        """Calculate token ranges between consecutive tokens."""
        ranges = []
        n = len(self.sorted_tokens)
        
        for i in range(n):
            current = self.sorted_tokens[i]
            next_idx = (i + 1) % n  # Wrap around for last token
            next_token = self.sorted_tokens[next_idx]
            
            # Calculate range size
            size = self._calculate_range_size(current.token, next_token.token)
            
            token_range = TokenRange(
                start_token=current.token,
                end_token=next_token.token,
                owner=next_token.address,  # Range is owned by the endpoint
                size=size,
                is_gap=False
            )
            ranges.append(token_range)
        
        return ranges
    
    def _calculate_range_size(self, start: int, end: int) -> int:
        """Calculate range size handling wrap-around."""
        if end >= start:
            return end - start
        else:
            # Wrap-around case
            return (self.TOKEN_SPACE + end - start) % self.TOKEN_SPACE
    
    def detect_gaps(self, ranges: List[TokenRange]) -> List[TokenRange]:
        """
        Detect gaps in the token ring.
        A gap is identified when consecutive tokens from the same node
        create an unusually large range.
        """
        # Group tokens by owner
        tokens_by_owner = {}
        for token in self.tokens:
            if token.address not in tokens_by_owner:
                tokens_by_owner[token.address] = []
            tokens_by_owner[token.address].append(token.token)
        
        # Calculate average range size for reference
        avg_range_size = self.TOKEN_SPACE / len(self.tokens)
        gap_threshold = avg_range_size * 2  # Ranges 2x larger than average might be gaps
        
        # Mark potential gaps
        for range_obj in ranges:
            # Check if this range is significantly larger than average
            if range_obj.size > gap_threshold:
                # Check if there are tokens from other nodes that should be in this range
                has_intermediate_tokens = False
                for owner, owner_tokens in tokens_by_owner.items():
                    if owner == range_obj.owner:
                        continue
                    for token in owner_tokens:
                        if self._is_token_in_range(token, range_obj.start_token, range_obj.end_token):
                            has_intermediate_tokens = True
                            break
                    if has_intermediate_tokens:
                        break
                
                # If no intermediate tokens and range is large, it's likely a gap
                if not has_intermediate_tokens:
                    range_obj.is_gap = True
        
        return ranges
    
    def _is_token_in_range(self, token: int, start: int, end: int) -> bool:
        """Check if token is between start and end, handling wrap-around."""
        if end >= start:
            return start < token < end
        else:
            # Wrap-around case
            return token > start or token < end
    
    def calculate_statistics(self, ranges: List[TokenRange]) -> Dict:
        """Calculate comprehensive statistics about the ring."""
        stats = {
            'datacenter': None,
            'total_tokens': len(self.tokens),
            'total_ranges': len(ranges),
            'nodes': {},
            'gap_count': 0,
            'gap_percentage': 0.0,
            'balance_score': 0.0,
            'largest_gap': 0,
            'smallest_range': float('inf'),
            'average_range': 0
        }
        
        # Per-node statistics
        for token in self.tokens:
            node = token.address
            if node not in stats['nodes']:
                stats['nodes'][node] = {
                    'token_count': 0,
                    'load': token.load,
                    'total_range_size': 0,
                    'coverage_percentage': 0.0
                }
            stats['nodes'][node]['token_count'] += 1
        
        # Range statistics
        gap_space = 0
        range_sizes = []
        
        for range_obj in ranges:
            if range_obj.is_gap:
                stats['gap_count'] += 1
                gap_space += range_obj.size
                stats['largest_gap'] = max(stats['largest_gap'], range_obj.size)
            else:
                owner = range_obj.owner
                stats['nodes'][owner]['total_range_size'] += range_obj.size
                range_sizes.append(range_obj.size)
                stats['smallest_range'] = min(stats['smallest_range'], range_obj.size)
        
        # Calculate percentages
        stats['gap_percentage'] = (gap_space / self.TOKEN_SPACE) * 100
        
        for node in stats['nodes']:
            node_space = stats['nodes'][node]['total_range_size']
            stats['nodes'][node]['coverage_percentage'] = (node_space / self.TOKEN_SPACE) * 100
        
        # Calculate average range
        if range_sizes:
            stats['average_range'] = sum(range_sizes) / len(range_sizes)
        
        # Balance score (coefficient of variation - lower is better)
        if range_sizes and len(range_sizes) > 1:
            mean_size = stats['average_range']
            variance = sum((x - mean_size)**2 for x in range_sizes) / len(range_sizes)
            std_dev = variance ** 0.5
            cv = std_dev / mean_size if mean_size > 0 else 0
            stats['balance_score'] = max(0, 1.0 - cv)  # Convert to 0-1 scale where 1 is perfect
        else:
            stats['balance_score'] = 1.0
        
        return stats


class RingVisualizer:
    """Creates circular ring visualization using matplotlib."""
    
    TOKEN_SPACE = 2**64
    MIN_TOKEN = -(2**63)
    MAX_TOKEN = 2**63 - 1
    
    def __init__(self):
        self.color_map = {}
    
    def assign_colors(self, nodes: List[str]) -> Dict[str, Tuple[float, float, float]]:
        """Assign distinct colors to each node."""
        unique_nodes = sorted(set(nodes))
        n_nodes = len(unique_nodes)
        
        # Choose colormap based on number of nodes
        if n_nodes <= 10:
            colors = plt.cm.tab10.colors
        else:
            colors = plt.cm.tab20.colors
        
        color_map = {}
        for i, node in enumerate(unique_nodes):
            color_map[node] = colors[i % len(colors)]
        
        # Reserve white for gaps
        color_map['GAP'] = (1.0, 1.0, 1.0)
        
        self.color_map = color_map
        return color_map
    
    def token_to_angle(self, token_value: int) -> float:
        """Convert token value to angle in degrees (0-360)."""
        # Normalize to 0-1
        normalized = (token_value - self.MIN_TOKEN) / (self.MAX_TOKEN - self.MIN_TOKEN)
        # Convert to degrees
        angle = normalized * 360
        return angle
    
    def create_ring_plot(self, ranges: List[TokenRange], stats: Dict, 
                        figsize: Tuple[int, int] = (14, 12)) -> plt.Figure:
        """Create circular ring plot with colored segments."""
        
        fig = plt.figure(figsize=figsize)
        ax = fig.add_subplot(111, projection='polar')
        
        # Configure polar plot
        ax.set_theta_zero_location('N')  # 0° at top
        ax.set_theta_direction(-1)  # Clockwise
        ax.set_ylim(0, 1)
        ax.set_yticks([])
        ax.set_xticks([])
        ax.spines['polar'].set_visible(False)
        
        # Draw each range as a wedge
        for range_obj in ranges:
            start_angle = self.token_to_angle(range_obj.start_token)
            end_angle = self.token_to_angle(range_obj.end_token)
            
            # Handle wrap-around
            if end_angle < start_angle:
                end_angle += 360
            
            # Convert to radians for matplotlib
            start_rad = np.deg2rad(start_angle)
            end_rad = np.deg2rad(end_angle)
            
            # Get color
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
            
            # Create wedge using theta (angle) values
            theta = np.linspace(start_rad, end_rad, 100)
            r_inner = 0.6
            r_outer = 1.0
            
            # Fill the wedge
            ax.fill_between(theta, r_inner, r_outer, 
                           color=color, alpha=alpha, 
                           edgecolor=edgecolor, linewidth=linewidth)
        
        # Add title
        title = f"Cassandra Token Ring Distribution\nDatacenter: {stats.get('datacenter', 'Unknown')}"
        ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
        
        # Add legend
        self._add_legend(fig, stats)
        
        return fig
    
    def _add_legend(self, fig: plt.Figure, stats: Dict):
        """Add legend with node information and statistics."""
        legend_elements = []
        
        # Add node entries
        for node in sorted(stats['nodes'].keys()):
            node_stats = stats['nodes'][node]
            color = self.color_map[node]
            token_count = node_stats['token_count']
            load = node_stats['load']
            coverage = node_stats['coverage_percentage']
            
            label = f"{node}: {token_count} tokens, {load} ({coverage:.1f}%)"
            legend_elements.append(
                mpatches.Patch(facecolor=color, edgecolor='black', label=label)
            )
        
        # Add gap entry if gaps exist
        if stats['gap_count'] > 0:
            label = f"Gaps: {stats['gap_count']} detected ({stats['gap_percentage']:.2f}%)"
            legend_elements.append(
                mpatches.Patch(facecolor=self.color_map['GAP'], 
                             edgecolor='red', linewidth=2, label=label)
            )
        
        # Add statistics box
        stats_text = (
            f"\nStatistics:\n"
            f"Total Tokens: {stats['total_tokens']}\n"
            f"Total Nodes: {len(stats['nodes'])}\n"
            f"Balance Score: {stats['balance_score']:.3f}\n"
            f"Avg Range: {stats['average_range']/self.TOKEN_SPACE*100:.2f}%"
        )
        legend_elements.append(
            mpatches.Patch(facecolor='none', edgecolor='none', label=stats_text)
        )
        
        fig.legend(handles=legend_elements, loc='center left', 
                  bbox_to_anchor=(1.0, 0.5), fontsize=10, frameon=True)


def print_statistics(stats: Dict):
    """Print comprehensive statistics to console."""
    print("\n" + "="*60)
    print("Cassandra Ring Analysis Summary")
    print("="*60)
    print(f"Datacenter: {stats.get('datacenter', 'Unknown')}")
    print(f"Total Nodes: {len(stats['nodes'])}")
    print(f"Total Tokens: {stats['total_tokens']}")
    
    print("\nNode Distribution:")
    for node in sorted(stats['nodes'].keys()):
        node_stats = stats['nodes'][node]
        print(f"  {node}: {node_stats['token_count']} tokens "
              f"({node_stats['coverage_percentage']:.2f}%) - Load: {node_stats['load']}")
    
    print("\nToken Space Coverage:")
    total_coverage = sum(n['coverage_percentage'] for n in stats['nodes'].values())
    print(f"  Owned: {total_coverage:.2f}%")
    print(f"  Gaps: {stats['gap_percentage']:.2f}% ({stats['gap_count']} gaps detected)")
    
    if stats['gap_count'] > 0:
        print(f"\nLargest Gap: {stats['largest_gap']/TokenAnalyzer.TOKEN_SPACE*100:.2f}% of token space")
    
    print(f"Smallest Range: {stats['smallest_range']/TokenAnalyzer.TOKEN_SPACE*100:.2f}% of token space")
    print(f"Average Range: {stats['average_range']/TokenAnalyzer.TOKEN_SPACE*100:.2f}% of token space")
    print(f"\nBalance Score: {stats['balance_score']:.3f} (1.0 = perfect balance)")
    print("="*60 + "\n")


def parse_size(size_str: str) -> Tuple[int, int]:
    """Parse size string like '12x10' into tuple (12, 10)."""
    try:
        parts = size_str.lower().split('x')
        if len(parts) == 2:
            return (int(parts[0]), int(parts[1]))
    except:
        pass
    return (12, 10)  # Default


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Analyze and visualize Cassandra token ring distribution',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('ring_file', help='Path to nodetool ring output file')
    parser.add_argument('-o', '--output', default='ring_visualization.png',
                       help='Output image file (default: ring_visualization.png)')
    parser.add_argument('--format', choices=['png', 'pdf', 'svg'], default='png',
                       help='Output format (default: png)')
    parser.add_argument('--dpi', type=int, default=300,
                       help='Image resolution (default: 300)')
    parser.add_argument('--size', default='14x12',
                       help='Figure size in inches WxH (default: 14x12)')
    parser.add_argument('--show', action='store_true',
                       help='Display plot interactively')
    parser.add_argument('--stats-only', action='store_true',
                       help='Print statistics without visualization')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    # Parse ring file
    if args.verbose:
        print(f"Parsing ring file: {args.ring_file}")
    
    ring_parser = RingParser(args.ring_file)
    tokens = ring_parser.parse_file()
    
    # Calculate ranges
    if args.verbose:
        print("Calculating token ranges...")
    
    analyzer = TokenAnalyzer(tokens)
    ranges = analyzer.calculate_ranges()
    
    # Detect gaps
    if args.verbose:
        print("Detecting gaps...")
    
    ranges = analyzer.detect_gaps(ranges)
    
    # Calculate statistics
    stats = analyzer.calculate_statistics(ranges)
    stats['datacenter'] = ring_parser.datacenter
    
    # Print statistics
    print_statistics(stats)
    
    # Generate visualization if requested
    if not args.stats_only:
        if args.verbose:
            print("Generating visualization...")
        
        visualizer = RingVisualizer()
        visualizer.assign_colors([t.address for t in tokens])
        
        figsize = parse_size(args.size)
        fig = visualizer.create_ring_plot(ranges, stats, figsize=figsize)
        
        # Save or show
        if args.show:
            plt.show()
        
        # Always save unless only showing
        output_file = args.output
        if not output_file.endswith(f'.{args.format}'):
            output_file = f"{output_file.rsplit('.', 1)[0]}.{args.format}"
        
        fig.savefig(output_file, format=args.format, dpi=args.dpi, 
                   bbox_inches='tight', facecolor='white')
        print(f"Visualization saved to: {output_file}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())

# Made with Bob
