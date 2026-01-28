#!/usr/bin/env python3
"""
Interactive Ring Visualizer using Plotly
Creates interactive HTML visualizations with hover tooltips and zoom capabilities.
"""

from typing import List, Dict, Tuple, Optional
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
from cassandra_ring_analyzer import TokenEntry, TokenRange, TokenAnalyzer


class InteractiveRingVisualizer:
    """Creates interactive visualizations using Plotly."""
    
    TOKEN_SPACE = 2**64
    MIN_TOKEN = -(2**63)
    MAX_TOKEN = 2**63 - 1
    
    def __init__(self):
        self.color_map = {}
        self.color_palette = px.colors.qualitative.Set3
    
    def assign_colors(self, nodes: List[str]) -> Dict[str, str]:
        """Assign distinct colors to each node."""
        unique_nodes = sorted(set(nodes))
        
        for i, node in enumerate(unique_nodes):
            self.color_map[node] = self.color_palette[i % len(self.color_palette)]
        
        self.color_map['GAP'] = 'rgba(255, 255, 255, 0.5)'
        
        return self.color_map
    
    def token_to_angle(self, token_value: int) -> float:
        """Convert token value to angle in degrees (0-360)."""
        normalized = (token_value - self.MIN_TOKEN) / (self.MAX_TOKEN - self.MIN_TOKEN)
        return normalized * 360
    
    def create_interactive_ring(self, ranges: List[TokenRange], stats: Dict,
                               title: str = "Cassandra Token Ring") -> go.Figure:
        """
        Create an interactive ring visualization with hover information.
        
        Args:
            ranges: List of token ranges
            stats: Statistics dictionary
            title: Plot title
            
        Returns:
            Plotly Figure object
        """
        fig = go.Figure()
        
        # Assign colors
        nodes = [r.owner for r in ranges if not r.is_gap]
        self.assign_colors(nodes)
        
        # Create wedges for each range
        for range_obj in ranges:
            start_angle = self.token_to_angle(range_obj.start_token)
            end_angle = self.token_to_angle(range_obj.end_token)
            
            if end_angle < start_angle:
                end_angle += 360
            
            # Create arc points
            theta = np.linspace(np.deg2rad(start_angle), np.deg2rad(end_angle), 50)
            r_inner = 0.6
            r_outer = 1.0
            
            # Convert to cartesian coordinates
            x_inner = r_inner * np.cos(theta)
            y_inner = r_inner * np.sin(theta)
            x_outer = r_outer * np.cos(theta)
            y_outer = r_outer * np.sin(theta)
            
            # Create closed path for the wedge
            x = np.concatenate([x_inner, x_outer[::-1], [x_inner[0]]])
            y = np.concatenate([y_inner, y_outer[::-1], [y_inner[0]]])
            
            # Determine color and hover text
            if range_obj.is_gap:
                color = self.color_map['GAP']
                hover_text = (
                    f"<b>GAP DETECTED</b><br>"
                    f"Start Token: {range_obj.start_token:,}<br>"
                    f"End Token: {range_obj.end_token:,}<br>"
                    f"Size: {range_obj.size:,}<br>"
                    f"Percentage: {(range_obj.size/self.TOKEN_SPACE)*100:.3f}%"
                )
                name = "Gap"
                showlegend = True
            else:
                color = self.color_map[range_obj.owner]
                node_stats = stats['nodes'].get(range_obj.owner, {})
                hover_text = (
                    f"<b>Node: {range_obj.owner}</b><br>"
                    f"Start Token: {range_obj.start_token:,}<br>"
                    f"End Token: {range_obj.end_token:,}<br>"
                    f"Range Size: {range_obj.size:,}<br>"
                    f"Percentage: {(range_obj.size/self.TOKEN_SPACE)*100:.3f}%<br>"
                    f"Load: {node_stats.get('load', 'N/A')}<br>"
                    f"Total Tokens: {node_stats.get('token_count', 0)}"
                )
                name = range_obj.owner
                showlegend = range_obj.owner not in [trace.name for trace in fig.data]
            
            fig.add_trace(go.Scatter(
                x=x, y=y,
                fill='toself',
                fillcolor=color,
                line=dict(color='black' if not range_obj.is_gap else 'red', width=1),
                hovertext=hover_text,
                hoverinfo='text',
                name=name,
                showlegend=showlegend,
                mode='lines'
            ))
        
        # Add center circle
        theta_circle = np.linspace(0, 2*np.pi, 100)
        x_circle = 0.5 * np.cos(theta_circle)
        y_circle = 0.5 * np.sin(theta_circle)
        
        fig.add_trace(go.Scatter(
            x=x_circle, y=y_circle,
            fill='toself',
            fillcolor='white',
            line=dict(color='gray', width=2),
            hoverinfo='skip',
            showlegend=False,
            mode='lines'
        ))
        
        # Add title and statistics in center
        stats_text = (
            f"<b>{title}</b><br>"
            f"Datacenter: {stats.get('datacenter', 'Unknown')}<br>"
            f"Nodes: {len(stats.get('nodes', {}))}<br>"
            f"Tokens: {stats.get('total_tokens', 0)}<br>"
            f"Balance: {stats.get('balance_score', 0):.3f}"
        )
        
        fig.add_annotation(
            x=0, y=0,
            text=stats_text,
            showarrow=False,
            font=dict(size=12, color='black'),
            align='center'
        )
        
        # Update layout
        fig.update_layout(
            title=dict(
                text=f"<b>{title}</b>",
                x=0.5,
                xanchor='center',
                font=dict(size=20)
            ),
            xaxis=dict(
                showgrid=False,
                showticklabels=False,
                zeroline=False,
                range=[-1.2, 1.2]
            ),
            yaxis=dict(
                showgrid=False,
                showticklabels=False,
                zeroline=False,
                range=[-1.2, 1.2],
                scaleanchor='x',
                scaleratio=1
            ),
            plot_bgcolor='white',
            hovermode='closest',
            width=900,
            height=900,
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=1.01,
                bgcolor="rgba(255, 255, 255, 0.8)",
                bordercolor="black",
                borderwidth=1
            )
        )
        
        return fig
    
    def create_statistics_dashboard(self, stats: Dict) -> go.Figure:
        """
        Create an interactive dashboard with multiple statistics views.
        
        Args:
            stats: Statistics dictionary
            
        Returns:
            Plotly Figure with subplots
        """
        # Prepare data
        nodes = list(stats.get('nodes', {}).keys())
        token_counts = [stats['nodes'][n]['token_count'] for n in nodes]
        coverage_pcts = [stats['nodes'][n]['coverage_percentage'] for n in nodes]
        loads = [stats['nodes'][n]['load'] for n in nodes]
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Token Distribution by Node',
                'Coverage Percentage by Node',
                'Balance Metrics',
                'Node Loads'
            ),
            specs=[
                [{'type': 'bar'}, {'type': 'bar'}],
                [{'type': 'indicator'}, {'type': 'bar'}]
            ]
        )
        
        # 1. Token distribution
        fig.add_trace(
            go.Bar(
                x=nodes,
                y=token_counts,
                name='Token Count',
                marker_color='steelblue',
                hovertemplate='<b>%{x}</b><br>Tokens: %{y}<extra></extra>'
            ),
            row=1, col=1
        )
        
        # 2. Coverage percentage
        fig.add_trace(
            go.Bar(
                x=nodes,
                y=coverage_pcts,
                name='Coverage %',
                marker_color='mediumseagreen',
                hovertemplate='<b>%{x}</b><br>Coverage: %{y:.2f}%<extra></extra>'
            ),
            row=1, col=2
        )
        
        # 3. Balance score indicator
        balance_score = stats.get('balance_score', 0)
        fig.add_trace(
            go.Indicator(
                mode="gauge+number+delta",
                value=balance_score,
                title={'text': "Balance Score"},
                delta={'reference': 1.0},
                gauge={
                    'axis': {'range': [0, 1]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 0.7], 'color': "lightcoral"},
                        {'range': [0.7, 0.9], 'color': "lightyellow"},
                        {'range': [0.9, 1], 'color': "lightgreen"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 0.9
                    }
                }
            ),
            row=2, col=1
        )
        
        # 4. Node loads (parse load values)
        load_values = []
        for load in loads:
            try:
                # Extract numeric value from load string (e.g., "1.26 TiB")
                parts = load.split()
                if len(parts) >= 2:
                    value = float(parts[0])
                    unit = parts[1].upper()
                    # Convert to GB for comparison
                    if 'TIB' in unit or 'TB' in unit:
                        value *= 1024
                    elif 'MIB' in unit or 'MB' in unit:
                        value /= 1024
                    load_values.append(value)
                else:
                    load_values.append(0)
            except:
                load_values.append(0)
        
        fig.add_trace(
            go.Bar(
                x=nodes,
                y=load_values,
                name='Load (GB)',
                marker_color='coral',
                hovertemplate='<b>%{x}</b><br>Load: %{y:.2f} GB<extra></extra>'
            ),
            row=2, col=2
        )
        
        # Update layout
        fig.update_layout(
            title_text=f"<b>Ring Statistics Dashboard - {stats.get('datacenter', 'Unknown')}</b>",
            title_x=0.5,
            title_font_size=20,
            showlegend=False,
            height=800,
            width=1200
        )
        
        fig.update_xaxes(title_text="Node", row=1, col=1)
        fig.update_xaxes(title_text="Node", row=1, col=2)
        fig.update_xaxes(title_text="Node", row=2, col=2)
        
        fig.update_yaxes(title_text="Token Count", row=1, col=1)
        fig.update_yaxes(title_text="Coverage %", row=1, col=2)
        fig.update_yaxes(title_text="Load (GB)", row=2, col=2)
        
        return fig
    
    def save_html(self, fig: go.Figure, filepath: str):
        """Save figure as interactive HTML file."""
        fig.write_html(filepath, include_plotlyjs='cdn')
        print(f"Interactive visualization saved to: {filepath}")


if __name__ == '__main__':
    import argparse
    from cassandra_ring_analyzer import RingParser
    
    parser = argparse.ArgumentParser(
        description='Create interactive Cassandra ring visualization'
    )
    parser.add_argument('ring_file', help='Path to nodetool ring output file')
    parser.add_argument('-o', '--output', default='ring_interactive.html',
                       help='Output HTML file (default: ring_interactive.html)')
    parser.add_argument('--dashboard', action='store_true',
                       help='Generate statistics dashboard instead of ring')
    parser.add_argument('--show', action='store_true',
                       help='Open in browser automatically')
    
    args = parser.parse_args()
    
    # Parse ring file
    ring_parser = RingParser(args.ring_file)
    tokens = ring_parser.parse_file()
    
    # Analyze
    analyzer = TokenAnalyzer(tokens)
    ranges = analyzer.calculate_ranges()
    ranges = analyzer.detect_gaps(ranges)
    stats = analyzer.calculate_statistics(ranges)
    stats['datacenter'] = ring_parser.datacenter
    
    # Create visualization
    visualizer = InteractiveRingVisualizer()
    
    if args.dashboard:
        fig = visualizer.create_statistics_dashboard(stats)
        output_file = args.output.replace('.html', '_dashboard.html')
    else:
        fig = visualizer.create_interactive_ring(ranges, stats)
        output_file = args.output
    
    # Save
    visualizer.save_html(fig, output_file)
    
    # Show in browser if requested
    if args.show:
        fig.show()

# Made with Bob
