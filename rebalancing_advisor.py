#!/usr/bin/env python3
"""
Rebalancing Advisor
Analyzes ring imbalances and provides recommendations for token rebalancing.
"""

from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
import numpy as np
from cassandra_ring_analyzer import TokenEntry, TokenRange, RingParser, TokenAnalyzer


@dataclass
class RebalancingRecommendation:
    """Represents a recommendation for rebalancing."""
    node: str
    current_tokens: int
    recommended_tokens: int
    change: int
    priority: str  # 'high', 'medium', 'low'
    reason: str


@dataclass
class TokenMovement:
    """Represents a suggested token movement."""
    token_value: int
    from_node: str
    to_node: str
    impact_score: float


class RebalancingAdvisor:
    """Analyzes ring balance and provides rebalancing recommendations."""
    
    TOKEN_SPACE = 2**64
    
    def __init__(self, tokens: List[TokenEntry], ranges: List[TokenRange], stats: Dict):
        self.tokens = tokens
        self.ranges = ranges
        self.stats = stats
        self.nodes = list(stats['nodes'].keys())
        self.n_nodes = len(self.nodes)
        self.total_tokens = len(tokens)
    
    def analyze_balance(self) -> Dict:
        """
        Perform comprehensive balance analysis.
        
        Returns:
            Dictionary containing balance analysis results
        """
        analysis = {
            'balance_score': self.stats['balance_score'],
            'is_balanced': self.stats['balance_score'] >= 0.9,
            'imbalance_severity': self._calculate_imbalance_severity(),
            'node_analysis': {},
            'recommendations': []
        }
        
        # Ideal tokens per node
        ideal_tokens_per_node = self.total_tokens / self.n_nodes
        
        # Analyze each node
        for node in self.nodes:
            node_stats = self.stats['nodes'][node]
            current_tokens = node_stats['token_count']
            deviation = current_tokens - ideal_tokens_per_node
            deviation_pct = (deviation / ideal_tokens_per_node) * 100
            
            analysis['node_analysis'][node] = {
                'current_tokens': current_tokens,
                'ideal_tokens': ideal_tokens_per_node,
                'deviation': deviation,
                'deviation_percentage': deviation_pct,
                'status': self._classify_node_balance(deviation_pct)
            }
        
        return analysis
    
    def _calculate_imbalance_severity(self) -> str:
        """Calculate overall imbalance severity."""
        balance_score = self.stats['balance_score']
        
        if balance_score >= 0.95:
            return 'excellent'
        elif balance_score >= 0.9:
            return 'good'
        elif balance_score >= 0.8:
            return 'fair'
        elif balance_score >= 0.7:
            return 'poor'
        else:
            return 'critical'
    
    def _classify_node_balance(self, deviation_pct: float) -> str:
        """Classify node balance status based on deviation percentage."""
        abs_dev = abs(deviation_pct)
        
        if abs_dev <= 5:
            return 'balanced'
        elif abs_dev <= 10:
            return 'slightly_imbalanced'
        elif abs_dev <= 20:
            return 'imbalanced'
        else:
            return 'severely_imbalanced'
    
    def generate_recommendations(self) -> List[RebalancingRecommendation]:
        """
        Generate rebalancing recommendations.
        
        Returns:
            List of RebalancingRecommendation objects
        """
        recommendations = []
        analysis = self.analyze_balance()
        
        if analysis['is_balanced']:
            return recommendations
        
        ideal_tokens = self.total_tokens / self.n_nodes
        
        for node, node_analysis in analysis['node_analysis'].items():
            deviation = node_analysis['deviation']
            deviation_pct = node_analysis['deviation_percentage']
            status = node_analysis['status']
            
            if status == 'balanced':
                continue
            
            # Determine priority
            if abs(deviation_pct) > 20:
                priority = 'high'
            elif abs(deviation_pct) > 10:
                priority = 'medium'
            else:
                priority = 'low'
            
            # Generate reason
            if deviation > 0:
                reason = f"Node has {abs(deviation):.1f} more tokens than ideal ({abs(deviation_pct):.1f}% over). Consider removing tokens."
            else:
                reason = f"Node has {abs(deviation):.1f} fewer tokens than ideal ({abs(deviation_pct):.1f}% under). Consider adding tokens."
            
            recommendation = RebalancingRecommendation(
                node=node,
                current_tokens=node_analysis['current_tokens'],
                recommended_tokens=int(round(ideal_tokens)),
                change=int(round(ideal_tokens - node_analysis['current_tokens'])),
                priority=priority,
                reason=reason
            )
            
            recommendations.append(recommendation)
        
        # Sort by priority
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        recommendations.sort(key=lambda r: (priority_order[r.priority], abs(r.change)), reverse=True)
        
        return recommendations
    
    def suggest_token_movements(self, max_movements: int = 10) -> List[TokenMovement]:
        """
        Suggest specific token movements to improve balance.
        
        Args:
            max_movements: Maximum number of movements to suggest
            
        Returns:
            List of TokenMovement objects
        """
        movements = []
        analysis = self.analyze_balance()
        
        # Identify over-allocated and under-allocated nodes
        over_allocated = []
        under_allocated = []
        
        for node, node_analysis in analysis['node_analysis'].items():
            if node_analysis['deviation'] > 0:
                over_allocated.append((node, node_analysis['deviation']))
            elif node_analysis['deviation'] < 0:
                under_allocated.append((node, abs(node_analysis['deviation'])))
        
        # Sort by deviation magnitude
        over_allocated.sort(key=lambda x: x[1], reverse=True)
        under_allocated.sort(key=lambda x: x[1], reverse=True)
        
        # Generate movement suggestions
        for from_node, _ in over_allocated:
            # Get tokens owned by this node
            node_tokens = sorted([t.token for t in self.tokens if t.address == from_node])
            
            for to_node, _ in under_allocated:
                if len(movements) >= max_movements:
                    break
                
                # Suggest moving a token
                if node_tokens:
                    token_to_move = node_tokens[len(node_tokens) // 2]  # Pick middle token
                    
                    # Calculate impact score (higher is better)
                    impact_score = self._calculate_movement_impact(token_to_move, from_node, to_node)
                    
                    movement = TokenMovement(
                        token_value=token_to_move,
                        from_node=from_node,
                        to_node=to_node,
                        impact_score=impact_score
                    )
                    
                    movements.append(movement)
                    node_tokens.remove(token_to_move)
            
            if len(movements) >= max_movements:
                break
        
        # Sort by impact score
        movements.sort(key=lambda m: m.impact_score, reverse=True)
        
        return movements[:max_movements]
    
    def _calculate_movement_impact(self, token: int, from_node: str, to_node: str) -> float:
        """
        Calculate the impact score of moving a token.
        Higher score means better balance improvement.
        """
        # Find the range this token belongs to
        token_range = None
        for r in self.ranges:
            if r.start_token <= token < r.end_token or (r.end_token < r.start_token and (token >= r.start_token or token < r.end_token)):
                token_range = r
                break
        
        if not token_range:
            return 0.0
        
        # Impact is based on range size and current imbalance
        range_size_factor = token_range.size / self.TOKEN_SPACE
        
        from_node_tokens = self.stats['nodes'][from_node]['token_count']
        to_node_tokens = self.stats['nodes'][to_node]['token_count']
        ideal_tokens = self.total_tokens / self.n_nodes
        
        # Calculate how much this movement improves balance
        from_deviation_before = abs(from_node_tokens - ideal_tokens)
        from_deviation_after = abs((from_node_tokens - 1) - ideal_tokens)
        
        to_deviation_before = abs(to_node_tokens - ideal_tokens)
        to_deviation_after = abs((to_node_tokens + 1) - ideal_tokens)
        
        improvement = (from_deviation_before + to_deviation_before) - (from_deviation_after + to_deviation_after)
        
        # Combine factors
        impact_score = improvement * (1 + range_size_factor * 10)
        
        return impact_score
    
    def estimate_rebalancing_cost(self, movements: List[TokenMovement]) -> Dict:
        """
        Estimate the cost and impact of proposed token movements.
        
        Args:
            movements: List of proposed token movements
            
        Returns:
            Dictionary containing cost estimates
        """
        # Calculate data movement (simplified estimation)
        total_data_movement = 0
        
        for movement in movements:
            # Find the range size for this token
            for r in self.ranges:
                if r.start_token <= movement.token_value < r.end_token:
                    # Estimate data size based on range size and total load
                    range_percentage = r.size / self.TOKEN_SPACE
                    total_data_movement += range_percentage
                    break
        
        # Estimate time (very rough - depends on network, data size, etc.)
        # Assume 100 MB/s transfer rate and average 1 TB per node
        estimated_data_gb = total_data_movement * 1000  # Rough estimate
        estimated_time_minutes = (estimated_data_gb / 100) * 60 / 1000
        
        # Calculate expected balance improvement
        current_balance = self.stats['balance_score']
        
        # Simulate the movements
        simulated_token_counts = {node: self.stats['nodes'][node]['token_count'] 
                                 for node in self.nodes}
        
        for movement in movements:
            simulated_token_counts[movement.from_node] -= 1
            simulated_token_counts[movement.to_node] += 1
        
        # Calculate new balance score
        ideal_tokens = self.total_tokens / self.n_nodes
        deviations = [abs(count - ideal_tokens) for count in simulated_token_counts.values()]
        mean_deviation = np.mean(deviations)
        std_deviation = np.std(deviations)
        cv = std_deviation / ideal_tokens if ideal_tokens > 0 else 0
        estimated_new_balance = max(0, 1.0 - cv)
        
        balance_improvement = estimated_new_balance - current_balance
        
        cost_estimate = {
            'number_of_movements': len(movements),
            'estimated_data_movement_gb': estimated_data_gb,
            'estimated_time_minutes': estimated_time_minutes,
            'current_balance_score': current_balance,
            'estimated_new_balance_score': estimated_new_balance,
            'balance_improvement': balance_improvement,
            'improvement_percentage': (balance_improvement / current_balance) * 100 if current_balance > 0 else 0
        }
        
        return cost_estimate


def print_rebalancing_report(advisor: RebalancingAdvisor):
    """Print a comprehensive rebalancing report."""
    analysis = advisor.analyze_balance()
    recommendations = advisor.generate_recommendations()
    movements = advisor.suggest_token_movements(max_movements=5)
    
    print("\n" + "="*70)
    print("REBALANCING ANALYSIS REPORT")
    print("="*70)
    
    print(f"\nOVERALL BALANCE:")
    print(f"  Balance Score: {analysis['balance_score']:.3f}")
    print(f"  Status: {analysis['imbalance_severity'].upper()}")
    print(f"  Balanced: {'✓ YES' if analysis['is_balanced'] else '✗ NO'}")
    
    print(f"\nNODE ANALYSIS:")
    for node, node_analysis in analysis['node_analysis'].items():
        status_symbol = '✓' if node_analysis['status'] == 'balanced' else '⚠' if 'slightly' in node_analysis['status'] else '✗'
        print(f"  {status_symbol} {node}:")
        print(f"      Current: {node_analysis['current_tokens']} tokens")
        print(f"      Ideal: {node_analysis['ideal_tokens']:.1f} tokens")
        print(f"      Deviation: {node_analysis['deviation']:+.1f} ({node_analysis['deviation_percentage']:+.1f}%)")
        print(f"      Status: {node_analysis['status'].replace('_', ' ').title()}")
    
    if recommendations:
        print(f"\nRECOMMENDATIONS ({len(recommendations)}):")
        for i, rec in enumerate(recommendations, 1):
            priority_symbol = '🔴' if rec.priority == 'high' else '🟡' if rec.priority == 'medium' else '🟢'
            print(f"\n  {i}. {priority_symbol} {rec.node} [{rec.priority.upper()} PRIORITY]")
            print(f"      Current: {rec.current_tokens} tokens")
            print(f"      Recommended: {rec.recommended_tokens} tokens")
            print(f"      Change: {rec.change:+d} tokens")
            print(f"      Reason: {rec.reason}")
    else:
        print("\n✓ No rebalancing needed - cluster is well balanced!")
    
    if movements:
        print(f"\nSUGGESTED TOKEN MOVEMENTS (Top {len(movements)}):")
        for i, movement in enumerate(movements, 1):
            print(f"\n  {i}. Move token {movement.token_value}")
            print(f"      From: {movement.from_node}")
            print(f"      To: {movement.to_node}")
            print(f"      Impact Score: {movement.impact_score:.2f}")
        
        # Cost estimation
        cost = advisor.estimate_rebalancing_cost(movements)
        print(f"\nESTIMATED REBALANCING COST:")
        print(f"  Movements: {cost['number_of_movements']}")
        print(f"  Data Movement: ~{cost['estimated_data_movement_gb']:.1f} GB")
        print(f"  Estimated Time: ~{cost['estimated_time_minutes']:.1f} minutes")
        print(f"  Current Balance: {cost['current_balance_score']:.3f}")
        print(f"  Expected Balance: {cost['estimated_new_balance_score']:.3f}")
        print(f"  Improvement: {cost['balance_improvement']:+.3f} ({cost['improvement_percentage']:+.1f}%)")
    
    print("\n" + "="*70 + "\n")


if __name__ == '__main__':
    import argparse
    import json
    
    parser = argparse.ArgumentParser(
        description='Analyze ring balance and provide rebalancing recommendations'
    )
    parser.add_argument('ring_file', help='Path to nodetool ring output file')
    parser.add_argument('--max-movements', type=int, default=10,
                       help='Maximum number of token movements to suggest (default: 10)')
    parser.add_argument('--export-json', help='Export recommendations to JSON file')
    
    args = parser.parse_args()
    
    # Parse and analyze ring
    ring_parser = RingParser(args.ring_file)
    tokens = ring_parser.parse_file()
    
    analyzer = TokenAnalyzer(tokens)
    ranges = analyzer.calculate_ranges()
    ranges = analyzer.detect_gaps(ranges)
    stats = analyzer.calculate_statistics(ranges)
    
    # Create advisor and generate report
    advisor = RebalancingAdvisor(tokens, ranges, stats)
    print_rebalancing_report(advisor)
    
    # Export if requested
    if args.export_json:
        analysis = advisor.analyze_balance()
        recommendations = advisor.generate_recommendations()
        movements = advisor.suggest_token_movements(max_movements=args.max_movements)
        cost = advisor.estimate_rebalancing_cost(movements)
        
        export_data = {
            'analysis': analysis,
            'recommendations': [
                {
                    'node': r.node,
                    'current_tokens': r.current_tokens,
                    'recommended_tokens': r.recommended_tokens,
                    'change': r.change,
                    'priority': r.priority,
                    'reason': r.reason
                }
                for r in recommendations
            ],
            'suggested_movements': [
                {
                    'token': m.token_value,
                    'from_node': m.from_node,
                    'to_node': m.to_node,
                    'impact_score': m.impact_score
                }
                for m in movements
            ],
            'cost_estimate': cost
        }
        
        with open(args.export_json, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        print(f"Recommendations exported to: {args.export_json}")

# Made with Bob
