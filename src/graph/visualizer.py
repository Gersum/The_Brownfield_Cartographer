import os
import networkx as nx
from rich.console import Console

console = Console()

class Visualizer:
    """Utility class to generate premium visualizations from KnowledgeGraph."""
    
    def __init__(self, G, output_dir):
        """
        Initialize with a NetworkX graph and output directory.
        G can be any NetworkX graph (module or lineage).
        """
        self.G = G
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def generate_interactive_network(self, filename="network_map.html"):
        """Generate a physics-enabled interactive HTML graph using Pyvis."""
        try:
            from pyvis.network import Network
            
            # Create a Pyvis network
            net = Network(height="750px", width="100%", bgcolor="#0a0a0f", font_color="white", notebook=False)
            
            # Stylize based on node type
            for node_id, data in self.G.nodes(data=True):
                label = node_id.split('/')[-1]
                node_type = data.get('node_type', 'unknown')
                
                color = "#00f2ff"  # Default Cyan
                shape = "dot"
                size = 20
                
                # Critical hubs (PageRank > 0.1)
                if data.get('pagerank', 0) > 0.1:
                    color = "#ff9966"  # Orange
                    size = 35
                
                # Module types
                if node_type == 'dataset':
                    color = "#4caf50" if data.get('is_source') else "#e91e63" if data.get('is_sink') else "#9090a0"
                    shape = "database"
                elif node_type == 'transformation':
                    color = "#ff00ff"
                    shape = "triangle"
                
                net.add_node(node_id, label=label, title=f"Type: {node_type}\nID: {node_id}", 
                            color=color, shape=shape, size=size)
            
            # Add edges
            for source, target in self.G.edges():
                net.add_edge(source, target, color="rgba(255,255,255,0.2)")
            
            # Set physics options for premium feel
            net.toggle_physics(True)
            net.set_options("""
            var options = {
              "physics": {
                "forceAtlas2Based": {
                  "gravitationalConstant": -50,
                  "centralGravity": 0.01,
                  "springLength": 100,
                  "springConstant": 0.08
                },
                "maxVelocity": 50,
                "solver": "forceAtlas2Based",
                "timestep": 0.35,
                "stabilization": { "iterations": 150 }
              }
            }
            """)
            
            output_path = os.path.join(self.output_dir, filename)
            net.save_graph(output_path)
            console.print(f"  ✨ [bold cyan]Interactive network map generated:[/bold cyan] {output_path}")
            return output_path
            
        except ImportError:
            console.print("  ⚠️  [orange3]Pyvis not installed. Skipping interactive network generation.[/orange3]")
        except Exception as e:
            console.print(f"  ❌ Failed to generate interactive network: {e}")

    def generate_static_map(self, filename="system_architecture.png"):
        """Generate a high-resolution static PNG visualization using Matplotlib."""
        try:
            import matplotlib.pyplot as plt
            import matplotlib.patches as mpatches
            
            plt.figure(figsize=(24, 18), facecolor='#0a0a0f')
            plt.style.use('dark_background')
            
            # Category color mapping
            CATEGORY_COLORS = {
                'model': '#4caf50',     # Green
                'logic': '#ff9800',     # Orange
                'interface': '#2196f3', # Blue
                'utility': '#9c27b0',   # Purple
                'unknown': '#00f2ff'    # Cyan
            }
            
            # Use a good layout (try spring first, it's more stable for large graphs than random)
            console.print(f"  🎨 Calculating layout for {len(self.G)} nodes...")
            try:
                # k= Repulsive force. Increasing it spreads nodes further apart.
                pos = nx.spring_layout(self.G, k=0.25, iterations=50)
            except Exception as le:
                console.print(f"  ⚠️  Layout failed: {le}. Falling back to random.")
                pos = nx.random_layout(self.G)
            
            # Draw nodes by category
            node_colors = []
            node_sizes = []
            
            for n, data in self.G.nodes(data=True):
                category = data.get('category', 'unknown')
                node_type = data.get('node_type', 'unknown')
                rank = data.get('pagerank', 0)
                
                # Sizing
                size = 400
                if rank > 0.1:
                    size = 1200
                elif node_type == 'dataset':
                    size = 600
                
                # Coloring
                if node_type == 'dataset':
                    color = '#E91E63' # Pink for datasets
                else:
                    color = CATEGORY_COLORS.get(category, CATEGORY_COLORS['unknown'])
                
                node_colors.append(color)
                node_sizes.append(size)
            
            nx.draw_networkx_nodes(self.G, pos, node_color=node_colors, node_size=node_sizes, alpha=0.8, edgecolors="white", linewidths=0.5)
            
            # Draw edges
            nx.draw_networkx_edges(self.G, pos, edge_color='#808080', alpha=0.1, arrows=True, width=0.5)
            
            # Draw labels (only for significant nodes if too many)
            labels = {}
            for n, data in self.G.nodes(data=True):
                if len(self.G) < 100 or data.get('pagerank', 0) > 0.01:
                    labels[n] = n.split('/')[-1]
            
            nx.draw_networkx_labels(self.G, pos, labels, font_size=7, font_color='white', font_family='sans-serif', alpha=0.9)
            
            # Add Legend
            legend_handles = [
                mpatches.Patch(color=color, label=cat.capitalize()) 
                for cat, color in CATEGORY_COLORS.items()
            ]
            legend_handles.append(mpatches.Patch(color='#E91E63', label='Dataset/Table'))
            plt.legend(handles=legend_handles, loc='upper right', frameon=True, facecolor='#1a1a24', edgecolor='white')
            
            plt.title("Codebase Architecture Overview (Color-Coded)", color='white', size=24, pad=20)
            plt.axis('off')
            
            output_path = os.path.join(self.output_dir, filename)
            plt.savefig(output_path, dpi=300, facecolor='#0a0a0f', bbox_inches='tight')
            plt.close()
            
            console.print(f"  🖼️  [bold cyan]Static architecture map generated:[/bold cyan] {output_path}")
            return output_path
            
            console.print(f"  🖼️  [bold cyan]Static architecture map generated:[/bold cyan] {output_path}")
            return output_path
            
        except ImportError:
            console.print("  ⚠️  [orange3]Matplotlib not installed. Skipping static map generation.[/orange3]")
        except Exception as e:
            console.print(f"  ❌ Failed to generate static map: {e}")
