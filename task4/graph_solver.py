import heapq
from collections import defaultdict, deque
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from db_config import get_database_url

DATABASE_URL = get_database_url()

# Graph Data Structures

class TransportGraph:
    def __init__(self):
        # Static Graph for Task 4.1: { source_station: { target_station } }
        self.adjacency = defaultdict(set)
        
        # Time-Dependent Schedule for Task 4.2:
        # { source_station: [ (departure_time, arrival_time, target_station, train_id), ... ] }
        self.schedule = defaultdict(list)
        
        # Mapping Names to IDs and vice versa
        self.id_to_name = {}
        self.name_to_id = {}

    def add_station(self, s_id, s_name):
        self.id_to_name[s_id] = s_name
        self.name_to_id[s_name] = s_id

    def add_connection(self, u, v, dep_time, arr_time, train_id):
        # 1. Add to Static Graph (trains usually go both ways, but we treat as directed)
        self.adjacency[u].add(v)
        
        # 2. Add to Schedule
        # We store edges outgoing from u
        self.schedule[u].append((dep_time, arr_time, v, train_id))

    def sort_schedule(self):
        # Sorting schedule by departure time helps the earliest arrival algorithm
        for u in self.schedule:
            self.schedule[u].sort(key=lambda x: x[0])

# Algorithms

def bfs_shortest_hops(graph, start_name, end_name):
    """
    Task 4.1: Shortest path in terms of number of stops (hops).
    Ignores time, just looks at connectivity.
    """
    if start_name not in graph.name_to_id:
        print(f"Error: Station '{start_name}' not found.")
        return None
    if end_name not in graph.name_to_id:
        print(f"Error: Station '{end_name}' not found.")
        return None

    start_node = graph.name_to_id[start_name]
    end_node = graph.name_to_id[end_name]
    
    # Queue for BFS: (current_node, path_of_names)
    queue = deque([(start_node, [start_name])])
    visited = {start_node}
    
    while queue:
        current, path = queue.popleft()
        
        if current == end_node:
            return path
        
        for neighbor in graph.adjacency[current]:
            if neighbor not in visited:
                visited.add(neighbor)
                new_path = path + [graph.id_to_name[neighbor]]
                queue.append((neighbor, new_path))
                
    return None

def earliest_arrival_search(graph, start_name, end_name, departure_time_str):
    """
    Task 4.2: Earliest Arrival Time (Time-Dependent Dijkstra).
    Finds the route that arrives at the destination as early as possible.
    """
    if start_name not in graph.name_to_id or end_name not in graph.name_to_id:
        return None, "Station not found"

    start_node = graph.name_to_id[start_name]
    end_node = graph.name_to_id[end_name]
    start_time = datetime.strptime(departure_time_str, "%Y-%m-%d %H:%M")
    
    # Priority Queue: (arrival_time, current_node, path_history)
    # We want to minimize 'arrival_time'
    pq = [(start_time, start_node, [])]
    
    # Dictionary to keep track of best arrival time at each station
    min_arrival_times = {start_node: start_time}
    
    # Limit iterations to prevent infinite loops in cyclic graphs (we have directed graph)
    visited_count = 0 
    
    while pq:
        current_time, u, path = heapq.heappop(pq)
        
        # If we reached the target, we found the earliest arrival!
        if u == end_node:
            full_path = path + [(graph.id_to_name[u], current_time.strftime("%H:%M"))]
            return full_path, current_time
        
        # Optimization: If we found a faster way to u already, skip
        if u in min_arrival_times and current_time > min_arrival_times[u]:
            continue
            
        # Explore neighbors via the Schedule
        # We need trains departing >= current_time
        if u in graph.schedule:
            for dep_t, arr_t, v, train_id in graph.schedule[u]:
                
                min_departure_needed = current_time
                # Only add buffer if we are transferring (not at the start)
                if u != start_node:
                    min_departure_needed += timedelta(minutes=2)
                
                # Check constraint
                if dep_t >= min_departure_needed:
                    
                    # Constraint 2: Is this a better arrival time for v?
                    if v not in min_arrival_times or arr_t < min_arrival_times[v]:
                        min_arrival_times[v] = arr_t
                        
                        # Add step to path: (Station Name, Departure Time, Train)
                        new_step = (graph.id_to_name[u], dep_t.strftime("%H:%M"), train_id)
                        heapq.heappush(pq, (arr_t, v, path + [new_step]))

    return None, "No connection found"

# Data Loading

def build_graph_from_db():
    print("⏳ Building Graph from Database...")
    graph = TransportGraph()
    engine = create_engine(DATABASE_URL)

    # Store the range for the UI
    graph.min_date = None
    graph.max_date = None
    
    with engine.connect() as conn:
        # 1. Load Stations
        print("\tLoading Stations...")
        res = conn.execute(text("SELECT station_id, station_name FROM DimStation"))
        for row in res:
            graph.add_station(row[0], row[1])
            
        # 2. Load Connections (Edges)
        # Fetch a subset of data (2-3 days) to keep memory usage low for the demo.
        # Query logic: Join Fact table to itself to find "Sequence" (Dep at A -> Arr at B)
        # Rely on train_id and ordering by time.
        # Define your range here once so it's easy to change
        start_bound = '2025-09-03 00:00'
        end_bound = '2025-09-13 00:00'
        print(f"   Loading Schedule ({start_bound} - {end_bound})...")
        
        query = text(f"""
            SELECT f.train_id, f.station_id, f.is_arrival, f.planned_time
            FROM FactTrainMovement f
            WHERE f.planned_time >= '{start_bound}' AND f.planned_time < '{end_bound}'
            ORDER BY f.train_id, f.planned_time
        """)
        
        rows = conn.execute(query).fetchall()

        if rows:
            # Track the actual date range found in the data
            graph.min_date = rows[0][3]
            graph.max_date = rows[-1][3]
        
        # Process sequential rows to build edges
        # Row N (Departure) -> Row N+1 (Arrival)
        count = 0
        for i in range(len(rows) - 1):
            curr_row , next_row = rows[i], rows[i+1]
            
            # Check if it's the same train
            if curr_row[0] == next_row[0]:
                # Check sequence: Departure (False) -> Arrival (True)
                # Note: Some trains might have Dep -> Dep if data is missing, so we ensure logical flow
                if (not curr_row[2]) and (next_row[2]):
                    
                    u = curr_row[1] # Source Station ID
                    v = next_row[1] # Target Station ID
                    dep_time = curr_row[3]
                    arr_time = next_row[3]
                    train_id = curr_row[0]
                    
                    if u != v: # Ignore loops
                        graph.add_connection(u, v, dep_time, arr_time, train_id)
                        count += 1
                        
        print(f"   Graph Built! Loaded {count} connections.")
        graph.sort_schedule()
        return graph
    
def validate_station(graph, prompt, allow_quit=True):
    """Validate station input with optional quit."""
    while True:
        station = input(prompt).strip()
        if allow_quit and station.lower() == 'quit':
            return None
        if station in graph.name_to_id:
            return station
        print(f"❌ Error: Station '{station}' not found. Please check the spelling.")

# Main Runner

if __name__ == "__main__":
    # 1. Build the graph only ONCE
    berlin_graph = build_graph_from_db()

    default_time = "2025-09-03 08:00"
    
    while True:
        print("\n" + "="*50)
        print("TRANSIT ROUTE FINDER (Type 'quit' to exit)")
        print("="*50)
        
        # 1. Source Validation
        source = validate_station(berlin_graph, "Enter Source Station: ")
        if source is None:
            break
        
        # 2. Target Validation
        target = validate_station(berlin_graph, "Enter Target Station: ")
        if target is None:
            break
        
        # 3. Date and Time Input with Default
        prompt = f"Enter Departure Time [YYYY-MM-DD HH:MM]: "
        start_time_str = input(prompt).strip()
        
        if start_time_str.lower() == 'quit': break
        if not start_time_str: # If user just pressed Enter
            start_time_str = default_time

        # Logic check: is the requested time within our graph's range?
        try:
            req_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M")
            if req_time < berlin_graph.min_date or req_time > berlin_graph.max_date:
                print(f"⚠️ Warning: {start_time_str} is outside the loaded graph range.")
                print(f"   Results may be empty or incomplete.")
        except ValueError:
            print("❌ Invalid format. Please use YYYY-MM-DD HH:MM")
            continue

        print(f"\nSearching for route from {source} to {target}...")

        # Task 4.1: Shortest Hops
        print("\n--- Task 4.1: Shortest Path (Hops) ---")
        path = bfs_shortest_hops(berlin_graph, source, target)
        if path:
            print(f"Path found ({len(path)-1} hops):")
            print(" -> ".join(path))

        # Task 4.2: Earliest Arrival
        print(f"\n--- Task 4.2: Earliest Arrival ---")
        try:
            route, arrival = earliest_arrival_search(berlin_graph, source, target, start_time_str)
            if route:
                print(f"✅ Success! Earliest Arrival: {arrival}")
                print("Itinerary:")
                for step in route:
                    if len(step) == 3:
                        print(f"  Dep {step[1]} from {step[0]} [Train: {step[2]}]")
                    else:
                        print(f"  Arr {step[1]} at   {step[0]}")
            else:
                print(f"❌ Result: {arrival}")
        except ValueError:
            print("❌ Invalid date format. Please use YYYY-MM-DD HH:MM")

    print("\nShutting down. Goodbye!")