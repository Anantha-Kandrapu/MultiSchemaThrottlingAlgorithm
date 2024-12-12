import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crystal import Pipeline

def test_multiple_inputs_single_destination_single_schema(capsys):
    service_flows = {
        "Input1": {"S1": (50, 50)},
        "Input2": {"S1": (70, 70)},
        "Aggregator": {"S1": (150, 150)},
        "Destination": {"S1": (150, 150)}
    }

    schema_capacities = {
        "Input1": {"S1": (40, 60)},
        "Input2": {"S1": (60, 80)},
        "Aggregator": {"S1": (120, 180)},
        "Destination": {"S1": (130, 170)}
    }

    graph = {
        "Input1": ["Aggregator"],
        "Input2": ["Aggregator"],
        "Aggregator": ["Destination"],
        "Destination": []
    }

    schema_priorities = {
        "S1": 1
    }

    pipeline = Pipeline(service_flows, schema_capacities, graph, schema_priorities)
    
    pipeline.run_cycle(service_flows)
    
    captured = capsys.readouterr()
        
    print("\n--- Full Pipeline Output ---")
    print(captured.out)
    print("--- End of Pipeline Output ---\n")
