import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crystal import Pipeline

def test_multi_source_multi_destination_normal(capsys):
    service_flows = {
        "Source1": {"S1": (50, 50), "S2": (30, 30)},
        "Source2": {"S1": (40, 40), "S2": (20, 20)},
        "Processor": {"S1": (90, 90), "S2": (50, 50)},
        "Dest1": {"S1": (45, 45), "S2": (25, 25)},
        "Dest2": {"S1": (45, 45), "S2": (25, 25)}
    }

    schema_capacities = {
        "Source1": {"S1": (40, 60), "S2": (20, 40)},
        "Source2": {"S1": (30, 50), "S2": (10, 30)},
        "Processor": {"S1": (80, 100), "S2": (40, 60)},
        "Dest1": {"S1": (40, 50), "S2": (20, 30)},
        "Dest2": {"S1": (40, 50), "S2": (20, 30)}
    }

    graph = {
        "Source1": ["Processor"],
        "Source2": ["Processor"],
        "Processor": ["Dest1", "Dest2"],
        "Dest1": [],
        "Dest2": []
    }

    schema_priorities = {
        "S1": 2,
        "S2": 1
    }

    pipeline = Pipeline(service_flows, schema_capacities, graph, schema_priorities)
    
    pipeline.run_cycle(service_flows)
    
    captured = capsys.readouterr()
    print("\n--- Multi-Source Multi-Destination Normal ---")
    print(captured.out)
    print("--- End of Output ---\n")

def test_multi_source_multi_destination_overloaded(capsys):
    service_flows = {
        "Source1": {"S1": (70, 70), "S2": (50, 50)},
        "Source2": {"S1": (60, 60), "S2": (40, 40)},
        "Processor": {"S1": (130, 130), "S2": (90, 90)},
        "Dest1": {"S1": (65, 65), "S2": (45, 45)},
        "Dest2": {"S1": (65, 65), "S2": (45, 45)}
    }

    schema_capacities = {
        "Source1": {"S1": (40, 60), "S2": (20, 40)},
        "Source2": {"S1": (30, 50), "S2": (10, 30)},
        "Processor": {"S1": (80, 100), "S2": (40, 60)},
        "Dest1": {"S1": (40, 50), "S2": (20, 30)},
        "Dest2": {"S1": (40, 50), "S2": (20, 30)}
    }

    graph = {
        "Source1": ["Processor"],
        "Source2": ["Processor"],
        "Processor": ["Dest1", "Dest2"],
        "Dest1": [],
        "Dest2": []
    }

    schema_priorities = {
        "S1": 2,
        "S2": 1
    }

    pipeline = Pipeline(service_flows, schema_capacities, graph, schema_priorities)
    
    pipeline.run_cycle(service_flows)
    
    captured = capsys.readouterr()
    print("\n--- Multi-Source Multi-Destination Overloaded  ---")
    print(captured.out)
    print("--- End of Output ---\n")
