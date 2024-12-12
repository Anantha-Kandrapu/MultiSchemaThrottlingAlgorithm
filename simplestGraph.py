import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from output import print_dependency_graph
from crystal import Pipeline

def test_simple_linear_overload():
    service_flows = {
        "Source": {"S1": (90, 100)},
        "Processor": {"S1": (90, 80)},
        "Destination": {"S1": (90, 80)}
    }

    schema_capacities = {
        "Source": {"S1": (100, 100)},
        "Processor": {"S1": (100, 100)},
        "Destination": {"S1": (50, 60)}
    }

    graph = {
        "Source": ["Processor"],
        "Processor": ["Destination"],
        "Destination": []
    }

    schema_priorities = {"S1": 1}
    pipeline = Pipeline(service_flows, schema_capacities, graph, schema_priorities)
    print_dependency_graph(pipeline)
    pipeline.run_cycle(service_flows)