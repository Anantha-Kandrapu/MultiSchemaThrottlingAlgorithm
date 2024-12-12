import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crystal import Pipeline

def test_simple_linear_overload():
    """
    Scenario 1: Simple Linear Overload
    --------------------------------
    Source -> Processor -> Destination

    What's happening:
    - Source is sending 100 requests/sec
    - Processor can only handle 80 requests/sec
    - This creates a bottleneck at the Processor
    - System should automatically throttle the Source to match Processor's capacity

    Expected behavior:
    - Processor will be detected as overloaded
    - Backpressure will be applied to Source
    - Final flow should stabilize around 80 requests/sec
    """
    service_flows = {
        "Source": {"S1": (100, 100)},
        "Processor": {"S1": (100, 80)},
        "Destination": {"S1": (80, 80)}
    }

    schema_capacities = {
        "Source": {"S1": (0, 120)},
        "Processor": {"S1": (60, 80)},
        "Destination": {"S1": (60, 100)}
    }

    graph = {
        "Source": ["Processor"],
        "Processor": ["Destination"],
        "Destination": []
    }

    schema_priorities = {"S1": 1}

    pipeline = Pipeline(service_flows, schema_capacities, graph, schema_priorities)
    pipeline.run_cycle(service_flows)

def test_dual_path_bottleneck():
    """
    Scenario 2: Dual Path with Bottleneck
    -----------------------------------
    Source1 --→ ProcessorA --→ Destination
    Source2 --→ ProcessorB --↗

    What's happening:
    - Source1 sends 60 requests/sec to ProcessorA
    - Source2 sends 70 requests/sec to ProcessorB
    - Destination can only handle 100 requests/sec total
    - Combined input (130) exceeds Destination capacity

    Expected behavior:
    - Destination will be detected as overloaded
    - Backpressure will be applied equally to both paths
    - Sources should be throttled proportionally
    - Final flow should total 100 requests/sec at Destination
    """
    service_flows = {
        "Source1": {"S1": (60, 60)},
        "Source2": {"S1": (70, 70)},
        "ProcessorA": {"S1": (60, 60)},
        "ProcessorB": {"S1": (70, 70)},
        "Destination": {"S1": (130, 100)}
    }

    schema_capacities = {
        "Source1": {"S1": (0, 80)},
        "Source2": {"S1": (0, 80)},
        "ProcessorA": {"S1": (50, 70)},
        "ProcessorB": {"S1": (50, 80)},
        "Destination": {"S1": (80, 100)}
    }

    graph = {
        "Source1": ["ProcessorA"],
        "Source2": ["ProcessorB"],
        "ProcessorA": ["Destination"],
        "ProcessorB": ["Destination"],
        "Destination": []
    }

    schema_priorities = {"S1": 1}

    pipeline = Pipeline(service_flows, schema_capacities, graph, schema_priorities)
    pipeline.run_cycle(service_flows)

def test_multi_schema_priority():
    """
    Scenario 3: Multi-Schema Priority Handling
    ---------------------------------------
    Source --→ Processor --→ Destination
    
    What's happening:
    - Source sends two types of requests: High Priority (S1) and Low Priority (S2)
    - S1: 70 requests/sec (High Priority)
    - S2: 50 requests/sec (Low Priority)
    - Processor can only handle 100 requests/sec total
    
    Expected behavior:
    - When overloaded, system should prioritize S1 traffic
    - S2 traffic should be throttled more aggressively
    - High priority traffic should maintain higher throughput
    """
    service_flows = {
        "Source": {
            "S1": (70, 70),  # High priority traffic
            "S2": (50, 50)   # Low priority traffic
        },
        "Processor": {
            "S1": (70, 70),
            "S2": (50, 30)
        },
        "Destination": {
            "S1": (70, 70),
            "S2": (30, 30)
        }
    }

    schema_capacities = {
        "Source": {
            "S1": (0, 80),
            "S2": (0, 60)
        },
        "Processor": {
            "S1": (60, 70),
            "S2": (20, 30)
        },
        "Destination": {
            "S1": (60, 80),
            "S2": (20, 40)
        }
    }

    graph = {
        "Source": ["Processor"],
        "Processor": ["Destination"],
        "Destination": []
    }

    schema_priorities = {
        "S1": 2,  # Higher priority
        "S2": 1   # Lower priority
    }

    pipeline = Pipeline(service_flows, schema_capacities, graph, schema_priorities)
    pipeline.run_cycle(service_flows)

def test_complex_diamond_pattern():
    """
    Scenario 4: Complex Diamond Pattern with Multiple Schemas
    ---------------------------------------------------
    
                  /--→ ProcessorA --→\
    Source --→ Split                  Merger --→ Destination
                  \--→ ProcessorB --→/
    
    What's happening:
    - Source sends mixed traffic (S1: 80/sec, S2: 60/sec)
    - Traffic splits between ProcessorA and ProcessorB
    - Merger combines traffic but has limited capacity
    - Destination has even lower capacity
    
    Expected behavior:
    - System should balance load between ProcessorA and ProcessorB
    - When Merger gets overloaded, backpressure should propagate up
    - High priority schema (S1) should maintain better throughput
    - End-to-end flow should stabilize within capacity limits
    """
    service_flows = {
        "Source": {"S1": (80, 80), "S2": (60, 60)},
        "Split": {"S1": (80, 80), "S2": (60, 60)},
        "ProcessorA": {"S1": (40, 40), "S2": (30, 30)},
        "ProcessorB": {"S1": (40, 40), "S2": (30, 30)},
        "Merger": {"S1": (80, 70), "S2": (60, 50)}
    }

    schema_capacities = {
        "Source": {"S1": (0, 100), "S2": (0, 80)},
        "Split": {"S1": (70, 90), "S2": (50, 70)},
        "ProcessorA": {"S1": (30, 50), "S2": (20, 40)},
        "ProcessorB": {"S1": (30, 50), "S2": (20, 40)},
        "Merger": {"S1": (60, 70), "S2": (40, 50)}
    }

    graph = {
        "Source": ["Split"],
        "Split": ["ProcessorA", "ProcessorB"],
        "ProcessorA": ["Merger"],
        "ProcessorB": ["Merger"],
        "Merger": []
    }

    schema_priorities = {
        "S1": 2,  # Higher priority
        "S2": 1   # Lower priority
    }

    pipeline = Pipeline(service_flows, schema_capacities, graph, schema_priorities)
    pipeline.run_cycle(service_flows)

# Run all tests with clear separation
def run_all_tests():
    print("\n" + "="*80)
    print("TEST 1: SIMPLE LINEAR OVERLOAD")
    print("="*80)
    test_simple_linear_overload()

    print("\n" + "="*80)
    print("TEST 2: DUAL PATH BOTTLENECK")
    print("="*80)
    test_dual_path_bottleneck()

    print("\n" + "="*80)
    print("TEST 3: MULTI-SCHEMA PRIORITY HANDLING")
    print("="*80)
    test_multi_schema_priority()

    print("\n" + "="*80)
    print("TEST 4: COMPLEX DIAMOND PATTERN")
    print("="*80)
    test_complex_diamond_pattern()

if __name__ == "__main__":
    run_all_tests()