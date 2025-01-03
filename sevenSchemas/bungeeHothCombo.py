import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crystal import Pipeline

def test_bungee_hoth_combo_overload(capsys):
    service_flows = {
        "C1": {"S1": (200, 200), "S2": (0, 0), "S3": (0, 0), "S4": (0, 0), "S5": (0, 0), "S6": (0, 0), "S7": (0, 0)},
        "C2": {"S1": (0, 0), "S2": (50, 50), "S3": (0, 0), "S4": (0, 0), "S5": (0, 0), "S6": (0, 0), "S7": (0, 0)},
        "C3": {"S1": (0, 0), "S2": (0, 0), "S3": (50, 50), "S4": (0, 0), "S5": (0, 0), "S6": (0, 0), "S7": (0, 0)},
        "C4": {"S1": (0, 0), "S2": (0, 0), "S3": (0, 0), "S4": (50, 50), "S5": (0, 0), "S6": (0, 0), "S7": (0, 0)},
        "C5": {"S1": (0, 0), "S2": (0, 0), "S3": (0, 0), "S4": (0, 0), "S5": (50, 50), "S6": (0, 0), "S7": (0, 0)},
        "C6": {"S1": (0, 0), "S2": (0, 0), "S3": (0, 0), "S4": (0, 0), "S5": (0, 0), "S6": (50, 50), "S7": (0, 0)},
        "C7": {"S1": (0, 0), "S2": (0, 0), "S3": (0, 0), "S4": (0, 0), "S5": (0, 0), "S6": (0, 0), "S7": (50, 50)},
        "AggStream": {"S1": (200, 200), "S2": (50, 50), "S3": (50, 50), "S4": (50, 50), "S5": (50, 50), "S6": (50, 50), "S7": (50, 50)},
        "SlowLane": {"S1": (0, 0), "S2": (0, 0), "S3": (0, 0), "S4": (0, 0), "S5": (0, 0), "S6": (0, 0), "S7": (0, 0)},
        "R1": {"S1": (100, 100), "S2": (25, 25), "S3": (25, 25), "S4": (25, 25), "S5": (25, 25), "S6": (25, 25), "S7": (25, 25)},
        "R2": {"S1": (100, 100), "S2": (25, 25), "S3": (25, 25), "S4": (25, 25), "S5": (25, 25), "S6": (25, 25), "S7": (25, 25)},
        "CDIS1": {"S1": (33, 33), "S2": (8, 8), "S3": (8, 8), "S4": (8, 8), "S5": (8, 8), "S6": (8, 8), "S7": (8, 8)},
        "CDIS2": {"S1": (33, 33), "S2": (8, 8), "S3": (8, 8), "S4": (8, 8), "S5": (8, 8), "S6": (8, 8), "S7": (8, 8)},
        "CDIS3": {"S1": (34, 34), "S2": (9, 9), "S3": (9, 9), "S4": (9, 9), "S5": (9, 9), "S6": (9, 9), "S7": (9, 9)},
        "Hoth": {"S1": (250, 100), "S2": (25, 25), "S3": (25, 25), "S4": (25, 25), "S5": (25, 25), "S6": (25, 25), "S7": (25, 25)},
        "Bungee1": {"S1": (11, 11), "S2": (3, 3), "S3": (3, 3), "S4": (3, 3), "S5": (3, 3), "S6": (3, 3), "S7": (3, 3)},
        "Bungee2": {"S1": (11, 11), "S2": (3, 3), "S3": (3, 3), "S4": (3, 3), "S5": (3, 3), "S6": (3, 3), "S7": (3, 3)},
        "Bungee3": {"S1": (11, 11), "S2": (2, 2), "S3": (2, 2), "S4": (2, 2), "S5": (2, 2), "S6": (2, 2), "S7": (2, 2)},
        "Bungee4": {"S1": (11, 11), "S2": (2, 2), "S3": (2, 2), "S4": (2, 2), "S5": (2, 2), "S6": (2, 2), "S7": (2, 2)},
        "Bungee5": {"S1": (11, 11), "S2": (2, 2), "S3": (2, 2), "S4": (2, 2), "S5": (2, 2), "S6": (2, 2), "S7": (2, 2)},
        "Bungee6": {"S1": (11, 11), "S2": (2, 2), "S3": (2, 2), "S4": (2, 2), "S5": (2, 2), "S6": (2, 2), "S7": (2, 2)},
        "Bungee7": {"S1": (50, 11), "S2": (2, 2), "S3": (2, 2), "S4": (2, 2), "S5": (2, 2), "S6": (2, 2), "S7": (2, 2)},
        "Bungee8": {"S1": (11, 11), "S2": (2, 2), "S3": (2, 2), "S4": (2, 2), "S5": (2, 2), "S6": (2, 2), "S7": (2, 2)},
        "Bungee9": {"S1": (12, 12), "S2": (2, 2), "S3": (2, 2), "S4": (2, 2), "S5": (2, 2), "S6": (2, 2), "S7": (2, 2)},
    }

    schema_capacities = {
        "C1": {"S1": (200, 250), "S2": (50, 100), "S3": (50, 100), "S4": (50, 100), "S5": (50, 100), "S6": (50, 100), "S7": (50, 100)},
        "C2": {"S1": (50, 100), "S2": (50, 100), "S3": (50, 100), "S4": (50, 100), "S5": (50, 100), "S6": (50, 100), "S7": (50, 100)},
        "C3": {"S1": (50, 100), "S2": (50, 100), "S3": (50, 100), "S4": (50, 100), "S5": (50, 100), "S6": (50, 100), "S7": (50, 100)},
        "C4": {"S1": (50, 100), "S2": (50, 100), "S3": (50, 100), "S4": (50, 100), "S5": (50, 100), "S6": (50, 100), "S7": (50, 100)},
        "C5": {"S1": (50, 100), "S2": (50, 100), "S3": (50, 100), "S4": (50, 100), "S5": (50, 100), "S6": (50, 100), "S7": (50, 100)},
        "C6": {"S1": (50, 100), "S2": (50, 100), "S3": (50, 100), "S4": (50, 100), "S5": (50, 100), "S6": (50, 100), "S7": (50, 100)},
        "C7": {"S1": (50, 100), "S2": (50, 100), "S3": (50, 100), "S4": (50, 100), "S5": (50, 100), "S6": (50, 100), "S7": (50, 100)},
        "AggStream": {"S1": (200, 300), "S2": (50, 100), "S3": (50, 100), "S4": (50, 100), "S5": (50, 100), "S6": (50, 100), "S7": (50, 100)},
        "SlowLane": {"S1": (50, 100), "S2": (50, 100), "S3": (50, 100), "S4": (50, 100), "S5": (50, 100), "S6": (50, 100), "S7": (50, 100)},
        "R1": {"S1": (100, 150), "S2": (25, 50), "S3": (25, 50), "S4": (25, 50), "S5": (25, 50), "S6": (25, 50), "S7": (25, 50)},
        "R2": {"S1": (100, 150), "S2": (25, 50), "S3": (25, 50), "S4": (25, 50), "S5": (25, 50), "S6": (25, 50), "S7": (25, 50)},
        "CDIS1": {"S1": (33, 50), "S2": (8, 20), "S3": (8, 20), "S4": (8, 20), "S5": (8, 20), "S6": (8, 20), "S7": (8, 20)},
        "CDIS2": {"S1": (33, 50), "S2": (8, 20), "S3": (8, 20), "S4": (8, 20), "S5": (8, 20), "S6": (8, 20), "S7": (8, 20)},
        "CDIS3": {"S1": (34, 50), "S2": (9, 20), "S3": (9, 20), "S4": (9, 20), "S5": (9, 20), "S6": (9, 20), "S7": (9, 20)},
        "Hoth": {"S1": (80, 90), "S2": (25, 50), "S3": (25, 50), "S4": (25, 50), "S5": (25, 50), "S6": (25, 50), "S7": (25, 50)},
        "Bungee1": {"S1": (11, 20), "S2": (3, 10), "S3": (3, 10), "S4": (3, 10), "S5": (3, 10), "S6": (3, 10), "S7": (3, 10)},
        "Bungee2": {"S1": (11, 20), "S2": (3, 10), "S3": (3, 10), "S4": (3, 10), "S5": (3, 10), "S6": (3, 10), "S7": (3, 10)},
        "Bungee3": {"S1": (11, 20), "S2": (2, 10), "S3": (2, 10), "S4": (2, 10), "S5": (2, 10), "S6": (2, 10), "S7": (2, 10)},
        "Bungee4": {"S1": (11, 20), "S2": (2, 10), "S3": (2, 10), "S4": (2, 10), "S5": (2, 10), "S6": (2, 10), "S7": (2, 10)},
        "Bungee5": {"S1": (11, 20), "S2": (2, 10), "S3": (2, 10), "S4": (2, 10), "S5": (2, 10), "S6": (2, 10), "S7": (2, 10)},
        "Bungee6": {"S1": (11, 20), "S2": (2, 10), "S3": (2, 10), "S4": (2, 10), "S5": (2, 10), "S6": (2, 10), "S7": (2, 10)},
        "Bungee7": {"S1": (11, 20), "S2": (2, 10), "S3": (2, 10), "S4": (2, 10), "S5": (2, 10), "S6": (2, 10), "S7": (2, 10)},
        "Bungee8": {"S1": (11, 20), "S2": (2, 10), "S3": (2, 10), "S4": (2, 10), "S5": (2, 10), "S6": (2, 10), "S7": (2, 10)},
        "Bungee9": {"S1": (12, 20), "S2": (2, 10), "S3": (2, 10), "S4": (2, 10), "S5": (2, 10), "S6": (2, 10), "S7": (2, 10)},
    }

    graph = {
        "C1": ["AggStream", "SlowLane"],
        "C2": ["AggStream", "SlowLane"],
        "C3": ["AggStream", "SlowLane"],
        "C4": ["AggStream", "SlowLane"],
        "C5": ["AggStream", "SlowLane"],
        "C6": ["AggStream", "SlowLane"],
        "C7": ["AggStream", "SlowLane"],
        "AggStream": ["R1", "R2"],
        "SlowLane": ["AggStream"],
        "R1": ["CDIS1", "CDIS2", "CDIS3"],
        "R2": ["Hoth"],
        "CDIS1": [f"Bungee{i}" for i in range(1, 10)],
        "CDIS2": [f"Bungee{i}" for i in range(1, 10)],
        "CDIS3": [f"Bungee{i}" for i in range(1, 10)],
        "Hoth": [],
        "Bungee1": [], "Bungee2": [], "Bungee3": [], "Bungee4": [], "Bungee5": [],
        "Bungee6": [], "Bungee7": [], "Bungee8": [], "Bungee9": [],
    }

    schema_priorities = {
        "S1": 7, "S2": 6, "S3": 5, "S4": 4, "S5": 3, "S6": 2, "S7": 1
    }

    pipeline = Pipeline(service_flows, schema_capacities, graph, schema_priorities)
    
    pipeline.run_cycle(service_flows)
    
    captured = capsys.readouterr()
    
    print("\n--- Full Pipeline Output ---")
    print(captured.out)
    print("--- End of Pipeline Output ---\n")