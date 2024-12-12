
def get_user_input():
    schemas = [f"S{i}" for i in range(1, 8)]
    service_flows = {}

    services = [
        "C1",
        "C2",
        "C3",
        "C4",
        "C5",
        "C6",
        "C7",
        "AggStream",
        "SlowLane",
        "R1",
        "R2",
        "CDIS1",
        "CDIS2",
        "CDIS3",
        "Bungee1",
        "Bungee2",
        "Bungee3",
        "Bungee4",
        "Bungee5",
        "Bungee6",
        "Bungee7",
        "Bungee8",
        "Bungee9",
        "Hoth",
    ]

    print("Input and output flows for each service and schema.")
    print("Format: input_flow,output_flow.")
    for service in services:
        service_flows[service] = {}
        print(f"\nEnter flows for {service}:")

        if service in ["C1", "C2", "C3", "C4", "C5", "C6", "C7"]:
            target_schema = f"S{service[1]}"
            for schema in schemas:
                if schema == target_schema:
                    flows = input(f"  {schema}: ")
                    if flows == "":
                        service_flows[service][schema] = (150, 150)
                    else:
                        in_flow, out_flow = map(int, flows.split(","))
                        service_flows[service][schema] = (in_flow, out_flow)
                else:
                    service_flows[service][schema] = (0, 0)
        else:
            for schema in schemas:
                flows = input(f"  {schema}: ")
                if flows == "":
                    service_flows[service][schema] = (150, 150)
                else:
                    in_flow, out_flow = map(int, flows.split(","))
                    service_flows[service][schema] = (in_flow, out_flow)

    schema_capacities = {}
    for service in services:
        schema_capacities[service] = {}
        print(f"\nEnter min and max capacities for {service} (format: min,max):")
        for schema in schemas:
            capacities = input(f"  {schema}: ")
            if capacities:
                min_cap, max_cap = map(int, capacities.split(","))
                schema_capacities[service][schema] = (min_cap, max_cap)
            else:
                schema_capacities[service][schema] = (10, 150)
    return service_flows, schema_capacities