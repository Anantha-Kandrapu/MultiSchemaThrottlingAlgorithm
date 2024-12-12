import random
from enum import Enum
from collections import defaultdict, deque
from tabulate import tabulate

from output import print_dependency_graph, print_service_table


class ServiceStatus(Enum):
    NORMAL = "NORMAL"
    OVERLOADED = "OVERLOADED"
    UNDERUTILIZED = "UNDERUTILIZED"


class ServiceAction(Enum):
    NO_ACTION = "NO ACTION"
    SPEEDUP = "SPEEDUP"
    SLOWDOWN = "SLOWDOWN"


class Schema:
    def __init__(self, name, priority):
        self.name = name
        self.priority = priority

    def __repr__(self):
        return self.name


class Service:
    def __init__(self, name, supported_schemas, initial_capacity=150):
        self.name = name
        self.supported_schemas = supported_schemas
        self.min_capacity = 100
        self.max_capacity = 666
        self.current_capacity = initial_capacity
        self.incoming_flow = {schema: 0 for schema in supported_schemas}
        self.outgoing_flow = {schema: 0 for schema in supported_schemas}
        self.allocated_capacity = self.allocate_capacity()
        self.status = ServiceStatus.NORMAL
        self.action = ServiceAction.NO_ACTION

    def allocate_capacity(self):
        total_priority = sum(schema.priority for schema in self.supported_schemas)
        return {
            schema: int(self.current_capacity * schema.priority / total_priority)
            for schema in self.supported_schemas
        }

    def process_flow(self):
        total_incoming = sum(self.incoming_flow.values())
        print(f"\nProcessing flow for {self.name}")
        print(f" Incoming flow: {self.incoming_flow}")
        print(f" Current capacity: {self.current_capacity}")
        print(f" Initial allocated capacity: {self.allocated_capacity}")

        for schema in self.supported_schemas:
            incoming = self.incoming_flow[schema]
            allocated = self.allocated_capacity[schema]
            self.outgoing_flow[schema] = min(incoming, allocated)

        self.reallocate_capacity()
        print(f" After reallocation:")
        print(f" Allocated capacity: {self.allocated_capacity}")
        print(f" Outgoing flow: {self.outgoing_flow}")
        print(f" Service status: {self.status.value}")
        print(f" Service action: {self.action.value}")
    
    def reallocate_capacity_across_schemas(self):
        total_incoming = sum(self.incoming_flow.values())
        total_allocated = sum(self.allocated_capacity.values())
        
        if total_incoming <= total_allocated:
            return  # No need to reallocate if we have enough capacity

        # Sort schemas by incoming flow (descending) to prioritize high-demand schemas
        sorted_schemas = sorted(
            self.supported_schemas,
            key=lambda s: self.incoming_flow[s],
            reverse=True
        )

        # Redistribute excess capacity
        excess_capacity = {s: max(0, self.allocated_capacity[s] - self.incoming_flow[s]) for s in self.supported_schemas}
        total_excess = sum(excess_capacity.values())

        for schema in sorted_schemas:
            if self.incoming_flow[schema] > self.allocated_capacity[schema]:
                needed_capacity = self.incoming_flow[schema] - self.allocated_capacity[schema]
                reallocated = min(needed_capacity, total_excess)
                self.allocated_capacity[schema] += reallocated
                total_excess -= reallocated

        # Redistribute any remaining excess capacity proportionally
        if total_excess > 0:
            total_deficit = sum(max(0, self.incoming_flow[s] - self.allocated_capacity[s]) for s in self.supported_schemas)
            for schema in sorted_schemas:
                if self.incoming_flow[schema] > self.allocated_capacity[schema]:
                    deficit = self.incoming_flow[schema] - self.allocated_capacity[schema]
                    share = deficit / total_deficit if total_deficit > 0 else 0
                    self.allocated_capacity[schema] += int(total_excess * share)

    def process_flow(self):
        print(f"\nProcessing flow for {self.name}")
        print(f" Incoming flow: {self.incoming_flow}")
        print(f" Current capacity: {self.current_capacity}")
        print(f" Initial allocated capacity: {self.allocated_capacity}")

        self.reallocate_capacity_across_schemas()
        
        for schema in self.supported_schemas:
            incoming = self.incoming_flow[schema]
            allocated = self.allocated_capacity[schema]
            self.outgoing_flow[schema] = min(incoming, allocated)

        print(f" After reallocation:")
        print(f" Allocated capacity: {self.allocated_capacity}")
        print(f" Outgoing flow: {self.outgoing_flow}")
        print(f" Service status: {self.status.value}")
        print(f" Service action: {self.action.value}")

    def reallocate_capacity(self):
        print(f" Reallocating capacity for {self.name}")
        sorted_schemas = sorted(
            self.supported_schemas, key=lambda s: s.priority, reverse=True
        )
        total_incoming = sum(self.incoming_flow.values())

        if total_incoming <= self.current_capacity:
            for schema in sorted_schemas:
                self.allocated_capacity[schema] = self.incoming_flow[schema]
        else:
            for schema in sorted_schemas:
                self.allocated_capacity[schema] = int(
                    self.current_capacity
                    * (self.incoming_flow[schema] / total_incoming)
                )

        remaining_capacity = self.current_capacity - sum(
            self.allocated_capacity.values()
        )
        while remaining_capacity > 0:
            for schema in sorted_schemas:
                if remaining_capacity > 0:
                    self.allocated_capacity[schema] += 1
                    remaining_capacity -= 1
                else:
                    break

        print(f" Final allocated capacity: {self.allocated_capacity}")
        print(f" Final outgoing flow: {self.outgoing_flow}")


class Pipeline:
    def __init__(self, service_flows):
        self.schemas = [Schema(f"S{i}", 8 - i) for i in range(1, 8)]

        self.services = {
            "C1": Service(
                "C1", [self.schemas[0]], service_flows.get("C1", (150, 150))[0]
            ),
            "C2": Service(
                "C2", [self.schemas[1]], service_flows.get("C2", (150, 150))[0]
            ),
            "C3": Service(
                "C3", [self.schemas[2]], service_flows.get("C3", (150, 150))[0]
            ),
            "C4": Service(
                "C4", [self.schemas[3]], service_flows.get("C4", (150, 150))[0]
            ),
            "C5": Service(
                "C5", [self.schemas[4]], service_flows.get("C5", (150, 150))[0]
            ),
            "C6": Service(
                "C6", [self.schemas[5]], service_flows.get("C6", (150, 150))[0]
            ),
            "C7": Service(
                "C7", [self.schemas[6]], service_flows.get("C7", (150, 150))[0]
            ),
            "AggStream": Service(
                "AggStream", self.schemas, service_flows.get("AggStream", (150, 150))[0]
            ),
            "SlowLane": Service(
                "SlowLane", self.schemas, service_flows.get("SlowLane", (150, 150))[0]
            ),
            "CDIS1": Service(
                "CDIS1", self.schemas, service_flows.get("CDIS1", (150, 150))[0]
            ),
            "CDIS2": Service(
                "CDIS2", self.schemas, service_flows.get("CDIS2", (150, 150))[0]
            ),
            "CDIS3": Service(
                "CDIS3", self.schemas, service_flows.get("CDIS3", (150, 150))[0]
            ),
        }

        self.bungee_clusters = {
            f"Bungee{i}": Service(
                f"Bungee{i}",
                self.schemas,
                service_flows.get(f"Bungee{i}", (150, 150))[0],
            )
            for i in range(1, 10)
        }
        self.services.update(self.bungee_clusters)

        for service_name, (in_flow, out_flow) in service_flows.items():
            service = self.services[service_name]
            for schema in service.supported_schemas:
                service.incoming_flow[schema] = in_flow // len(
                    service.supported_schemas
                )
                service.outgoing_flow[schema] = out_flow // len(
                    service.supported_schemas
                )

        self.graph = {
            "C1": ["AggStream", "SlowLane"],
            "C2": ["AggStream", "SlowLane"],
            "C3": ["AggStream", "SlowLane"],
            "C4": ["AggStream", "SlowLane"],
            "C5": ["AggStream", "SlowLane"],
            "C6": ["AggStream", "SlowLane"],
            "C7": ["AggStream", "SlowLane"],
            "AggStream": ["CDIS1", "CDIS2", "CDIS3"],
            "SlowLane": ["AggStream"],
            "CDIS1": list(self.bungee_clusters.keys()),
            "CDIS2": list(self.bungee_clusters.keys()),
            "CDIS3": list(self.bungee_clusters.keys()),
        }

    def topological_sort_with_loops(self):
        in_degree = {node: 0 for node in self.services}
        for node in self.graph:
            for neighbor in self.graph[node]:
                in_degree[neighbor] += 1

        queue = deque([node for node in self.services if in_degree[node] == 0])
        result = []

        while queue:
            node = queue.popleft()
            result.append(node)

            for neighbor in self.graph.get(node, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        remaining = [node for node in self.services if node not in result]
        result.extend(remaining)

        return result

    def propagate_flow(self, sorted_services):
        print("\nPropagating flow through the pipeline:")
        processed = set()
        iteration = 0
        max_iterations = len(self.services) * 2

        while len(processed) < len(self.services) and iteration < max_iterations:
            iteration += 1
            pushback_factors = {
                schema: self.calculate_pushback_factor_for_schema(schema)
                if self.is_bungee_overloaded_for_schema(schema) else 1.0
                for schema in self.schemas
            }

            for service_name in sorted_services:
                if service_name in processed and not self.has_new_input(service_name):
                    continue

                service = self.services[service_name]
                previous_outgoing = service.outgoing_flow.copy()

                # Apply pushback to incoming flow if Bungee is overloaded for specific schemas
                if not service_name.startswith("Bungee"):
                    for schema in service.incoming_flow:
                        service.incoming_flow[schema] *= pushback_factors[schema]

                service.process_flow()
                processed.add(service_name)

                if self.flow_changed(previous_outgoing, service.outgoing_flow):
                    self.propagate_to_downstream(service_name)
                    processed.difference_update(set(self.graph.get(service_name, [])))

            print(f"Iteration {iteration} completed")

    def has_new_input(self, service_name):
        service = self.services[service_name]
        return any(
            service.incoming_flow[schema] > service.outgoing_flow[schema]
            for schema in service.supported_schemas
        )

    def flow_changed(self, previous, current):
        return any(previous[schema] != current[schema] for schema in previous)

    def propagate_to_downstream(self, service_name):
        service = self.services[service_name]
        downstream_services = self.graph.get(service_name, [])
        if downstream_services:
            num_downstream = len(downstream_services)
            for schema in service.supported_schemas:
                outgoing_per_downstream = service.outgoing_flow[schema] / num_downstream
                for downstream in downstream_services:
                    downstream_service = self.services[downstream]
                    if schema in downstream_service.supported_schemas:
                        downstream_service.incoming_flow[
                            schema
                        ] += outgoing_per_downstream
                        print(f" Propagating from {service_name} to {downstream}")
                        print(f" {schema}: {outgoing_per_downstream}")

    def assess_service_status(self):
        for service in self.services.values():
            total_incoming = sum(service.incoming_flow.values())
            if total_incoming > service.current_capacity:
                service.status = ServiceStatus.OVERLOADED
            elif total_incoming < service.current_capacity * 0.5:
                service.status = ServiceStatus.UNDERUTILIZED
            else:
                service.status = ServiceStatus.NORMAL

    def determine_service_actions(self):
        bungee_at_capacity = any(
            sum(bungee.incoming_flow.values()) > bungee.current_capacity * 0.9
            for bungee in self.bungee_clusters.values()
        )

        for service in self.services.values():
            if service.name.startswith("Bungee"):
                continue
            if service.status == ServiceStatus.OVERLOADED:
                service.action = ServiceAction.SPEEDUP
            elif (
                service.status == ServiceStatus.UNDERUTILIZED and not bungee_at_capacity
            ):
                service.action = ServiceAction.SLOWDOWN
            else:
                service.action = ServiceAction.NO_ACTION

    def run_cycle(self, input_flows, service_flows):
        print("\n--- New Cycle ---")
        print(f"Input Flows: {input_flows}")
        print(f"Service Flows: {service_flows}")

        for service_name, (in_flow, out_flow) in service_flows.items():
            service = self.services[service_name]
            num_schemas = len(service.supported_schemas)
            for schema in service.supported_schemas:
                service.incoming_flow[schema] = in_flow // num_schemas
                service.outgoing_flow[schema] = out_flow // num_schemas

        for i, flow in enumerate(input_flows, start=1):
            self.services[f"C{i}"].incoming_flow = {self.schemas[i - 1]: flow}

        sorted_services = self.topological_sort_with_loops()
        self.propagate_flow(sorted_services)

        self.assess_service_status()
        self.determine_service_actions()

        print_service_table(self.services)


def get_user_input():
    input_flows = []
    print("Enter input flow for each S (1-7). Press Enter to use default value of 100.")
    for i in range(1, 8):
        flow = input(f"Enter input flow for S{i}: ")
        if flow == "":
            input_flows.append(100)
        else:
            input_flows.append(int(flow))

    service_flows = {}
    print("\nEnter input and output flows for each service.")
    print(
        "Format: input_flow,output_flow (e.g., 100,90). Press Enter to use default values."
    )

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
        "CDIS1",
        "CDIS2",
        "CDIS3",
    ]
    services.extend([f"Bungee{i}" for i in range(1, 10)])

    for service in services:
        flows = input(f"Enter flows for {service}: ")
        if flows == "":
            service_flows[service] = (150, 150)
        else:
            in_flow, out_flow = map(int, flows.split(","))
            service_flows[service] = (in_flow, out_flow)

    return input_flows, service_flows


def main():
    input_flows, service_flows = get_user_input()
    pipeline = Pipeline(service_flows)
    pipeline.run_cycle(input_flows, service_flows)


if __name__ == "__main__":
    main()
