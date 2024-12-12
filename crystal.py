import random
from enum import Enum
from collections import defaultdict, deque

from output import print_service_table_only_ips, print_dependency_graph


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
    def __init__(self, name, supported_schemas, schema_capacities):
        self.name = name
        self.supported_schemas = supported_schemas
        self.schema_capacities = schema_capacities
        self.incoming_flow = {schema: 0 for schema in supported_schemas}
        self.outgoing_flow = {schema: 0 for schema in supported_schemas}
        self.current_capacity = {
            schema: schema_capacities[schema][1] for schema in supported_schemas
        }
        self.allocated_capacity = self.allocate_capacity()
        self.status = ServiceStatus.NORMAL
        self.action = ServiceAction.NO_ACTION
        # will be determined by the actual service not by this throttling algo
        self.visited = {schema: False for schema in supported_schemas}
        self.reduction_factors = {schema: 0 for schema in supported_schemas}

    def allocate_capacity(self):
        allocated = {}
        remaining_capacity = sum(self.current_capacity.values())

        # First pass: Allocate based on incoming flow
        for schema in self.supported_schemas:
            needed = min(self.incoming_flow[schema], self.current_capacity[schema])
            allocated[schema] = needed
            remaining_capacity -= needed

        # Second pass: Distribute remaining capacity proportionally if needed
        if remaining_capacity > 0:
            total_unfulfilled = sum(
                max(0, self.incoming_flow[s] - allocated[s])
                for s in self.supported_schemas
            )
            if total_unfulfilled > 0:
                for schema in self.supported_schemas:
                    unfulfilled = max(0, self.incoming_flow[schema] - allocated[schema])
                    extra = int(remaining_capacity * (unfulfilled / total_unfulfilled))
                    allocated[schema] += extra
                    remaining_capacity -= extra

        return allocated

    def apply_backpressure(self, schema, reduction_percentage):
        if (
            not self.visited[schema]
            or reduction_percentage > self.reduction_factors[schema]
        ):
            self.visited[schema] = True
            self.reduction_factors[schema] = reduction_percentage

            original_flow = self.incoming_flow[schema]
            new_flow = max(0, original_flow * (1 - reduction_percentage))
            actual_reduction_percentage = (
                (original_flow - new_flow) / original_flow if original_flow > 0 else 0
            )

            self.incoming_flow[schema] = new_flow
            print(
                f"  Applying {actual_reduction_percentage:.2%} backpressure to {self.name} for {schema}"
            )
            print(
                f"    Reduced {self.name} {schema} input from {original_flow:.2f} to {new_flow:.2f}"
            )

            return actual_reduction_percentage
        return 0

    def reset_backpressure_state(self):
        self.visited = {schema: False for schema in self.supported_schemas}
        self.reduction_factors = {schema: 0 for schema in self.supported_schemas}

    def reallocate_capacity_across_schemas(self):
        self.allocated_capacity = self.allocate_capacity()
        total_incoming = sum(self.incoming_flow.values())
        total_allocated = sum(self.allocated_capacity.values())

        if total_incoming <= total_allocated:
            return

        sorted_schemas = sorted(
            self.supported_schemas, key=lambda s: self.incoming_flow[s], reverse=True
        )

        excess_capacity = {
            s: max(0, self.allocated_capacity[s] - self.incoming_flow[s])
            for s in self.supported_schemas
        }
        total_excess = sum(excess_capacity.values())

        for schema in sorted_schemas:
            if self.incoming_flow[schema] > self.allocated_capacity[schema]:
                needed_capacity = (
                    self.incoming_flow[schema] - self.allocated_capacity[schema]
                )
                reallocated = min(needed_capacity, total_excess)
                self.allocated_capacity[schema] += reallocated
                total_excess -= reallocated

        if total_excess > 0:
            total_deficit = sum(
                max(0, self.incoming_flow[s] - self.allocated_capacity[s])
                for s in self.supported_schemas
            )
            for schema in sorted_schemas:
                if self.incoming_flow[schema] > self.allocated_capacity[schema]:
                    deficit = (
                        self.incoming_flow[schema] - self.allocated_capacity[schema]
                    )
                    share = deficit / total_deficit if total_deficit > 0 else 0
                    additional = min(
                        int(total_excess * share),
                        self.current_capacity[schema] - self.allocated_capacity[schema],
                    )
                    self.allocated_capacity[schema] += additional

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

    def is_overloaded(self):
        return any(
            self.incoming_flow[s] > self.current_capacity[s]
            for s in self.supported_schemas
        )

    def is_underutilized(self):
        return all(
            self.incoming_flow[s] < 0.5 * self.current_capacity[s]
            for s in self.supported_schemas
        )


class Pipeline:
    def __init__(self, service_flows, schema_capacities, graph, schema_priorities):
        self.schemas = {
            name: Schema(name, priority) for name, priority in schema_priorities.items()
        }

        self.services = {}
        for service_name, flows in service_flows.items():
            service_schema_capacities = schema_capacities.get(service_name, {})

            supported_schemas = [
                self.schemas[schema_name] for schema_name in flows.keys()
            ]
            schema_obj_capacities = {
                self.schemas[schema_name]: caps
                for schema_name, caps in service_schema_capacities.items()
            }

            self.services[service_name] = Service(
                service_name, supported_schemas, schema_obj_capacities
            )
            for schema_name, (in_flow, out_flow) in flows.items():
                schema = self.schemas[schema_name]
                self.services[service_name].incoming_flow[schema] = in_flow
                self.services[service_name].outgoing_flow[schema] = out_flow

        self.graph = graph

    def is_bungee_overloaded_for_schema(self, schema):
        return any(
            service.incoming_flow[schema] > service.allocated_capacity[schema]
            for service in self.services.values()
            if service.name.startswith("Bungee")
        )

    def propagate_slowdown(self, service_name):
        upstream_services = [
            s for s, deps in self.graph.items() if service_name in deps
        ]
        for upstream in upstream_services:
            service = self.services[upstream]
            if service.status != ServiceStatus.OVERLOADED:
                service.status = ServiceStatus.OVERLOADED
                service.action = ServiceAction.SLOWDOWN
                self.propagate_slowdown(upstream)

    def determine_service_actions(self):
        for service in self.services.values():
            total_incoming = sum(service.incoming_flow.values())
            if total_incoming > service.current_capacity:
                service.status = ServiceStatus.OVERLOADED
                service.action = ServiceAction.SLOWDOWN
                self.propagate_slowdown(service.name)
            elif total_incoming < service.current_capacity * 0.5:
                service.status = ServiceStatus.UNDERUTILIZED
                service.action = ServiceAction.SPEEDUP
            else:
                service.status = ServiceStatus.NORMAL
                service.action = ServiceAction.NO_ACTION

    def print_overload_dependencies_dfs_way(self):
        print("\nOverload Dependencies:")
        for service_name, service in self.services.items():
            if service.status == ServiceStatus.OVERLOADED:
                for schema in service.supported_schemas:
                    if (
                        service.incoming_flow[schema]
                        > service.allocated_capacity[schema]
                    ):
                        paths = self.find_all_paths_to_service_dfs(schema, service_name)
                        if paths:
                            print(f"For {schema.name} to {service_name}:")
                            for path in paths:
                                print(f"  {' -> '.join(path)}")

    def find_all_paths_to_service_dfs(self, schema, target_service):
        def dfs(current, path, visited):
            if current == target_service:
                paths.append(path + [current])
                return
            visited.add(current)
            for next_service in self.graph.get(current, []):
                if next_service not in visited:
                    dfs(next_service, path + [current], visited.copy())

        paths = []
        start_services = [f"C{schema.name[1]}"]
        for start in start_services:
            dfs(start, [schema.name], set())
        return paths

    def print_overload_dependencies(self):
        print("\nOverload Dependencies:")
        sorted_services = self.topological_sort_with_loops()
        service_to_schema = {f"C{i}": f"S{i}" for i in range(1, 8)}

        for service_name in sorted_services:
            service = self.services[service_name]
            if service.status == ServiceStatus.OVERLOADED:
                for schema in service.supported_schemas:
                    if (
                        service.incoming_flow[schema]
                        > service.allocated_capacity[schema]
                    ):
                        path = self.find_path_to_service_topological(
                            schema, service_name, sorted_services, service_to_schema
                        )
                        if path:
                            print(f"For {schema.name}: {' -> '.join(path)}")

    def find_path_to_service_topological(
        self, schema, target_service, sorted_services, service_to_schema
    ):
        path = []
        found_start = False
        for service in sorted_services:
            if (
                service == f"C{schema.name[1]}"
                or service_to_schema.get(service) == schema.name
            ):
                found_start = True
                path.append(schema.name)

            if found_start:
                path.append(service)

            if service == target_service:
                return path
        return None

    def calculate_pushback_factor_for_schema(self, schema):
        bungee_services = [
            s for s in self.services.values() if s.name.startswith("Bungee")
        ]
        total_incoming = sum(s.incoming_flow[schema] for s in bungee_services)
        total_capacity = sum(s.allocated_capacity[schema] for s in bungee_services)
        return min(1.0, total_capacity / total_incoming) if total_incoming > 0 else 1.0

    def tarjan_scc(self):
        index = 0
        stack = []
        on_stack = set()
        indices = {}
        lowlinks = {}
        sccs = []

        def strongconnect(v):
            nonlocal index
            indices[v] = index
            lowlinks[v] = index
            index += 1
            stack.append(v)
            on_stack.add(v)

            for w in self.graph.get(v, []):
                if w not in indices:
                    strongconnect(w)
                    lowlinks[v] = min(lowlinks[v], lowlinks[w])
                elif w in on_stack:
                    lowlinks[v] = min(lowlinks[v], indices[w])

            if lowlinks[v] == indices[v]:
                scc = []
                while True:
                    w = stack.pop()
                    on_stack.remove(w)
                    scc.append(w)
                    if w == v:
                        break
                sccs.append(scc)

        for v in self.services:
            if v not in indices:
                strongconnect(v)
        return sccs

    def topological_sort_with_loops(self):
        sccs = self.tarjan_scc()
        scc_graph = defaultdict(list)
        scc_map = {}

        # Create a graph of SCCs
        for i, scc in enumerate(sccs):
            for node in scc:
                scc_map[node] = i
            for node in scc:
                for neighbor in self.graph.get(node, []):
                    if scc_map[neighbor] != i:
                        scc_graph[i].append(scc_map[neighbor])

        # Perform topological sort on the SCC graph
        in_degree = defaultdict(int)
        for scc in scc_graph:
            for neighbor in scc_graph[scc]:
                in_degree[neighbor] += 1

        queue = deque([scc for scc in range(len(sccs)) if in_degree[scc] == 0])
        result = []

        while queue:
            scc = queue.popleft()
            result.extend(sccs[scc])
            for neighbor in scc_graph[scc]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        return result

    def propagate_flow(self, sorted_services):
        print("\nPropagating flow through the pipeline:")
        processed = set()
        iteration = 0
        max_iterations = len(self.services) * 2

        while len(processed) < len(self.services) and iteration < max_iterations:
            iteration += 1

            for service_name in sorted_services:
                if service_name in processed and not self.has_new_input(service_name):
                    continue

                service = self.services[service_name]
                previous_outgoing = service.outgoing_flow.copy()

                service.process_flow()
                processed.add(service_name)

                if self.flow_changed(previous_outgoing, service.outgoing_flow):
                    self.propagate_to_downstream(service_name)
                    # Remove downstream services from processed set to reprocess them
                    processed.difference_update(set(self.graph.get(service_name, [])))

            print(f"Iteration {iteration} completed")

        self.determine_service_actions()

    def has_new_input(self, service_name):
        service = self.services[service_name]
        return any(
            service.incoming_flow[schema] > service.outgoing_flow[schema]
            for schema in service.supported_schemas
        )

    def flow_changed(self, previous, current):
        return any(previous[schema] != current[schema] for schema in previous)

    def apply_backpressure(
        self, service_name, schema, reduction_percentage, visited=None
    ):
        if visited is None:
            visited = set()

        if service_name in visited:
            return

        visited.add(service_name)
        service = self.services[service_name]
        if schema in service.supported_schemas:
            original_flow = service.incoming_flow[schema]
            new_flow = max(0, original_flow * (1 - reduction_percentage))
            actual_reduction_percentage = (
                (original_flow - new_flow) / original_flow if original_flow > 0 else 0
            )
            print(
                f"  Applying {actual_reduction_percentage:.2%} backpressure to {service_name} for {schema}"
            )
            service.incoming_flow[schema] = new_flow
            print(
                f"    Reduced {service_name} {schema} input from {original_flow:.2f} to {new_flow:.2f}"
            )
            upstream_services = [
                s for s, deps in self.graph.items() if service_name in deps
            ]
            for upstream in upstream_services:
                self.apply_backpressure(
                    upstream, schema, actual_reduction_percentage, visited.copy()
                )

    def resolve_overloads_by_backprop(self):
        iteration = 0
        max_iterations = len(self.services) * 2
        changes_made = True

        while changes_made and iteration < max_iterations:
            changes_made = False
            iteration += 1
            print(f"\nIteration {iteration}")
            overloaded = self.calculate_overloads()
            if not overloaded:
                print("No overloads detected. Ending resolution.")
                break

            # Reset backpressure state for all services
            for service in self.services.values():
                service.reset_backpressure_state()

            for service_name, schema_overloads in overloaded.items():
                for schema, overload_percentage in schema_overloads.items():
                    self.propagate_backpressure(
                        service_name, schema, overload_percentage
                    )
                    changes_made = True

            # Print the current state after each iteration
            print("\nCurrent state after iteration:")
            print_service_table_only_ips(self.services)

    def propagate_backpressure(self, service_name, schema, reduction_percentage):
        service = self.services[service_name]
        actual_reduction = service.apply_backpressure(schema, reduction_percentage)

        if actual_reduction > 0:
            # Propagate backpressure upstream
            upstream_services = [
                s for s, deps in self.graph.items() if service_name in deps
            ]
            for upstream in upstream_services:
                self.propagate_backpressure(upstream, schema, actual_reduction)

    def calculate_overloads(self):
        overloaded = {}
        for service_name, service in self.services.items():
            service_overloads = {}
            for schema in service.supported_schemas:
                if service.incoming_flow[schema] > service.allocated_capacity[schema]:
                    service.reallocate_capacity_across_schemas()
                    overload_percentage = (
                        service.incoming_flow[schema]
                        - service.allocated_capacity[schema]
                    ) / service.incoming_flow[schema]
                    service_overloads[schema] = overload_percentage
            if service_overloads:
                overloaded[service_name] = service_overloads
        return overloaded

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

    def resolve_overloads(self):
        print("\nResolving overloads in the pipeline:")
        self.resolve_overloads_by_backprop()
        # Final pass to update service statuses
        for service_name, service in self.services.items():
            service.reallocate_capacity_across_schemas()  # One final reallocation
            overloaded_schemas = [
                schema
                for schema in service.supported_schemas
                if service.incoming_flow[schema] > service.allocated_capacity[schema]
            ]
            if overloaded_schemas:
                service.status = ServiceStatus.OVERLOADED
                service.action = ServiceAction.SLOWDOWN
            elif all(
                service.incoming_flow[schema] < 0.5 * service.allocated_capacity[schema]
                for schema in service.supported_schemas
            ):
                service.status = ServiceStatus.UNDERUTILIZED
                service.action = ServiceAction.SPEEDUP
            else:
                service.status = ServiceStatus.NORMAL
                service.action = ServiceAction.NO_ACTION
        print("\nFinal state after resolution:")
        print_service_table_only_ips(self.services)

    def run_cycle(self, service_flows):
        print("\n---- New Cycle ----")
        print_service_table_only_ips(self.services)
        for service_name, flows in service_flows.items():
            service = self.services[service_name]
            for schema_name, (in_flow, out_flow) in flows.items():
                schema = self.schemas[schema_name]
                service.incoming_flow[schema] = in_flow
                service.outgoing_flow[schema] = out_flow
        self.print_overload_dependencies_dfs_way()
        self.resolve_overloads()
        self.assess_service_status()

        print("\n---- Crystallized ----")
        print_service_table_only_ips(self.services)

    def assess_service_status(self):
        for service in self.services.values():
            if service.is_overloaded():
                service.status = ServiceStatus.OVERLOADED
            elif service.is_underutilized():
                service.status = ServiceStatus.UNDERUTILIZED
            else:
                service.status = ServiceStatus.NORMAL


def main(service_flows, schema_capacities, graph, schema_priorities):
    pipeline = Pipeline(
        service_flows=service_flows,
        schema_capacities=schema_capacities,
        graph=graph,
        schema_priorities=schema_priorities,  # Required parameter
    )
    print("\n ---- Pipeline Graph ---- ")
    pipeline.run_cycle(service_flows)
    return pipeline


if __name__ == "__main__":
    main()


## Valency , Equilibrium , Crystallized.