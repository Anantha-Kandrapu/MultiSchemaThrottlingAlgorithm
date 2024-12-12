from tabulate import tabulate


def print_dependency_graph(pipeline):
    headers = ["Service", "Supported Schemas", "Downstream Services"]
    table_data = []
    for service_name, service in pipeline.services.items():
        supported_schemas = ", ".join(
            [schema.name for schema in service.supported_schemas]
        )
        downstream = ", ".join(pipeline.graph.get(service_name, []))
        table_data.append([service_name, supported_schemas, downstream])
    print("\nInitial Dependency Graph:")
    print(tabulate(table_data, headers=headers, tablefmt="grid"))


def formatted_print_service_table(services):
    headers = [
        "ServiceName",
        "Status",
        "Action",
        "Incoming Flow",
        "Outgoing Flow",
        "Current Capacity",
        "Allocated Capacity",
    ]
    table_data = []

    for name, service in services.items():
        incoming_flow = format_dict(service.incoming_flow)
        outgoing_flow = format_dict(service.outgoing_flow)
        allocated_capacity = format_dict(service.allocated_capacity)

        table_data.append(
            [
                name,
                service.status.value,
                service.action.value,
                incoming_flow,
                outgoing_flow,
                service.current_capacity,
                allocated_capacity,
            ]
        )

    print("\nService Status Table:")
    print(
        tabulate(
            table_data,
            headers=headers,
            tablefmt="grid",
            maxcolwidths=[None, None, None, 30, 30, None, 30],
        )
    )


def format_dict(d):
    return (
        "{\n "
        + ",\n ".join([f"S{i+1}: {v:.2f}" for i, v in enumerate(d.values())])
        + "\n}"
    )


def print_service_table_only_ips(services):
    headers = ["ServiceName", "Status", "Actions","Max Capacity"] + [f"S{i}:In/Cap" for i in range(1, 8)]
    table_data = []

    for service_name, service in services.items():
        row = [
            service_name,
            service.status.value,
            service.action.value,
            service.current_capacity,
        ]
        for schema in service.supported_schemas:
            incoming_flow = round(service.incoming_flow[schema], 2)
            capacity = round(service.allocated_capacity[schema], 2)
            row.append(f"{incoming_flow}/{capacity}")
        table_data.append(row)

    print(tabulate(table_data, headers=headers, tablefmt="grid"))


def print_service_table(services):
    headers = ["ServiceName", "Status", "Action", "Capacity"] + [
        f"S{i} In/Out" for i in range(1, 8)
    ]
    table_data = []

    for service_name, service in services.items():
        row = [
            service_name,
            service.status.value,
            service.action.value,
            service.current_capacity,
        ]
        for schema in service.supported_schemas:
            incoming_flow = round(service.incoming_flow[schema], 2)
            outgoing_flow = round(service.outgoing_flow[schema], 2)
            row.append(f"{incoming_flow}/{outgoing_flow}")
        table_data.append(row)

    print(tabulate(table_data, headers=headers, tablefmt="grid"))
