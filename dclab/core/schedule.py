from itertools import compress
import tabulate


def add_node_to_schedule(node, schedule):
    if node in schedule:
        schedule.remove(node)
    schedule.insert(0, node)


def add_node_and_parents_to_schedule(node, schedule, node_list):
    # Add node
    add_node_to_schedule(node, schedule)
    node.assemble_parent_nodes()
    # Add parents
    for parents in node.parent_nodes:
        add_node_and_parents_to_schedule(parents, schedule, node_list)


def get_tail_nodes(node_list):
    has_no_children = [True] * len(node_list)

    for i in range(len(node_list)):
        current_node = node_list[i]

        for j in range(len(node_list)):
            if current_node in node_list[j].parent_nodes:
                has_no_children[i] = False

    end_point_list = list(compress(node_list, has_no_children))
    return end_point_list


def extract_schedule_from_node_list(node_list):
    schedule = list()
    end_point_list = get_tail_nodes(node_list)

    for end_point in end_point_list:
        add_node_and_parents_to_schedule(end_point, schedule, node_list)

    return schedule


def switch_places_in_schedule(schedule, i, j):
    schedule[i], schedule[j] = schedule[j], schedule[i]


def schedule_to_string(schedule):
    list_to_print = list()
    for i, node in enumerate(schedule):
        list_to_print.append(list([i, type(node).__name__, node.name]))
    schedule_string = tabulate.tabulate(list_to_print, headers=["ID", "Type", "Name"])
    return schedule_string


def schedule_consistency_check(schedule):
    for i, node in enumerate(schedule):
        node_parents = node.parent_nodes
        for parent_node in node_parents:
            schedule_index = schedule.index(parent_node)
            if not schedule_index < i:
                return False
    return True
