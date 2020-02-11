import uuid
import logging

from redgraph.graphql import schema
from redgraph import graph, anchors


logger = logging.getLogger("redgraph.graphql")


class IDException(Exception):
    pass


def resolver(type, field):
    def register(func):
        if type == "Query":
            fields = schema.query_type.fields
        elif type == "Mutation":
            fields = schema.mutation_type.fields
        else:
            fields = schema.get_type(type).fields
        fields[field] = func

        return func

    return register


@resolver("Query", "node")
@resolver("Query", "edge")
@resolver("Query", "entity")
def id(root, info, id):
    try:
        uid = uuid.UUID(hex=id)
        return uid
    except Exception as e:
        logger.exception(e)
        raise IDException("Must provide uuid as id input field to node query")


@resolver("Query", "anchor")
def anchor(root, info, name):
    return name


@resolver("Node", "id")
@resolver("Edge", "id")
def node_id(id, info):
    return id.hex


@resolver("Node", "entity")
@resolver("Edge", "entity")
def node_entity(id, info):
    return id


@resolver("Node", "relationships")
async def node_relationships(id, info, outgoing, incoming):
    relationships = []
    if outgoing:
        outgoing_relationships = await graph.sp(info.context["redis"], id)
        relationships += outgoing_relationships
    if incoming:
        incoming_relationships = await graph.op(info.context["redis"], id)
        relationships += incoming_relationships

    return relationships


@resolver("Node", "adjacent")
async def node_adjacent(id, info, outgoing, incoming):
    adjacency = []
    if outgoing:
        outgoing_adjacency = await graph.so(info.context["redis"], id)
        adjacency += outgoing_adjacency
    if incoming:
        incoming_adjacency = await graph.os(info.context["redis"], id)
        adjacency += incoming_adjacency

    return adjacency


@resolver("Edge", "nodes")
async def node_adjacent(id, info, subjects, objects):
    nodes = []
    if subject:
        subject_nodes = await graph.ps(info.context["redis"], id)
        nodes += subject_nodes
    if object:
        object_nodes = await graph.po(info.context["redis"], id)
        nodes += object_nodes

    return nodes


@resolver("Anchor", "name")
def anchor_name(name, info):
    return name


@resolver("Anchor", "entities")
async def anchor_entities(name, info):
    return await anchors.members(info.context["redis"], name)


@resolver("Anchor", "membership")
async def anchor_membership(name, info, entities):
    return [await anchors.is_member(info.context["redis"], name, id) for id in entities]


@resolver("Anchor", "intersection")
async def anchor_intersection(name, info, intersect):
    return await anchors.intersection(info.context["redis"], name, *intersect)


@resolver("Anchor", "union")
async def anchor_union(name, info, unite):
    return await anchors.union(info.context["redis"], name, *unite)


@resolver("Anchor", "diff")
async def anchor_diff(name, info, less):
    return await anchors.diff(info.context["redis"], name, *less)
