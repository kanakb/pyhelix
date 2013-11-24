def get_empty_znode(node_id):
    """
    Get an empty ZNode with headers filled.

    Args:
        node_id: String that identifies the ZNode

    Returns:
        A dictionary representing a ZNRecord
    """
    return {'id': node_id, 'simpleFields': {}, 'listFields': {}, 'mapFields': {}}
