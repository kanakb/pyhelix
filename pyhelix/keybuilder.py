def propertykey(ephemeral=False, sequential=False, merge_on_update=False,
    update_only_on_exists=False):
    def decorator(func):
        def hidden_func(*args, **kwargs):
            prop = {'ephemeral': ephemeral,
                    'sequential': sequential,
                    'merge_on_update': merge_on_update,
                    'update_only_on_exists': update_only_on_exists}
            prop['path'] = func(*args, **kwargs)
            return prop
        return hidden_func
    return decorator

class KeyBuilder(object):
    """
    A collection of functions to get ZooKeeper metadata
    """
    def __init__(self, cluster_id):
        self._cluster_id = cluster_id

    @propertykey()
    def cluster_config(self):
        return '/{0}/CONFIGS/CLUSTER/{0}'.format(self._cluster_id)

    @propertykey()
    def external_views(self):
        return '/{0}/EXTERNALVIEW'.format(self._cluster_id)

    @propertykey()
    def external_view(self, resource_id):
        return '/{0}/EXTERNALVIEW/{1}'.format(self._cluster_id, resource_id)

    @propertykey()
    def ideal_states(self, resource_id):
        return '/{0}/IDEALSTATES'.format(self._cluster_id)

    @propertykey()
    def ideal_state(self, resource_id):
        return '/{0}/IDEALSTATES/{1}'.format(self._cluster_id, resource_id)

    @propertykey()
    def participant_configs(self):
        return '/{0}/CONFIGS/PARTICIPANT'.format(self._cluster_id)

    @propertykey()
    def participant_config(self, participant_id):
        return '/{0}/CONFIGS/PARTICIPANT/{1}'.format(self._cluster_id, participant_id)

    @propertykey()
    def live_instances(self):
        return '/{0}/LIVEINSTANCES'.format(self._cluster_id)

    @propertykey()
    def instance(self, participant_id):
        return '/{0}/INSTANCES/{1}'.format(self._cluster_id, participant_id)

    @propertykey(ephemeral=True)
    def live_instance(self, participant_id):
        return '/{0}/LIVEINSTANCES/{1}'.format(self._cluster_id, participant_id)

    @propertykey()
    def current_states(self, participant_id, session_id=None):
        path = '/{0}/INSTANCES/{1}/CURRENTSTATES'.format(self._cluster_id, participant_id,
            session_id)
        if session_id:
            path += '/{0}'.format(session_id)
        return path

    @propertykey(merge_on_update=True)
    def current_state(self, participant_id, session_id, resource_id):
        return '/{0}/INSTANCES/{1}/CURRENTSTATES/{2}/{3}'.format(self._cluster_id, participant_id,
            session_id, resource_id)

    @propertykey(merge_on_update=True)
    def errors(self, participant_id):
        return '/{0}/INSTANCES/{1}/ERRORS'.format(self._cluster_id, participant_id)

    @propertykey(merge_on_update=True)
    def health_report(self, participant_id):
        return '/{0}/INSTANCES/{1}/HEALTHREPORT'.format(self._cluster_id, participant_id)

    @propertykey()
    def status_updates(self, participant_id):
        return '/{0}/INSTANCES/{1}/STATUSUPDATES'.format(self._cluster_id, participant_id)

    @propertykey()
    def state_models(self):
        return '/{0}/STATEMODELDEFS'.format(self._cluster_id)

    @propertykey()
    def state_model(self, resource_id):
        return '/{0}/STATEMODELDEFS/{1}'.format(self._cluster_id, resource_id)

    @propertykey()
    def messages(self, participant_id):
        return '/{0}/INSTANCES/{1}/MESSAGES'.format(self._cluster_id, participant_id)

    @propertykey(merge_on_update=True, update_only_on_exists=True)
    def message(self, participant_id, message_id):
        return '/{0}/INSTANCES/{1}/MESSAGES/{2}'.format(self._cluster_id, participant_id,
            message_id)
