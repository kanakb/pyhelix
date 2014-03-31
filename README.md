pyhelix
-------

Python bindings for Apache Helix

Install:

```
sudo easy_install pyhelix

OR

sudo pip install pyhelix
```

Start a participant:

```python
# Imports
import pyhelix.participant as participant
import pyhelix.examples.dummy.dummy_statemodel as statemodel

# Define a participant
p = participant.Participant('test-cluster', 'myhost', 12120, 'zkhost:2181')

# Register a state model factory
p.register_state_model_fty('MasterSlave', statemodel.DummyStateModelFactory())

# Connect
p.connect()

# Disconnect
p.disconnect()
```
[![Build Status](https://travis-ci.org/kanakb/pyhelix.png?branch=master)](https://travis-ci.org/kanakb/pyhelix)
