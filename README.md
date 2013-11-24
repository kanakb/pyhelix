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
from pyhelix import participant
from pyhelix import statemodel

# Define a participant
p = participant.Participant('test-cluster', 'myhost', 12120, 'zkhost:2181')

# Register a state model factory
p.register_state_model_fty('MasterSlave', statemodel.MockStateModelFactory())

# Connect
p.connect()

# Disconnect
p.disconnect()
```
