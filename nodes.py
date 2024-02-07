from kubernetes import client, config
import pandas as pd
from datetime import datetime

# Load kube-config
config.load_kube_config()

# Initialize clients
v1 = client.CoreV1Api()

# Function to parse resource quantities
def parse_quantity(q):
    if q.endswith('Ki'):
        return float(q[:-2]) * 1024
    elif q.endswith('Mi'):
        return float(q[:-2]) * 1024**2
    elif q.endswith('Gi'):
        return float(q[:-2]) * 1024**3
    elif q.endswith('Ti'):
        return float(q[:-2]) * 1024**4
    elif q.endswith('m'):
        return float(q[:-1]) / 1000
    else:
        return float(q)

# Aggregate resource requests and allocatable by node
node_resources = {}
pods = v1.list_pod_for_all_namespaces(watch=False)
nodes = v1.list_node()
current_time = datetime.now()

# Initialize node data with a timestamp
for node in nodes.items:
    allocatable = node.status.allocatable
    node_resources[node.metadata.name] = {
        'timestamp': current_time,
        'pod_count': 0,
        'cpu_request': 0,
        'memory_request': 0,
        'allocatable_cpu': parse_quantity(allocatable['cpu']),
        'allocatable_memory': parse_quantity(allocatable['memory']),
    }

# Sum resource requests for pods on each node
for pod in pods.items:
    node_name = pod.spec.node_name
    if node_name and node_name in node_resources:
        node_resources[node_name]['pod_count'] += 1
        for container in pod.spec.containers:
            requests = container.resources.requests or {}
            node_resources[node_name]['cpu_request'] += parse_quantity(requests.get('cpu', '0'))
            node_resources[node_name]['memory_request'] += parse_quantity(requests.get('memory', '0'))

# Calculate percentage of memory usage
for node, resources in node_resources.items():
    if resources['allocatable_memory'] > 0:
        resources['memory_usage_pct'] = (resources['memory_request'] / resources['allocatable_memory']) * 100
    else:
        resources['memory_usage_pct'] = 0

# Create DataFrame
df = pd.DataFrame.from_dict(node_resources, orient='index')
df.reset_index(inplace=True)
df.rename(columns={'index': 'Node'}, inplace=True)

print(df)
