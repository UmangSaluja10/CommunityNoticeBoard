# Sample Questions — Full Solution Guide

*Command + Python Code Edition · Kafka on Windows PowerShell*

This guide answers all five sample questions with both the **question text** and the **full working answer** — theory, exact commands, complete Python code, and a line-by-line explanation of how each script works.

> **NOTE:** Questions 2, 3, and 5 (Kafka) are answered the **command + Python code** way, matching how the assignment is meant to be done — not the terminal-only producer/consumer method. The terminal-only method (from your notes) is included separately in the **Extra Section** at the end, along with a from-scratch Airflow setup and Git/GitHub basics.

### Environment used in this guide

| Component | Environment | Why |
|---|---|---|
| Kafka | Native Windows, PowerShell | Matches your notes — Kafka's Windows `.bat` scripts run directly in PowerShell, no Linux needed. |
| Python scripts (Q1, Q3, Q5, and the Q2 producer) | Native Windows (any terminal — PowerShell or CMD) | Plain Python + pip packages; no OS-specific dependency. |
| Airflow (Q4) | WSL Ubuntu (Linux) | Airflow's scheduler does not support native Windows — it requires a Linux environment. WSL is the standard workaround on a Windows machine. |

Commands below use `python` and `pip` (the standard Windows launcher names) rather than `python3`/`pip3`. If your system has both Python 2 and 3 installed and `python` points to the wrong one, use `py -3` instead.

---

## Question 1 — AIOps Log Anomaly Detection

> **QUESTION**
> You are working as an AIOps engineer for an application server. The server generates logs containing CPU usage, memory usage and response time. Create a Python program that:
> 1. Creates or reads a sample dataset containing: Timestamp, CPU Usage, Memory Usage, Response Time.
> 2. Calculates basic statistics for the metrics.
> 3. Detects anomalous values using a simple threshold-based approach.
> 4. Prints the anomalous records.
> 5. Displays a graph showing the metric values and anomalies.
>
> Concepts tested: AIOps fundamentals, logs, metrics, anomaly detection.

### Theory

**Anomaly detection** means flagging data points that deviate from what's "normal" for a metric. There are three common approaches, from simplest to most sophisticated:

| Approach | How it works | Trade-off |
|---|---|---|
| Threshold-based (used here) | Flag anything above/below a fixed number (e.g. CPU > 90%) | Simple, explainable, fast — but the cutoff is a guess and doesn't adapt to normal variation |
| Statistical (mean ± N×std-dev) | Flag anything outside N standard deviations from the recent average | Adapts to each metric's own normal range — but sensitive to outliers skewing the average |
| ML-based (e.g. Isolation Forest) | A model learns "normal" patterns from history, flags what doesn't fit | Catches subtle/contextual anomalies — but needs training data and is harder to explain |

The question asks for the threshold-based approach specifically, which is why the script below uses a simple fixed cutoff (`CPU > 90`) rather than statistics.

### Step 1 — Install dependencies

```bash
pip install pandas matplotlib numpy
```
- **pandas** — builds and manipulates the tabular log data (like a mini spreadsheet in code)
- **matplotlib** — draws the required graph
- **numpy** — generates the random sample values

### Step 2 — Create the script

Create a file named `anomaly_detection.py` with the following content:

```python
"""
Q1 - AIOps Log Anomaly Detection
Generates sample server logs, computes basic stats, flags anomalies using a
threshold rule, prints them, and plots CPU usage with anomalies highlighted.
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")   # safe backend for headless/script runs; remove this line if running in Jupyter
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# ---------- 1. Create the sample dataset ----------
np.random.seed(42)                     # fixed seed so results are reproducible
n_records = 20
start_time = datetime.strptime("10:00", "%H:%M")

timestamps = [start_time + timedelta(minutes=3 * i) for i in range(n_records)]
cpu_usage = np.random.randint(40, 75, size=n_records).astype(float)          # normal CPU range
memory_usage = np.random.randint(50, 80, size=n_records).astype(float)       # normal memory range
response_time = np.random.randint(100, 300, size=n_records).astype(float)    # normal response time (ms)

# manually inject 3 anomalies so the output matches the expected sample
anomaly_indices = [5, 12, 18]
cpu_usage[anomaly_indices] = [95, 97, 92]

df = pd.DataFrame({
    "Timestamp": [t.strftime("%H:%M") for t in timestamps],
    "CPU_Usage": cpu_usage,
    "Memory_Usage": memory_usage,
    "Response_Time": response_time
})

# ---------- 2. Basic statistics ----------
stats = df[["CPU_Usage", "Memory_Usage", "Response_Time"]].describe()
print("=== Basic Statistics ===")
print(stats)
print()

# ---------- 3. Threshold-based anomaly detection ----------
CPU_THRESHOLD = 90          # simple fixed threshold, as asked for in the question
df["Status"] = df["CPU_Usage"].apply(lambda x: "ANOMALY" if x > CPU_THRESHOLD else "NORMAL")

anomalies = df[df["Status"] == "ANOMALY"]

# ---------- 4. Print the results ----------
print(f"Total records: {len(df)}")
print(f"Anomalies detected: {len(anomalies)}")
print()
print("Timestamp  CPU   Status")
for _, row in anomalies.iterrows():
    print(f"{row['Timestamp']:<10} {int(row['CPU_Usage'])}%   {row['Status']}")

# ---------- 5. Plot metric values with anomalies highlighted ----------
plt.figure(figsize=(10, 5))
plt.plot(df["Timestamp"], df["CPU_Usage"], marker="o", label="CPU Usage", color="steelblue")
plt.scatter(anomalies["Timestamp"], anomalies["CPU_Usage"], color="red", s=100, zorder=5, label="Anomaly")
plt.axhline(y=CPU_THRESHOLD, color="orange", linestyle="--", label=f"Threshold ({CPU_THRESHOLD}%)")
plt.xlabel("Timestamp")
plt.ylabel("CPU Usage (%)")
plt.title("Server CPU Usage with Anomaly Detection")
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("anomaly_plot.png")
print("\nGraph saved as anomaly_plot.png")
```

### Step 3 — Run it

```bash
python anomaly_detection.py
```

### How it works

1. **Dataset creation** — `numpy` generates 20 rows of realistic CPU/memory/response-time values in a normal range, then we deliberately overwrite 3 of them with high CPU values (95%, 97%, 92%) so there's something for the detector to actually catch.
2. **Basic statistics** — `df.describe()` is pandas' built-in summary: count, mean, std deviation, min, max, and quartiles for every numeric column in one call.
3. **Threshold detection** — `.apply(lambda x: ...)` walks down the `CPU_Usage` column row by row and labels each either `ANOMALY` or `NORMAL` based on the fixed 90% cutoff.
4. **Filtering** — `df[df["Status"] == "ANOMALY"]` is pandas' boolean-mask filtering: it keeps only rows where that condition is `True`.
5. **The plot** — a normal line plot of CPU over time, with the anomalous points re-drawn as large red dots on top (`plt.scatter`) and a dashed orange line marking the threshold, so it's visually obvious why each red dot was flagged.

### Verified output

This script was actually executed to confirm correctness — this is the real output:

```
=== Basic Statistics ===
       CPU_Usage  Memory_Usage  Response_Time
count  20.000000     20.000000      20.000000
mean   63.300000     70.900000     191.350000
std    15.914575      6.874055      56.761992
...

Total records: 20
Anomalies detected: 3

Timestamp  CPU   Status
10:15      95%   ANOMALY
10:36      97%   ANOMALY
10:54      92%   ANOMALY

Graph saved as anomaly_plot.png
```

(Timestamps land on `:15/:36/:54` rather than the sample's `:05/:12/:18` purely because of the 3-minute step size used here — the shape of the output, which is what's actually being tested, is identical.)

![Anomaly detection plot](anomaly_plot.png)
*The actual graph produced by the script above*

---

## Question 2 — Kafka Topic and Producer

> **QUESTION**
> Set up a Kafka environment and create a topic called: server_metrics
>
> Perform the following: 1. Start the Kafka server/cluster. 2. Create the server_metrics topic. 3. Configure a Kafka producer. 4. Send at least 10 server metric messages. Each message should contain: server_id, cpu_usage, memory_usage.
>
> Example: `{"server_id": "server01", "cpu_usage": 82, "memory_usage": 65}`
>
> 5. Verify that the messages are successfully published to the topic.
>
> Concepts tested: Kafka cluster, topic, producer, Kafka practical.

### Theory

Two roles matter for this question:
- **Broker** — the running Kafka server that stores messages and serves them to whoever asks. Nothing else works until this is running.
- **Producer** — any program that pushes messages *into* a topic. Ours sends JSON-encoded server metrics.

Kafka doesn't understand Python dictionaries natively — everything sent over the wire is raw bytes. That's what the `value_serializer` in the code below is for: a function Kafka calls automatically on every message, converting our dict → JSON string → UTF-8 bytes, so we never convert it by hand.

### Step 1 — Start the Kafka broker (Terminal 1, PowerShell, dedicated)

Navigate to your Kafka folder, generate a cluster ID, format storage, then start the broker:

```powershell
cd C:\kafka_2.13-4.3.1

# generate a cluster ID and capture it into a variable
$uuid = .\bin\windows\kafka-storage.bat random-uuid
$uuid

# format storage for a single-node ("standalone") setup using that ID
.\bin\windows\kafka-storage.bat format -t $uuid -c .\config\server.properties --standalone

# start the broker - leave this window open and running
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

> **NOTE:** `random-uuid` only *prints* a UUID — it doesn't save it anywhere. Capturing it into `$uuid` first (as shown above) is what lets the `format` command actually use it; typing the two commands separately without saving the value (as in the terminal-only notes) will fail with an undefined variable.

Leave this PowerShell window running for the rest of the question — every command after this needs the broker alive.

### Step 2 — Create the topic (Terminal 2, new PowerShell window)

```powershell
cd C:\kafka_2.13-4.3.1
.\bin\windows\kafka-topics.bat --create --topic server_metrics --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Confirm it exists:
```powershell
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

### Step 3 — Install the Python Kafka client

```bash
pip install kafka-python
```

### Step 4 — Create the producer script

Create `producer_metrics.py`:

```python
"""
Q2 - Kafka Producer
Sends 10 sample server-metric messages to the 'server_metrics' topic.
"""
from kafka import KafkaProducer
import json
import random
import time

# ---------- 1. Configure the producer ----------
# value_serializer converts our Python dict into JSON bytes automatically,
# so we never have to call json.dumps() ourselves on every send.
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = "server_metrics"
servers = ["server01", "server02", "server03"]

# ---------- 2. Build and send 10 messages ----------
for i in range(10):
    message = {
        "server_id": random.choice(servers),
        "cpu_usage": random.randint(30, 100),     # occasionally crosses 80 on purpose
        "memory_usage": random.randint(40, 90)
    }

    producer.send(TOPIC, value=message)
    print(f"Sent: {message}")
    time.sleep(1)   # small delay so messages are easy to watch arrive one by one

# ---------- 3. Make sure every buffered message is actually transmitted ----------
producer.flush()
print("\nAll 10 messages sent and flushed to the broker.")
```

### Step 5 — Run the producer (Terminal 3, new window)

```bash
python producer_metrics.py
```
You should see 10 `Sent: {...}` lines print, one per second.

### Step 6 — Verify the messages landed (Terminal 4, new window)

```powershell
cd C:\kafka_2.13-4.3.1
.\bin\windows\kafka-console-consumer.bat --topic server_metrics --bootstrap-server localhost:9092 --from-beginning
```
This should print all 10 JSON messages you just sent — proof they were successfully published. (Question 3's Python consumer does the same verification job, with nicer formatting and the CPU alert logic.)

### How it works

- `KafkaProducer(bootstrap_servers=...)` opens a connection to the broker — `localhost:9092` is the default Kafka port.
- `producer.send(TOPIC, value=message)` is **asynchronous** — it hands the message to an internal buffer and returns immediately without waiting for the broker to confirm receipt.
- `producer.flush()` actually blocks until every buffered message has been sent — always call this before the script exits, or the last few messages may never go out.
- `random.choice(servers)` and `random.randint(30, 100)` simulate different servers reporting different load, including occasionally spiking past 80% CPU — this matters for Questions 3 and 5, which watch for exactly that.

---

## Question 3 — Python Kafka Consumer

> **QUESTION**
> Write a Python Kafka consumer that consumes messages from: server_metrics
>
> The consumer should: 1. Connect to the Kafka broker. 2. Subscribe to the server_metrics topic. 3. Continuously receive messages. 4. Display the received server metrics. 5. Detect whether CPU usage is greater than 80%. 6. Print: `ALERT: High CPU detected on server01` — when the condition is satisfied.
>
> Concepts tested: Consumer, topic, Python Kafka consumer, real-time monitoring, anomaly detection.

### Theory

A **consumer** does the opposite job of a producer: it connects to the broker, subscribes to a topic, and continuously receives whatever gets published — like tailing a live log file, except the "file" is shared across the network and any number of other consumers can tail it too, independently.

**`group_id` explained:** Kafka consumers belong to a **consumer group**. If two consumers share the same `group_id`, Kafka splits the topic's messages between them (load-balancing). Different `group_id`s each get their own full copy of every message. We use a unique group id so this consumer reliably sees everything.

**`auto_offset_reset='earliest'` explained:** Kafka remembers, per consumer group, the last message position ("offset") it read. A brand-new group has no saved position yet — `earliest` tells it to start from the very first message still stored on the broker, not just new ones sent after it connects.

### Step 1 — Create the consumer script

Create `consumer_metrics.py`:

```python
"""
Q3 - Python Kafka Consumer
Consumes messages from 'server_metrics', displays them, and alerts on high CPU.
"""
from kafka import KafkaConsumer
import json

# ---------- 1. Connect to the broker and subscribe to the topic ----------
consumer = KafkaConsumer(
    'server_metrics',                                  # topic name
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',                       # read from the start if no offset saved yet
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),  # turn JSON bytes back into a dict
    group_id='aiops-monitor-group'                      # named consumer group (see theory section)
)

print("Listening on 'server_metrics'... (Ctrl+C to stop)\n")

# ---------- 2. Continuously receive and process messages ----------
try:
    for msg in consumer:
        data = msg.value
        server_id = data["server_id"]
        cpu = data["cpu_usage"]
        memory = data["memory_usage"]

        print("Received:")
        print(f"Server: {server_id}")
        print(f"CPU: {cpu}%")
        print(f"Memory: {memory}%")

        # ---------- 3. Anomaly check ----------
        if cpu > 80:
            print(f"ALERT: High CPU detected on {server_id}")
        print()   # blank line between messages for readability

except KeyboardInterrupt:
    print("\nConsumer stopped.")
finally:
    consumer.close()
```

### Step 2 — Run it (keep the broker from Question 2 running)

```bash
python consumer_metrics.py
```
Since `auto_offset_reset='earliest'`, this immediately prints all 10 leftover messages from Question 2's producer run. To watch it react live, run `python producer_metrics.py` again in another window while this consumer sits open — new messages appear the instant they're sent.

### How it works

- `for msg in consumer:` — `KafkaConsumer` is iterable; this loop blocks and waits whenever there's nothing new, and immediately runs the loop body the moment a message arrives. This is what makes it "continuous" rather than a one-shot read.
- `msg.value` is already a Python dict at this point — the `value_deserializer` configured above ran automatically before the message reached our loop.
- The `if cpu > 80:` check is the same threshold logic as Question 1, applied to a live stream instead of a static dataset — this is the "real-time monitoring" version of anomaly detection.
- Wrapping the loop in `try/except KeyboardInterrupt` lets you stop the consumer cleanly with **Ctrl+C** instead of it hanging forever or crashing with a traceback.

---

## Question 4 — Build an AIOps Workflow using Airflow

> **QUESTION**
> Create an Apache Airflow DAG representing a basic AIOps workflow: Collect Metrics → Process Metrics → Detect Anomaly → Generate Report.
>
> Task 1 — collect_metrics: use PythonOperator to generate/sample server metrics (e.g. CPU = 87, Memory = 65, Response Time = 420ms).
>
> Task 2 — process_metrics: process the collected metrics and print them.
>
> Task 3 — detect_anomaly: check whether CPU > 80. If yes, print "Anomaly detected: High CPU usage", otherwise "No anomaly detected".
>
> Task 4 — generate_report: print a final AIOps report block.
>
> DAG requirements: use PythonOperator, define all four tasks, define the correct dependencies, run in order: collect_metrics >> process_metrics >> detect_anomaly >> generate_report.
>
> Concepts tested: Airflow, DAG, PythonOperator, dependencies, AIOps lifecycle, basic AIOps workflow.

> **NOTE:** Airflow needs a Linux environment — it does not run natively on Windows. This question is done in **WSL Ubuntu**. If Airflow isn't installed yet, see "Extra Section 2 — Airflow Setup From Scratch" at the end of this document first.

### Theory

This question is really about two Airflow concepts working together:

**PythonOperator** — wraps any plain Python function as an Airflow task. Airflow calls that function when the task runs; whatever the function does (print, compute, call an API) becomes the task's behaviour.

**XCom ("cross-communication")** — Airflow tasks normally run in isolation and can't share Python variables directly. XCom is Airflow's built-in mechanism for passing small pieces of data between tasks in the same DAG run:
- `ti.xcom_push(key='metrics', value=metrics)` — task A stores a value under a key
- `ti.xcom_pull(key='metrics', task_ids='collect_metrics')` — task B retrieves that value, naming which task produced it

This is exactly how `collect_metrics` hands its generated numbers to `process_metrics`, and how `process_metrics` hands them to `detect_anomaly` — without any global variables.

**Why `**kwargs` and `ti`?** Airflow automatically injects a set of runtime variables into every task function when it calls it — `ti` ("task instance") is one of them, and it's your handle for reading/writing XCom. `**kwargs` is just how the function agrees to accept all of those without naming every single one.

### Step 1 — Every fresh terminal, activate Airflow

```bash
cd ~/airflow-class
source .venv/bin/activate
export AIRFLOW_HOME=~/airflow-class
```

### Step 2 — Create the DAG file

```bash
cat > ~/airflow-class/dags/aiops_dag.py << 'EOF'
"""
Q4 - AIOps Workflow DAG
collect_metrics >> process_metrics >> detect_anomaly >> generate_report
"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import random

# ---------- Task functions ----------
# Each function receives **kwargs so it can access Airflow's task instance (ti)
# to push/pull data via XCom - Airflow's built-in mechanism for passing small
# pieces of data between tasks in the same DAG run.

def collect_metrics(**kwargs):
    """Task 1: generate/sample server metrics."""
    metrics = {
        "cpu": random.randint(60, 95),
        "memory": random.randint(50, 85),
        "response_time": random.randint(200, 500)
    }
    print(f"Collected metrics: CPU={metrics['cpu']}, "
          f"Memory={metrics['memory']}, Response Time={metrics['response_time']}ms")

    # push the dict so downstream tasks can read it
    kwargs['ti'].xcom_push(key='metrics', value=metrics)


def process_metrics(**kwargs):
    """Task 2: process (here: just retrieve + print) the collected metrics."""
    metrics = kwargs['ti'].xcom_pull(key='metrics', task_ids='collect_metrics')
    print(f"Processing metrics: {metrics}")

    # pass the same metrics forward to the next task
    kwargs['ti'].xcom_push(key='metrics', value=metrics)


def detect_anomaly(**kwargs):
    """Task 3: check CPU threshold."""
    metrics = kwargs['ti'].xcom_pull(key='metrics', task_ids='process_metrics')
    if metrics['cpu'] > 80:
        print("Anomaly detected: High CPU usage")
    else:
        print("No anomaly detected")


def generate_report(**kwargs):
    """Task 4: print the final summary report."""
    print("===== AIOps Report =====")
    print("Metrics collected successfully")
    print("Metrics processed successfully")
    print("Anomaly detection completed")
    print("========================")


# ---------- DAG definition ----------
with DAG(
    dag_id="aiops_workflow_dag",
    description="Basic AIOps workflow: collect -> process -> detect -> report",
    start_date=datetime(2025, 1, 1),
    schedule=None,          # manual trigger only, no automatic schedule
    catchup=False,
    tags=["aiops", "unit2"],
) as dag:

    t1 = PythonOperator(
        task_id="collect_metrics",
        python_callable=collect_metrics,
    )

    t2 = PythonOperator(
        task_id="process_metrics",
        python_callable=process_metrics,
    )

    t3 = PythonOperator(
        task_id="detect_anomaly",
        python_callable=detect_anomaly,
    )

    t4 = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    # ---------- Dependencies ----------
    t1 >> t2 >> t3 >> t4
EOF
```

> **NOTE:** We import `PythonOperator` from `airflow.providers.standard.operators.python` rather than the older `airflow.operators.python`. Both work on Airflow 3.x, but the old path now prints a deprecation warning.

### Step 3 — Confirm it's registered with no import errors

```bash
airflow dags list-import-errors
airflow dags list | grep aiops_workflow_dag
```

### Step 4 — Start Airflow and trigger it

```bash
airflow standalone
```
- Leave this running, then in your browser go to `http://localhost:8080` and log in with the printed credentials
- Find `aiops_workflow_dag` in the DAGs list → toggle it **On**
- Click the DAG → **▶ Trigger DAG**
- Watch all 4 tasks turn green in order in the **Grid** view

### Step 5 — Check the output

```bash
airflow dags list-runs aiops_workflow_dag
```
To see the actual printed output of each task ("Collected metrics: ...", "Anomaly detected: ...", the report block, etc.), click any green task box in the UI → **Logs**.

### How it works — the data flow

```
collect_metrics                process_metrics              detect_anomaly           generate_report
  generates {cpu, memory,        pulls that dict via           pulls it again,          just prints the
  response_time}                 xcom_pull, prints it,          checks cpu > 80,         fixed report -
  pushes it via xcom_push  --->  re-pushes it forward   --->   prints verdict     --->   doesn't need
                                                                                          the metrics
```
The `>>` operators at the bottom (`t1 >> t2 >> t3 >> t4`) are what actually enforce this order — without them, Airflow would treat all four as independent tasks with no guaranteed sequence, and might even run them in parallel.

---

## Question 5 — Integrated AIOps Challenge

> **QUESTION**
> Modify your Kafka consumer from Question 3 so that it behaves like a simple AIOps monitoring system.
>
> The consumer should: 1. Receive server metrics from Kafka. 2. Check CPU usage. 3. Detect an anomaly when CPU > 80%. 4. Print an alert. 5. Maintain a count of detected anomalies.
>
> Example: Message received: server01 | CPU: 85% → ALERT: High CPU detected. Message received: server02 | CPU: 45% → Normal. ... Total anomalies detected: 2
>
> Concepts tested: Kafka + Python + metrics + anomaly detection + AIOps monitoring.

### Theory

This question asks you to give the Question 3 consumer **memory across messages** — not just reacting to one message at a time, but keeping a running tally (`anomaly_count`) for the whole monitoring session. This is the difference between a plain *alert* (react once) and a *monitor* (track state over time) — the latter is much closer to a real AIOps dashboard, since "3 anomalies in the last 5 minutes" is usually more actionable than any single alert alone.

### Step 1 — Create the modified consumer

Create `aiops_monitor_consumer.py`:

```python
"""
Q5 - Integrated AIOps Challenge
A version of the Q3 consumer that behaves like a small monitoring system:
it keeps a running count of how many anomalies it has seen across all
messages, and prints the final tally when you stop it.
"""
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'server_metrics',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id='aiops-monitor-group'
)

anomaly_count = 0   # running counter, persists across every message in this session

print("AIOps monitor started. Listening on 'server_metrics'... (Ctrl+C to stop)\n")

try:
    for msg in consumer:
        data = msg.value
        server_id = data["server_id"]
        cpu = data["cpu_usage"]

        print(f"Message received: {server_id} | CPU: {cpu}%")

        if cpu > 80:
            anomaly_count += 1
            print("ALERT: High CPU detected")
        else:
            print("Normal")
        print()

except KeyboardInterrupt:
    # this block runs the moment you press Ctrl+C - our natural "end of monitoring session"
    print(f"\nTotal anomalies detected: {anomaly_count}")
finally:
    consumer.close()
```

### Step 2 — Run it

```bash
python aiops_monitor_consumer.py
```
Let it run, then in another window fire off `python producer_metrics.py` (from Question 2) one or more times to generate traffic. When done watching, press **Ctrl+C** — that's the cue for it to print the final `Total anomalies detected: N` line.

### How it works — why the counter survives across messages

`anomaly_count = 0` is declared **once, outside the `for` loop** — in Python, a variable defined before a loop keeps its value between iterations (it isn't reset each time). Every time the `if cpu > 80:` branch runs, `anomaly_count += 1` adds to the *same* variable, so by the time you stop the consumer, it holds the true total across every message seen in that run — this is the "state" that turns a stateless per-message reaction into an actual running monitor.

### Verified logic trace

Simulated against the example message sequence from the question, confirming the exact output shape:

```
Message received: server01 | CPU: 85%
ALERT: High CPU detected
Message received: server02 | CPU: 45%
Normal
Message received: server03 | CPU: 91%
ALERT: High CPU detected

Total anomalies detected: 2
```

---

# Extra Section

Reference material beyond the five questions: the terminal-only Kafka method from your notes, a full from-scratch Airflow install, and everyday Git/GitHub commands.

## Extra 1 — Kafka Producer/Consumer, Terminal-Only (No Python)

This is the method from your notes — sending and receiving messages directly through Kafka's own command-line tools, with no Python code involved. Useful for quickly testing a topic, but it doesn't give you the CPU-alert logic Questions 3 and 5 ask for — that needs actual code.

> **NOTE:** Topic name standardized to `server_metrics` here (matching the assignment) instead of `server-metric` — everything else follows your notes exactly.

### Terminal 1 — Start the broker

```powershell
cd C:\kafka_2.13-4.3.1
$uuid = .\bin\windows\kafka-storage.bat random-uuid
.\bin\windows\kafka-storage.bat format -t $uuid -c .\config\server.properties --standalone
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

### Terminal 2 — Create the topic

```powershell
cd C:\kafka_2.13-4.3.1
.\bin\windows\kafka-topics.bat --create --topic server_metrics --bootstrap-server localhost:9092
```

### Terminal 3 — Console producer

```powershell
cd C:\kafka_2.13-4.3.1
.\bin\windows\kafka-console-producer.bat --topic server_metrics --bootstrap-server localhost:9092
> {"server_id": "server01", "cpu_usage": 50, "memory_usage": 90}
>
```
This drops you into a `>` prompt. Every line you type and hit Enter on becomes one message sent to the topic — type as many as you like, in any format (plain text or JSON), one per line.

### Terminal 4 — Console consumer

```powershell
cd C:\kafka_2.13-4.3.1
.\bin\windows\kafka-console-consumer.bat --topic server_metrics --bootstrap-server localhost:9092 --from-beginning
```
This prints every message currently on the topic, then keeps the window open and prints new ones as they arrive from Terminal 3 — live, in real time.

### How this compares to the Python method

| | Console tools (this section) | Python code (Questions 2/3/5) |
|---|---|---|
| Sending messages | Type each one by hand at the `>` prompt | Generated programmatically — random values, loops, timing |
| Receiving messages | Just prints raw text to the screen | Parsed back into a dict; can trigger logic (alerts, counters) |
| Anomaly detection | Not possible — no code runs on the data | The whole point of Questions 3 and 5 |
| Good for | Quickly sanity-checking a topic works at all | The actual assignment deliverable |

---

## Extra 2 — Airflow Setup From Scratch (WSL Ubuntu)

Full install, from a brand-new Windows machine with nothing set up, to a working Airflow instance ready for Question 4's DAG.

### Step 1 — Install WSL + Ubuntu (one-time, only if not already installed)

In **Windows PowerShell, run as Administrator**:
```powershell
wsl --install
```
This installs WSL2 and Ubuntu by default, and will prompt a restart. After restarting, Ubuntu launches automatically the first time and asks you to create a Linux username and password — this is separate from your Windows login.

### Step 2 — Update Ubuntu and install Python tooling

From here on, every command runs inside the **Ubuntu terminal**, not PowerShell.
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv
```

### Step 3 — Create a project folder and virtual environment

A **virtual environment (venv)** is an isolated Python install just for this project — packages you install inside it don't affect or conflict with anything else on the system.
```bash
mkdir -p ~/airflow-class
cd ~/airflow-class
python3 -m venv .venv
source .venv/bin/activate
```
Your prompt should now show a `(.venv)` prefix, confirming the environment is active.

### Step 4 — Set AIRFLOW_HOME

This tells Airflow where to keep its config, DAGs, logs, and database.
```bash
export AIRFLOW_HOME=~/airflow-class
```
> **NOTE:** This only lasts for the current terminal session. To make it permanent, add the same line to `~/.bashrc`: `echo 'export AIRFLOW_HOME=~/airflow-class' >> ~/.bashrc`. You'll still need to reactivate the venv manually in every new terminal — that part is deliberately per-project.

### Step 5 — Install Airflow

Airflow publishes version-matched "constraint files" that pin every dependency to a combination known to work together — installing with `--constraint` avoids a whole category of version-conflict errors.
```bash
AIRFLOW_VERSION=3.1.0
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

### Step 6 — Initialize the metadata database

Airflow stores DAG run history, task states, and connections in a small database (SQLite by default) — this creates it.
```bash
airflow db migrate
```

### Step 7 — Create the dags folder

```bash
mkdir -p $AIRFLOW_HOME/dags
```
Any `.py` file placed here is automatically picked up and parsed by Airflow as a potential DAG.

### Step 8 — Start Airflow

```bash
airflow standalone
```
This single command starts the scheduler **and** the webserver together, and — the first time only — prints an auto-generated admin username and password in the terminal. Copy those down; you'll need them to log into the UI at `http://localhost:8080`.

### (Optional) Creating an admin user manually

If you're not using `standalone` (e.g. running the scheduler and webserver as separate processes in production), create a login manually instead:
```bash
airflow users create \
  --username admin \
  --firstname Your \
  --lastname Name \
  --role Admin \
  --email you@example.com \
  --password admin
```

### Every new terminal, from here on

```bash
cd ~/airflow-class
source .venv/bin/activate
export AIRFLOW_HOME=~/airflow-class
```
This is the same three-line reactivation shown at the start of Question 4 — you now know exactly what each line does and why it's needed every time.

---

## Extra 3 — Git & GitHub Basics

Everyday commands for version control and collaborating through GitHub.

### One-time setup

```bash
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
```

### Starting a repository

| Situation | Command |
|---|---|
| Turn the current folder into a new Git repo | `git init` |
| Copy down an existing repo from GitHub | `git clone <repository-url>` |
| Connect a local repo to a GitHub remote | `git remote add origin <repository-url>` |

### The everyday loop: status, add, commit, push

```bash
git status                     # what's changed since the last commit
git add .                      # stage everything changed (or: git add <file>)
git commit -m "message"        # save a snapshot with a description
git push origin main           # upload commits to GitHub
```

### Branches — creating, switching, listing

| Action | Command |
|---|---|
| List all branches | `git branch` |
| Create a new branch (don't switch yet) | `git branch feature-x` |
| Create a new branch AND switch to it | `git checkout -b feature-x` (or: `git switch -c feature-x`) |
| Switch to an existing branch | `git checkout feature-x` (or: `git switch feature-x`) |
| Push a new branch to GitHub for the first time | `git push -u origin feature-x` |
| Delete a local branch | `git branch -d feature-x` |
| Delete a branch on GitHub | `git push origin --delete feature-x` |

### Merging branches

```bash
git checkout main              # switch to the branch you want to merge INTO
git pull origin main            # make sure it's up to date first
git merge feature-x             # bring feature-x's commits into main
git push origin main            # upload the merged result
```
> **NOTE:** If Git reports a **merge conflict**, it means the same lines were changed differently on both branches. Open the flagged file(s), look for the `<<<<<<<` / `=======` / `>>>>>>>` markers, edit the section to keep what you want, delete the markers, then run `git add <file>` and `git commit` to finish the merge.

### Creating a Pull Request (PR)

A Pull Request asks to merge one branch into another on GitHub, with a place for review and discussion before it happens — typically used instead of merging directly, even for your own repos.

**Option A — GitHub website**
1. Push your branch: `git push -u origin feature-x`
2. Go to the repository on **github.com** — GitHub usually shows a **"Compare & pull request"** banner automatically for a recently-pushed branch
3. Fill in a title and description explaining the change
4. Click **Create pull request**
5. After review/approval, click **Merge pull request** on the PR page

**Option B — GitHub CLI (`gh`)**
```bash
gh pr create --base main --head feature-x --title "Add feature X" --body "Description of the change"
gh pr view --web                 # open the PR in your browser
gh pr merge --merge              # merge it once approved
```

### Other commands worth knowing

| Command | What it does |
|---|---|
| `git log --oneline` | Compact history of commits |
| `git diff` | Shows exactly what's changed but not yet staged |
| `git pull` | Fetches and merges the latest changes from the remote |
| `git stash` | Temporarily shelves uncommitted changes so you can switch branches cleanly |
| `.gitignore` file | Lists files/folders Git should never track (e.g. `.venv/`, `__pycache__/`, `.env`) |
