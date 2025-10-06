# fraud-01-kwameape
# Project: Streaming Mobile Money Transactions.
# Author: Arnold Atchoe.

## Project Goal: The focus of this project includes,
1.	fraud detection within a mobile money system.
2.  Tracking hourly fraud frequency.
3.	Monitoring hourly revenue.
4.	Monitoring hourly transaction amount.

## Notes
This project has;
1. Producers which write messages to a json file and kafka topic to simulate real world situations.
2. Consumers which reads messages from a json file and kafka topic and then process these messages to create visualization as well as move them to a database. Visualization included a:
    1. Bar charts to track the frequency fraudulent and non-fraudulent transactions.
    2. Line charts to track hourly revenue and hourly transaction amount. Revenue was assumed to be 2% of hourly transaction.
3. Configuration files them configure producers and consumers.
4. A logger that helps to keep track of errors and how project modules function.

**CSV data file was too large to be upload to GitHub via git, hence a link to download this file is https://www.kaggle.com/datasets/ealaxi/paysim1/data**


## Project Execution.

### STEP 1: Verify tool installation and Setup
1. VS Code.
2. Python. Python 3.11 is required.
3. Wsl and Kafka.
4. PostgreSQL.

### STEP 2: INITIALIZE PROJECT
Start a repository in **Github** for the project and then **clone** project to local machine to be 
worked on.

Occasionally as you work of the project, especially a important points, sync the project to **Github** applying the following steps in powershell or bash;
1. ```Git add .```
2. ```Git commit -m "commit message"```
3. ```Git push origin main.```

### STEP 3: If Windows, Start WSL
Launch WSL. Open a PowerShell terminal in VS Code. Run the following command:
```wsl```
You should now be in a Linux shell (prompt shows something like username@DESKTOP:.../repo-name$).
Do all steps related to starting Kafka in this WSL window.

### STEP 4: Start Kafka(Using WSL if Windows)
Before starting, run a short prep script to ensure Kafka has a persistent data directory and meta.properties set up. This step works on WSL, macOS, and Linux - be sure you have the $ prompt and you are in the root project folder.

Make sure the script is executable.
Run the shell script to set up Kafka.
Cd (change directory) to the kafka directory.
Start the Kafka server in the foreground. Keep this terminal open - Kafka will run here.
```chmod +x scripts/prepare_kafka.sh```
```scripts/prepare_kafka.sh```
```cd ~/kafka```
```bin/kafka-server-start.sh config/kraft/server.properties```
Keep this terminal open! Kafka is running and needs to stay active.

### STEP 5: Manage Local Project Virtual Environment

Open your project in VS Code and use the commands for your operating system to:

1. Create a Python virtual environment.
2. Activate the virtual environment.
3. Upgrade pip and key tools. 
4. Install from requirements.txt.

### Windows

Open a new PowerShell terminal in VS Code (Terminal / New Terminal / PowerShell).
**Python 3.11** is required for Apache Kafka. 

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install --upgrade pip wheel setuptools
py -m pip install --upgrade -r requirements.txt
```

If you get execution policy error, run this first:
`Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

### Mac / Linux

Open a new terminal in VS Code (Terminal / New Terminal)

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade -r requirements.txt
```
---

### STEP 6: Start a New Streaming Application

This will take four terminals:

1. Two to run the producers which writes messages to a file and Kafka topic. 
2. Four to run each consumer. Two consumers to consume from file and kafka topic for visualization. Another two consumers to consume from a file and kafka topic for writing to a database.

### Producer Terminal (Outputs to Various Sinks)

Start the producer to generate the messages. 

File_producer writes messages to a live data file in the data folder.
kafka_producer  writes messages to a  kafka topic.

In VS Code, open a NEW terminal.
Use the commands below to activate .venv, and start the producer. 

Windows:

```shell
.\.venv\Scripts\Activate.ps1
py -m producers.file_producer
py -m producers.kafka_producer
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m producers.file_producer
python3 -m producers.kafka_producer
```

### Consumer Terminal

Start associated consumer.  

1. Start consumers that reads from the live data file.
2. Start consumers that reads from the Kafka topic.

NOTE: Each consumer modifies or transforms the message read from the
live data or kafka topic and then sends the transformed data to a PostgreSQL database
as well as creates needed visualization. Transfromation employed include feature engineering, aggregation and creation to new features from old ones.

In VS Code, open a NEW terminal in your root project folder. 
Use the commands below to activate .venv, and start the consumer. 

Windows:
```shell
.\.venv\Scripts\Activate.ps1
py -m consumers.file_consumer_database
py -m consumers.file_consumer_visualization
py -m consumers.kafka_consumer_database
py -m consumers.kafka_consumer_visualization
```


Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m consumers.file_consumer_database
python3 -m consumers.file_consumer_visualization
python3 -m consumers.kafka_consumer_database
python3 -m consumers.kafka_consumer_visualization
```
---

## How To Stop a Continuous Process

To kill the terminal, hit CTRL c (hold both CTRL key and c key down at the same time).

## Later Work Sessions

When resuming work on this project:

1. Open the project repository folder in VS Code. 
2. Start the Kafka service (use WSL if Windows) and keep the terminal running. 
3. Activate your local project virtual environment (.venv) in your OS-specific terminal.
4. Run `git pull` to get any changes made from the remote repo (on GitHub).

## After Making Useful Changes

1. Git add everything to source control (`git add .`)
2. Git commit with a -m message.
3. Git push to origin main.

```shell
git add .
git commit -m "your message in quotes"
git push -u origin main
```

## Save Space

To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later.
Managing Python virtual environments is a valuable skill.

## License

This project is licensed under the MIT License as an example project.
You are encouraged to fork, copy, explore, and modify the code as you like.
See the [LICENSE](LICENSE.txt) file for more.

## Recommended VS Code Extensions

- Black Formatter by Microsoft
- Markdown All in One by Yu Zhang
- PowerShell by Microsoft (on Windows Machines)
- Python by Microsoft
- Python Debugger by Microsoft
- Ruff by Astral Software (Linter + Formatter)
- **SQLite Viewer by Florian Klampfer**
- WSL by Microsoft (on Windows Machines)






