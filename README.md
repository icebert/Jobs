# Jobs
Batch Job Manager for single-node Linux system

Jobs runs out of box. What you need is to edit the jobs.conf file to change the default settings, then add the Jobs path to the PATH environment variable.

## Usage
Start the server
```bash
sserver
```

Submit bash script to run
```bash
sbatch <script file>
```

Cancel submitted script
```bash
scancel <jobid>
```

Query all jobs status
```bash
squeue
```

Query job status for user
```bash
squeue <username>
```
