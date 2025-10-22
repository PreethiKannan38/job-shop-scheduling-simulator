# driver.py
from simulation import WorkspaceSimulation

if __name__ == "__main__":
    sim = WorkspaceSimulation(
        broker="localhost",   # change if broker is remote
        port=1883,
        keepalive=60,
        tick_seconds=1,     # your last run used 0.3s
        seed_jobs=20,          # your last run used 5 jobs
        
    )
    sim.run(max_ticks=1000)
