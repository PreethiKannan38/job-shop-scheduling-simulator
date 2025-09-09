import random

"""
-------------------------------------------------------------------------------------------------------------------------------------------

The Job class represents tasks occurring in a job shop simulation.

Parameters:
- job_id (int): Unique identifier for the job.
- job_name (string): Descriptive name of the job.
- job_priority (int): Priority of the job (initially -1, as no jobs are being processed).
- machine_requirement (list): Sequence of machines required to complete the job (e.g., ['M0', 'M4', 'M2']).
- machine_processing_time (list): Time each machine takes to process part of the job.
- total_processing_time (int): Total time required to complete the job (sum of processing times).
- threshold_temperature_reduction (float): The arrival of a new job causes a delay in allocating the required resources, resulting in a small temperature reduction in the machine.
- power_consumption (list) : Total power consumed by each machine to prcoess part of the job (estimation).
- job_failure_rate (float): Probability of job failure even after processing .
- status (string): Current status of the job.

-------------------------------------------------------------------------------------------------------------------------------------------

"""

class Job:
    
    def __init__(self, job_id,job_name,machine_requirement):
        self.job_id = job_id  
        self.job_name = job_name                    
        self.job_priority = -1
        self.machine_requirement = machine_requirement  
        self.machine_processing_time = [random.randint(1,5) for i in self.machine_requirement]
        self.total_processing_time = sum(self.machine_processing_time)  
        self.power_consumption =  [random.randint(4,15) * i for i in self.machine_processing_time]
        self.threshold_temperature_reduction = random.uniform(0.1, 0.5)
        
        if random.random() < 0.1:
            self.job_failure_rate = random.uniform(0.4,0.8)
        else:
            self.job_failure_rate = random.uniform(0.1,0.3)
        self.status = 'Not Started'            

