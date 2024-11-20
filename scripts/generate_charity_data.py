import csv
import random
import faker
import os
from datetime import timedelta , datetime

faker = faker()

def random_date():
      start_date = datetime.now() - timedelta(days=5*365)
      random_days = random.randint(0, 5*365)
      return (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")
