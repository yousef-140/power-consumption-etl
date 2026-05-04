import pandas as pd 
from pathlib import path
from datetime import datetime 

INPUT_PATH = "/home/yousef/Desktop/docker_section/data/household_power_consumption.txt"
OUTPUT_DIR = "data/raw"
DATASET_NAME = "my_dataset"
BATCH_SIZE = 1000


def extract_to_batches():

    df = pd.read_csv(INPUT_PATH)

    
