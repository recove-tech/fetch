from typing import List, Dict

import random, time, json
from uuid import uuid4
from datetime import datetime


def generate_uuid() -> str:
    return str(uuid4())


def generate_timestamp() -> str:
    return datetime.now().isoformat()


def generate_unix_timestamp() -> int:
    return int(datetime.now().timestamp())


def random_sleep(min_sleep: int = 1, max_sleep: int = 10) -> None:
    sleep_time = random.randint(min_sleep, max_sleep)
    time.sleep(sleep_time)


def create_batches(input_list: List, batch_size: int) -> List[List]:
    batches = []

    for i in range(0, len(input_list), batch_size):
        batch = input_list[i : i + batch_size]
        batches.append(batch)

    return batches


def load_json_file(filepath: str) -> List[Dict]:
    with open(filepath, "r", encoding="utf-8") as file:
        return json.load(file)


def save_to_jsonl(data_list: List[Dict], filepath: str, append: bool = False) -> None:
    mode = "a" if append else "w"
    with open(filepath, mode, encoding="utf-8") as file:
        for item in data_list:
            json_str = json.dumps(item, ensure_ascii=False)
            file.write(json_str + "\n")
