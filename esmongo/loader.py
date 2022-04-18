import pandas as pd


def load_dummy_data():
    dummy_data_1 = {"name": "john", "age": 24}
    dummy_data_2 = {"name": "reiner", "age": 25}
    dummy_data_3 = {"name": "john", "age": 26}
    dummy_filter_update = {"name": "john"}
    dummy_update = {"name": "paladin"}
    dummy_filter_delete_1 = {"name": "reiner"}
    dummy_filter_delete_2 = {"name": "paladin"}
    dummy_index = "pytest"

    return {
        "dummy_data_1": dummy_data_1,
        "dummy_data_2": dummy_data_2,
        "dummy_data_3": dummy_data_3,
        "dummy_filter_update": dummy_filter_update,
        "dummy_update": dummy_update,
        "dummy_filter_delete_1": dummy_filter_delete_1,
        "dummy_filter_delete_2": dummy_filter_delete_2,
        "dummy_index": dummy_index,
    }
