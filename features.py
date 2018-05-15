from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, create_engine, Boolean, func, Float, Sequence

features = [
    ("feature_1", String),
    ("feature_2", Integer),
    ("feature_3", Boolean),
    ("feature_4", Float),
    ("feature_5", Float),
    ("feature_6", Boolean)
]

