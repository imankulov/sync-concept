# -*- coding: utf-8 -*-
"""
Sync variant with sequence numbers
"""
import redis
from sqlalchemy import (create_engine, MetaData, Table, Column, Index, Integer,
                        Text, Boolean)


r = redis.Redis()
engine = create_engine("sqlite:///sync0.sqlite")
metadata = MetaData()
tasks = Table('tasks', metadata,
              Column('id', Integer, primary_key=True),
              Column('user_id', Integer, nullable=False),
              Column('content', Text, nullable=False),
              Column('deleted', Boolean, default=False),
              Column('seq_no', Integer, nullable=False),
              Index('idx_seq_no', 'user_id', 'seq_no'),
              Index('idx_deleted', 'user_id', 'deleted'))
metadata.create_all(engine)


def get_seq_no(user_id):
    return int(r.hget('seq_no', user_id) or 0)


def next_seq_no(user_id):
    return r.hincrby('seq_no', user_id, 1)


def add_task(user_id, content):
    seq_no = next_seq_no(user_id)
    result = engine.execute(tasks.insert().values(user_id=user_id,
                                                  content=content,
                                                  seq_no=seq_no))
    return result.inserted_primary_key[0]


def edit_task(user_id, task_id, content):
    seq_no = next_seq_no(user_id)
    engine.execute(tasks.update()
                   .where(tasks.c.user_id == user_id, tasks.c.id == task_id)
                   .values(content=content, seq_no=seq_no))


def delete_task(user_id, task_id):
    seq_no = next_seq_no(user_id)
    engine.execute(tasks.update()
                   .where(tasks.c.user_id == user_id, tasks.c.id == task_id)
                   .values(deleted=True, seq_no=seq_no))


def all_tasks(user_id):
    result = engine.execute(tasks.select()
                            .with_only_columns([tasks.c.id, tasks.c.content])
                            .where((tasks.c.user_id == user_id) & (tasks.c.deleted == False)))
    return [dict(i) for i in result]


def sync_tasks(user_id, seq_no):
    result = engine.execute(tasks.select()
                            .with_only_columns([tasks.c.id, tasks.c.content, tasks.c.deleted])
                            .where((tasks.c.user_id == user_id) & (tasks.c.seq_no > seq_no)))
    return [dict(i) for i in result]


if __name__ == '__main__':
    from IPython import embed
    embed()
