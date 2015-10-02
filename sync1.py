# -*- coding: utf-8 -*-
"""
Sync variant with Redis queue


What do we have in Redis queue:
      -3     -2     -1       (negative offset)
      34     35     36       (seq no examples)
 ...  seq1   seq2   seq3     (seq numbers)
 ... [ id1 ][ id2 ][ id3 ]   (values)

Separately we store the biggest sequence number we have seen so far (seq3). Given that we know the queue length,
we can easily define if we can return all ids, and what is the offset we should start returning from.

It's also convenient to use negative offsets. If we want to have just last record, we should ask lrange(..., -1, -1),
etc.

If user last seen value was 33 (from the example), then we should return the range (-3, -1), which is
(last_seq_no - seq_no), -1.
"""
import redis
from sqlalchemy import (create_engine, MetaData, Table, Column, Index, Integer,
                        Text)

QUEUE_LENGTH = 10


r = redis.Redis()
engine = create_engine("sqlite:///sync1.sqlite")
metadata = MetaData()
tasks = Table('tasks', metadata,
              Column('id', Integer, primary_key=True),
              Column('user_id', Integer, nullable=False),
              Column('content', Text, nullable=False),
              Index('idx_user_id', 'user_id'))
metadata.create_all(engine)


def get_seq_no(user_id):
    return int(r.hget('seq_no', user_id) or 0)


def next_seq_no(user_id):
    return r.hincrby('seq_no', user_id, 1)


def enqueue_task_id(user_id, task_id):
    r.rpush('queue:%s' % user_id, task_id)
    r.ltrim('queue:%s' % user_id, -QUEUE_LENGTH, -1)


def add_task(user_id, content):
    seq_no = next_seq_no(user_id)
    result = engine.execute(tasks.insert().values(user_id=user_id,
                                                  content=content))
    task_id = result.inserted_primary_key[0]
    enqueue_task_id(user_id, task_id)
    return task_id, seq_no


def edit_task(user_id, task_id, content):
    seq_no = next_seq_no(user_id)
    engine.execute(tasks.update()
                   .where((tasks.c.user_id == user_id) & (tasks.c.id == task_id))
                   .values(content=content))
    enqueue_task_id(user_id, task_id)
    return seq_no


def delete_task(user_id, task_id):
    seq_no = next_seq_no(user_id)
    engine.execute(tasks.delete()
                   .where((tasks.c.user_id == user_id) & (tasks.c.id == task_id)))
    enqueue_task_id(user_id, task_id)
    return seq_no


def all_tasks(user_id):
    result = engine.execute(tasks.select()
                            .with_only_columns([tasks.c.id, tasks.c.content])
                            .where((tasks.c.user_id == user_id)))
    return [dict(i) for i in result]


def sync_tasks(user_id, seq_no):
    current_seq_no = get_seq_no(user_id)
    offset = seq_no - current_seq_no

    if offset > -1:
        # offset has to be negative. If it's positive then we just have nothing to show
        return []

    if r.llen('queue:%s' % user_id) < -offset:
        # queue is too short, full re-sync required
        raise ValueError("Queue is too short for offset %s" % seq_no)

    task_ids = {int(i) for i in r.lrange('queue:%s' % user_id, offset, -1)}
    result = engine.execute(tasks.select()
                            .with_only_columns([tasks.c.id, tasks.c.content])
                            .where((tasks.c.id.in_(task_ids))))
    result = [dict(i) for i in result]

    # now we have to populate the result with deleted records
    result_ids = {r['id'] for r in result}
    deleted_ids = task_ids - result_ids
    result += [{'id': _id, 'deleted': True} for _id in deleted_ids]
    return result


if __name__ == '__main__':
    from IPython import embed
    embed()
