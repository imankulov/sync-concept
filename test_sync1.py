import pytest
import sync1
from sqlalchemy import create_engine


@pytest.fixture(scope='module', autouse=True)
def init():
    sync1.engine = create_engine("sqlite://")
    sync1.metadata.create_all(sync1.engine)


@pytest.fixture(scope='function', autouse=True)
def cleanup():
    sync1.r.delete('seq_no')
    sync1.r.delete('queue:1')
    sync1.engine.execute(sync1.tasks.delete())


def test_sync():
    # I'm adding a task...
    task_id, seq_no = sync1.add_task(1, 'foo')
    # I've just added a task, and I know the last sequence number, is there anything new for me?
    assert sync1.sync_tasks(1, seq_no) == []


def test_sync_add():
    # I'm adding a task...
    task_id, seq_no = sync1.add_task(1, 'foo')
    # Someone else is adding a task independently from me
    sync1.add_task(1, 'bar')
    # Let's see what new do we have. I only know my sequence number
    tasks = sync1.sync_tasks(1, seq_no)
    assert len(tasks) == 1
    assert tasks[0]['content'] == 'bar'


def test_sync_edit():
    # I'm adding a task...
    task_id, seq_no = sync1.add_task(1, 'foo')
    # Someone else changes my task
    sync1.edit_task(1, task_id, 'bar')
    # Let's see what new do we have. I only know my sequence number
    tasks = sync1.sync_tasks(1, seq_no)
    assert len(tasks) == 1
    assert tasks[0]['id'] == task_id
    assert tasks[0]['content'] == 'bar'


def test_sync_delete():
    # I'm adding a task...
    task_id, seq_no = sync1.add_task(1, 'foo')
    # Someone else deletes it!
    sync1.delete_task(1, task_id)
    # Let's see what new do we have. I only know my sequence number
    tasks = sync1.sync_tasks(1, seq_no)
    assert len(tasks) == 1
    assert tasks[0]['id'] == task_id
    assert tasks[0]['deleted']
    # let's ensure we don't get deleted tasks on initial sync
    assert sync1.all_tasks(1) == []


def test_sync_offset():
    for i in xrange(10):
        sync1.add_task(1, 'task%s' % i)
    # it's still allright
    assert len(sync1.sync_tasks(1, 0)) == 10
    # add one more task
    sync1.add_task(1, 'task10')
    with pytest.raises(ValueError):
        sync1.sync_tasks(1, 0)
    # but it works with bigger offset
    assert len(sync1.sync_tasks(1, seq_no=1)) == 10
    # and also, with offset 11 it returns nothing
    assert len(sync1.sync_tasks(1, seq_no=11)) == 0

