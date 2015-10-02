import pytest
import sync0
from sqlalchemy import create_engine


@pytest.fixture(scope='module', autouse=True)
def init():
    sync0.engine = create_engine("sqlite://")
    sync0.metadata.create_all(sync0.engine)


@pytest.fixture(scope='function', autouse=True)
def cleanup():
    sync0.r.delete('seq_no')
    sync0.engine.execute(sync0.tasks.delete())


def test_sync():
    # I'm adding a task...
    task_id, seq_no = sync0.add_task(1, 'foo')
    # I've just added a task, and I know the last sequence number, is there anything new for me?
    assert sync0.sync_tasks(1, seq_no) == []


def test_sync_add():
    # I'm adding a task...
    task_id, seq_no = sync0.add_task(1, 'foo')
    # Someone else is adding a task independently from me
    sync0.add_task(1, 'bar')
    # Let's see what new do we have. I only know my sequence number
    tasks = sync0.sync_tasks(1, seq_no)
    assert len(tasks) == 1
    assert tasks[0]['content'] == 'bar'


def test_sync_edit():
    # I'm adding a task...
    task_id, seq_no = sync0.add_task(1, 'foo')
    # Someone else changes my task
    sync0.edit_task(1, task_id, 'bar')
    # Let's see what new do we have. I only know my sequence number
    tasks = sync0.sync_tasks(1, seq_no)
    assert len(tasks) == 1
    assert tasks[0]['id'] == task_id
    assert tasks[0]['content'] == 'bar'


def test_sync_delete():
    # I'm adding a task...
    task_id, seq_no = sync0.add_task(1, 'foo')
    # Someone else deletes it!
    sync0.delete_task(1, task_id)
    # Let's see what new do we have. I only know my sequence number
    tasks = sync0.sync_tasks(1, seq_no)
    assert len(tasks) == 1
    assert tasks[0]['id'] == task_id
    assert tasks[0]['deleted']
    # let's ensure we don't get deleted tasks on initial sync
    assert sync0.all_tasks(1) == []
