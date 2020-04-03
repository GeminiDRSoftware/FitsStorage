from sqlalchemy import *
from migrate import *
from fits_storage.orm.target import Target, TargetPresence, TargetsChecked, TargetQueue
from fits_storage.orm import session_scope


def upgrade(migrate_engine):
    Target.metadata.create_all(migrate_engine)
    TargetPresence.metadata.create_all(migrate_engine)
    TargetsChecked.metadata.create_all(migrate_engine)
    TargetQueue.metadata.create_all(migrate_engine)

    targets = [
        ('Mercury', 'mercury'),
        ('Venus', 'venus'),
        ('Moon', 'moon'),
        ('Mars', 'mars'),
        ('Jupiter', 'jupiter'),
        ('Saturn', 'saturn'),
        ('Uranus', 'uranus'),
        ('Neptune', 'neptune'),
        ('Pluto', 'pluto')
    ]

    with session_scope() as session:
        for (a, b) in targets:
            if session.query(Target).filter(Target.ephemeris_name == b).one_or_none() is None:
                session.add(Target(a,b))


def downgrade(migrate_engine):
    meta = MetaData(bind = migrate_engine)
    target = Table('target', meta, autoload=True)
    target.drop()
    target_presence = Table('target_presence', meta, autoload=True)
    target_presence.drop()
    targets_checked = Table('targets_checked', meta, autoload=True)
    targets_checked.drop()
    target_queue = Table('targetqueue', meta, autoload=True)
    target_queue.drop()
