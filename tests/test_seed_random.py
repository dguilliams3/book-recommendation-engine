import uuid

from common.testing import seed_random

def test_seed_random_repro_uuid():
    seed_random(42)
    u1 = uuid.uuid4()
    seed_random(42)
    u2 = uuid.uuid4()
    assert u1 == u2 