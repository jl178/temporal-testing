"""The profile matrix: 5 sizes x 3 shapes, with shape semantics."""
from worker_platform.profiles import PROFILES

SIZES = ["xsmall", "small", "medium", "large", "xlarge"]


def test_matrix_is_complete():
    assert len(PROFILES) == 15
    for size in SIZES:
        for name in (size, f"{size}-cpu", f"{size}-mem"):
            assert name in PROFILES
            assert PROFILES[name].name == name


def test_only_large_tiers_use_the_resource_tuner():
    for name, profile in PROFILES.items():
        expected = name.startswith(("large", "xlarge"))
        assert profile.resource_tuner is expected, name


def test_large_tiers_carry_queue_dispatch_caps():
    assert PROFILES["large"].queue_activities_per_second == 4
    assert PROFILES["xlarge"].queue_activities_per_second == 8
    assert PROFILES["medium"].queue_activities_per_second is None


def test_mem_shape_halves_admission():
    assert PROFILES["medium"].max_concurrent_activities == 16
    assert PROFILES["medium-mem"].max_concurrent_activities == 8
    assert PROFILES["large"].tuner_max_slots == 2
    assert PROFILES["large-mem"].tuner_max_slots == 1
    assert PROFILES["xlarge"].tuner_max_slots == 4
    assert PROFILES["xlarge-mem"].tuner_max_slots == 2


def test_cpu_shape_leaves_concurrency_alone():
    for size in SIZES:
        assert (
            PROFILES[f"{size}-cpu"].max_concurrent_activities
            == PROFILES[size].max_concurrent_activities
        )


def test_admission_never_zero():
    for profile in PROFILES.values():
        assert profile.max_concurrent_activities >= 1
        assert profile.tuner_max_slots >= 1
        assert profile.thread_pool_size >= 1
