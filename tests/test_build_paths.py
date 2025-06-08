from aoietl.build_paths import build_config


def test_build_config(test_config):
    print(test_config)
    config = build_config(test_config)
    print(config)
    assert True