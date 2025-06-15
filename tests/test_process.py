from aoietl.process import process

def test_process(test_config, azure_blob):
    process(test_config, azure_blob, azure_blob)
    assert True