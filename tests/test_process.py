from aoietl.process import process

def test_process(test_config, azure_blob):
    process(test_config, azure_blob, azure_blob)
    # TODO need to add some assertions to check if the process worked correctly
    assert True