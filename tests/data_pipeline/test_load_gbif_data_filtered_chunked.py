# ---------------------------------------------------------------------------- #
#                                    IMPORTS                                   #
# ---------------------------------------------------------------------------- #

from backend.python.data_pipeline.load import load_gbif_data_filtered_chunked

# ---------------------------------------------------------------------------- #
#                                     TESTS                                    #
# ---------------------------------------------------------------------------- #

def test_load_gbif_data_filtered_chunked_returns_data():
    results, is_end = load_gbif_data_filtered_chunked("Lepidoptera", 797, 0)
    assert results is not None
    assert len(results) > 0
    

def test_load_gbif_data_filtered_chunked_returns_boolean_end():
    results, is_end = load_gbif_data_filtered_chunked("Lepidoptera", 797, 0)
    assert isinstance(is_end, bool)
