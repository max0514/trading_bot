"""The Registry is the coverage spec's mirror: every declared Dataset exposes
exactly the Fields the Catalog promises, and every Dataset module is discovered
without editing shared code."""
import pytest

from twlab import catalog, registry


@pytest.mark.parametrize("name", registry.names())
def test_registry_fields_match_catalog(name):
    spec = registry.get_spec(name)
    if spec.shape == "table":
        # Static tables are addressed by their bare Data Key in the Catalog.
        assert catalog.resolve(name).dataset == name
        return
    catalog_keys = {f.key for f in catalog.dataset_fields(name)}
    registry_keys = {f"{name}:{f}" for f in spec.fields}
    assert registry_keys == catalog_keys


@pytest.mark.parametrize("name", registry.names())
def test_every_spec_is_runnable(name):
    spec = registry.get_spec(name)
    if spec.is_derived:
        assert spec.fetch is None and spec.parse is None
    else:
        assert callable(spec.fetch) and callable(spec.parse)
    assert spec.frequency in ("daily", "monthly", "quarterly", "static")
    assert spec.cadence.kind in ("daily", "monthly", "quarterly")


def test_unknown_dataset_lists_known_ones():
    with pytest.raises(KeyError, match="price"):
        registry.get_spec("nope")
