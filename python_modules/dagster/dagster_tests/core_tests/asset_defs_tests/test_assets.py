from dagster import AssetKey, Out, Output, io_manager
from dagster.core.asset_defs import AssetGroup, AssetIn, SourceAsset, asset, multi_asset
from dagster.core.storage.mem_io_manager import InMemoryIOManager


def test_with_replaced_asset_keys():
    @asset(ins={"input2": AssetIn(namespace="something_else")})
    def asset1(input1, input2):
        assert input1
        assert input2

    replaced = asset1.with_replaced_asset_keys(
        output_asset_key_replacements={
            AssetKey(["asset1"]): AssetKey(["prefix1", "asset1_changed"])
        },
        input_asset_key_replacements={
            AssetKey(["something_else", "input2"]): AssetKey(["apple", "banana"])
        },
    )

    assert set(replaced.dependency_asset_keys) == {
        AssetKey("input1"),
        AssetKey(["apple", "banana"]),
    }
    assert replaced.asset_keys == {AssetKey(["prefix1", "asset1_changed"])}

    assert replaced.asset_keys_by_input_name["input1"] == AssetKey("input1")

    assert replaced.asset_keys_by_input_name["input2"] == AssetKey(["apple", "banana"])

    assert replaced.asset_keys_by_output_name["result"] == AssetKey(["prefix1", "asset1_changed"])


def test_to_source_assets():
    @asset(metadata={"a": "b"}, io_manager_key="abc", description="blablabla")
    def my_asset():
        ...

    assert my_asset.to_source_assets() == [
        SourceAsset(
            AssetKey(["my_asset"]),
            metadata={"a": "b"},
            io_manager_key="abc",
            description="blablabla",
        )
    ]

    @multi_asset(
        outs={
            "my_out_name": Out(
                asset_key=AssetKey("my_asset_name"),
                metadata={"a": "b"},
                io_manager_key="abc",
                description="blablabla",
            ),
            "my_other_out_name": Out(
                asset_key=AssetKey("my_other_asset"),
                metadata={"c": "d"},
                io_manager_key="def",
                description="ablablabl",
            ),
        }
    )
    def my_multi_asset():
        yield Output(1, "my_out_name")
        yield Output(2, "my_other_out_name")

    assert my_multi_asset.to_source_assets() == [
        SourceAsset(
            AssetKey(["my_asset_name"]),
            metadata={"a": "b"},
            io_manager_key="abc",
            description="blablabla",
        ),
        SourceAsset(
            AssetKey(["my_other_asset"]),
            metadata={"c": "d"},
            io_manager_key="def",
            description="ablablabl",
        ),
    ]


def test_asset_with_io_manager_def():
    io_manager_inst = InMemoryIOManager()

    @io_manager
    def the_io_manager():
        return io_manager_inst

    @asset(io_manager=the_io_manager)
    def the_asset():
        return 5

    AssetGroup([the_asset]).materialize()

    assert list(io_manager_inst.values.values())[0] == 5


def test_multiple_assets_io_manager_defs():
    io_manager_inst = InMemoryIOManager()
    num_times = [0]

    @io_manager
    def the_io_manager():
        num_times[0] += 1
        return io_manager_inst

    # These will produce different instances of the io manager.
    @asset(io_manager=the_io_manager)
    def the_asset():
        return 5

    @asset(io_manager=the_io_manager)
    def other_asset():
        return 6

    AssetGroup([the_asset, other_asset]).materialize()

    assert num_times[0] == 2

    the_asset_key = [key for key in io_manager_inst.values.keys() if key[1] == "the_asset"][0]
    assert io_manager_inst.values[the_asset_key] == 5

    other_asset_key = [key for key in io_manager_inst.values.keys() if key[1] == "other_asset"][0]
    assert io_manager_inst.values[other_asset_key] == 6


def test_asset_with_io_manager_key_and_def():
    io_manager_inst = InMemoryIOManager()

    @io_manager
    def the_io_manager():
        return io_manager_inst

    @asset(io_manager={"the_key": the_io_manager})
    def the_asset():
        return 5

    AssetGroup([the_asset]).materialize()

    assert list(io_manager_inst.values.values())[0] == 5


def test_asset_with_io_manager_key_only():
    io_manager_inst = InMemoryIOManager()

    @io_manager
    def the_io_manager():
        return io_manager_inst

    @asset(io_manager="the_key")
    def the_asset():
        return 5

    AssetGroup([the_asset], resource_defs={"the_key": the_io_manager}).materialize()

    assert list(io_manager_inst.values.values())[0] == 5
