"""配置缓存的隔离行为测试。"""

from piko.core.cache import CachedConfig, ConfigCache


def test_cached_config_does_not_share_mutable_nested_values() -> None:
    """验证写入和读取都不会把嵌套可变值泄漏给调用方。"""
    source_nested = {"items": [1]}
    source: dict[str, object] = {"nested": source_nested}
    config = CachedConfig(config_json=source, version=1, schema_version=1)
    cache = ConfigCache()

    source_nested["items"].append(2)
    cache.set("job", config)
    config.config_json["nested"] = {"items": [99]}

    cached = cache.get("job")
    assert cached is not None
    assert cached.config_json == {"nested": {"items": [1]}}

    cached.config_json["nested"] = {"items": [100]}
    reread = cache.get("job")
    assert reread is not None
    assert reread.config_json == {"nested": {"items": [1]}}
