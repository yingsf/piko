"""验证 PikoApp 组件只属于各自的应用实例"""

import piko.core.cache as cache_module
import piko.persistence.writer as writer_module
from piko import PikoApp


def test_app_components_are_instance_scoped() -> None:
    """验证不同应用实例不共享 Writer 和 ConfigCache"""
    first = PikoApp(name="first")
    second = PikoApp(name="second")

    assert first.writer is not second.writer
    assert first.config_cache is not second.config_cache
    assert not hasattr(cache_module, "config_cache")
    assert not hasattr(writer_module, "persistence_writer")
