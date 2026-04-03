"""
Shared pytest configuration.

ResourceWarnings about "unclosed database" are GC ordering artifacts:
pandas holds internal cursor references and the GC may surface them during
a different test's teardown. The connections are properly closed by the mocks.
"""
import warnings


def pytest_configure(config):
    warnings.filterwarnings(
        'ignore',
        category=ResourceWarning,
        message='unclosed database',
    )
