from pkg_resources import get_distribution

try:
        __version__ = get_distribution('farmpy').version
except:
    __version__ = 'local'


__all__ = [
    'lsf',
    'lsf_stats',
]

from farmpy import *
