"""
Hook personalizzato per scipy.stats per PyInstaller
Colleziona tutti i moduli interni necessari
"""
from PyInstaller.utils.hooks import collect_submodules, collect_data_files

# Colleziona tutti i submoduli di scipy.stats
hiddenimports = collect_submodules('scipy.stats')

# Aggiungi moduli specifici che potrebbero mancare
hiddenimports += [
    'scipy.stats._distn_infrastructure',
    'scipy.stats._continuous_distns',
    'scipy.stats._discrete_distns',
    'scipy.stats.distributions',
    'scipy.stats._stats_py',
    'scipy.stats._stats',
    'scipy.special',
    'scipy.special._ufuncs',
    'scipy._lib',
    'scipy._lib._ccallback',
]

# Colleziona anche i file di dati
datas = collect_data_files('scipy.stats')
