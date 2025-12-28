# -*- mode: python ; coding: utf-8 -*-
# ONEDIR Build - più compatibile con scipy

from PyInstaller.utils.hooks import collect_all


def _collect_optional(package_name):
    try:
        return collect_all(package_name)
    except Exception:
        return ([], [], [])


statsmodels_datas, statsmodels_binaries, statsmodels_hidden = _collect_optional('statsmodels')
patsy_datas, patsy_binaries, patsy_hidden = _collect_optional('patsy')
tbats_datas, tbats_binaries, tbats_hidden = _collect_optional('tbats')
pmdarima_datas, pmdarima_binaries, pmdarima_hidden = _collect_optional('pmdarima')
prophet_datas, prophet_binaries, prophet_hidden = _collect_optional('prophet')
cmdstanpy_datas, cmdstanpy_binaries, cmdstanpy_hidden = _collect_optional('cmdstanpy')
stanio_datas, stanio_binaries, stanio_hidden = _collect_optional('stanio')
holidays_datas, holidays_binaries, holidays_hidden = _collect_optional('holidays')


block_cipher = None

a = Analysis(
    ['analisi_trafficonewfct_profsari.py'],
    pathex=[],
    binaries=statsmodels_binaries + patsy_binaries + tbats_binaries + pmdarima_binaries + prophet_binaries + cmdstanpy_binaries + stanio_binaries + holidays_binaries,
    datas=statsmodels_datas + patsy_datas + tbats_datas + pmdarima_datas + prophet_datas + cmdstanpy_datas + stanio_datas + holidays_datas,
    hiddenimports=[
        'pandas',
        'numpy',
        'matplotlib',
        'matplotlib.backends.backend_tkagg',
        # 'seaborn',  # Rimosso per compatibilità PyInstaller
        # 'scipy',  # Rimosso per compatibilità PyInstaller (bug con scipy.stats)
        # 'scipy.stats',
        # 'scipy._lib',
        # 'scipy.special',
        # 'scipy.integrate',
        # 'scipy.linalg',
        # 'sklearn',  # Rimosso per compatibilità PyInstaller
        # 'sklearn.ensemble',
        # 'sklearn.tree',
        'statsmodels',
        'statsmodels.tsa.holtwinters',
        'statsmodels.tsa.statespace.sarimax',
        'tbats',
        'tbats.bats',
        'tbats.tbats',
        'prophet',
        'prophet.forecaster',
        'holidays',
        'holidays.countries.italy',
        'tkinter',
        'tkinter.ttk',
        'tkinter.filedialog',
        'tkinter.messagebox',
        'PIL',
        'PIL._tkinter_finder',
        'openpyxl',
        'xlsxwriter',
        'concurrent.futures',
    ] + statsmodels_hidden + patsy_hidden + tbats_hidden + pmdarima_hidden + prophet_hidden + cmdstanpy_hidden + stanio_hidden + holidays_hidden,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'IPython',
        'jupyter',
        'notebook',
        'sphinx',
        'pytest',
        'matplotlib.tests',
        'numpy.tests',
        'pandas.tests',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],  # Non includere binaries/zipfiles/datas qui (onedir mode)
    exclude_binaries=True,  # IMPORTANTE: usa onedir
    name='AnalisiForecast',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='AnalisiForecast',
)
