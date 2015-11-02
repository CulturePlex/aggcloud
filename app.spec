# -*- mode: python -*-
import os
added_files = [
    ('gooey', 'gooey'),
]
block_cipher = os.environ.get("BLOCK_CIPHER")
a = Analysis(['app.py'],
             pathex=[os.getcwd()],
             binaries=None,
             datas=added_files,
             hiddenimports=['gooey'],
             hookspath=None,
             runtime_hooks=None,
             excludes=None,
             win_no_prefer_redirects=None,
             win_private_assemblies=None,
             cipher=block_cipher)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='app',
          debug=False,
          strip=None,
          upx=True,
          console=False)
