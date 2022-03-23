# -*- mode: python ; coding: utf-8 -*-


block_cipher = None


a = Analysis(['bin/gpas-upload'],
             pathex=[],
             binaries=[],
             datas=[],
             hiddenimports=[],
             hookspath=[],
             hooksconfig={},
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)

<<<<<<< HEAD
#for d in a.binaries:
#    if 'pyarrow' in d[0]:
#        print("REMOVING "+d)
#        a.binaries.remove(d)
#        break


=======
>>>>>>> sprint10
exe = EXE(pyz,
          a.scripts,
          [('W ignore', None, 'OPTION')],
          a.binaries,
          a.zipfiles,
<<<<<<< HEAD
          a.datas,
=======
          a.datas,  
>>>>>>> sprint10
          [],
          name='gpas-upload',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=True,
          disable_windowed_traceback=False,
          target_arch=None,
          codesign_identity=None,
          entitlements_file=None )
