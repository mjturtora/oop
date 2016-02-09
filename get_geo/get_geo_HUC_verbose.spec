# -*- mode: python -*-
a = Analysis([os.path.join(HOMEPATH,'support\\_mountzlib.py'), os.path.join(HOMEPATH,'support\\unpackTK.py'), os.path.join(HOMEPATH,'support\\useTK.py'), os.path.join(HOMEPATH,'support\\useUnicode.py'), 'get_geo_HUC.py', os.path.join(HOMEPATH,'support\\removeTK.py')],
             pathex=['C:\\Downloads\\get_geo'])
pyz = PYZ(a.pure)
exe = EXE(TkPKG(), pyz,
          a.scripts + [('v', '', 'OPTION')],
          a.binaries,
          a.zipfiles,
          a.datas,
          name=os.path.join('dist', 'get_geo_HUC.exe'),
          debug=False,
          strip=False,
          upx=True,
          console=True )
app = BUNDLE(exe,
             name=os.path.join('dist', 'get_geo_HUC.exe.app'))
