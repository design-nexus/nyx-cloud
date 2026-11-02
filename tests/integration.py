#!/usr/bin/env python3
"""Run with dbus-run-session -- python3 tests/integration.py. No live accounts."""
import ast, json, os, pathlib, socket, subprocess, tempfile, time
ROOT = pathlib.Path(__file__).resolve().parents[1]
BUS = 'io.github.design_nexus.nyx.Cloud1'
def call(method, *args):
    owner=subprocess.check_output(['gdbus','call','--session','--dest','org.freedesktop.DBus','--object-path','/org/freedesktop/DBus','--method','org.freedesktop.DBus.NameHasOwner',BUS],text=True)
    if owner.strip() != '(true,)':
        raise subprocess.CalledProcessError(1,['Cloud service not ready'])

    out = subprocess.check_output(['gdbus', 'call', '--session', '--dest', BUS, '--object-path', '/io/github/design_nexus/nyx/Cloud1', '--method', BUS+'.'+method, *args], stderr=subprocess.PIPE, text=True)
    return ast.literal_eval(out)[0] if out.strip() != '()' else None
def wait(predicate, seconds=35):
    until = time.monotonic()+seconds
    while time.monotonic()<until:
        try:
            result = predicate()
            if result: return result
        except (subprocess.CalledProcessError, FileNotFoundError): pass
        time.sleep(.15)
    raise AssertionError('Timed out')
with tempfile.TemporaryDirectory(prefix='nyx-cloud-test-') as d:
    base = pathlib.Path(d)
    for key, directory in [('HOME','home'),('XDG_CONFIG_HOME','config'),('XDG_DATA_HOME','data'),('XDG_CACHE_HOME','cache'),('XDG_RUNTIME_DIR','runtime')]:
        path=base/directory; path.mkdir(mode=0o700); os.environ[key]=str(path)
    target=base/'remote'; target.mkdir(); (target/'sample.txt').write_text('cloud test\n')
    subprocess.run(['rclone','config','create','external','alias','remote',str(target)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    external=base/'config/rclone/rclone.conf'; original=external.read_bytes()
    log=(base/'daemon.log').open('w+')
    daemon=subprocess.Popen([os.environ.get('NYX_CLOUD_BIN',str(ROOT/'target/debug/nyx-cloudd'))], stdout=log, stderr=log)
    try:
        wait(lambda: call('ListRemotes')=='[]')
        discovered=json.loads(call('Discover')); assert discovered[0]['name']=='external'
        account=call('Adopt','external'); assert call('Adopt','external')==account
        assert external.read_bytes()==original
        rows=json.loads(call('ListRemotes')); assert rows[0]['adopted'] and not rows[0]['managed']
        # Fresh setup through the private channel; use a local alias backend.
        channel=socket.socket(socket.AF_UNIX); channel.connect(call('BeginSetup')); stream=channel.makefile('rwb',buffering=0)
        def request(obj):
            stream.write((json.dumps(obj)+'\n').encode()); return json.loads(stream.readline())
        result=request({'action':'configure','provider':'alias','parameters':{'remote':str(target)}})
        assert 'error' not in result, result
        result=request({'action':'finish','provider':'alias','name':'Fresh account'})
        assert 'id' in result, result
        managed=result['id']; channel.close(); stream.close()
        # Reauthentication stages a replacement and retains account identity.
        channel=socket.socket(socket.AF_UNIX); channel.connect(call('BeginSetup')); stream=channel.makefile('rwb',buffering=0)
        result=request({'action':'configure','provider':'alias','replace':managed,'parameters':{}})
        assert 'error' not in result, result
        result=request({'action':'finish','provider':'alias','name':'Fresh account'})
        assert result.get('id')==managed, result
        channel.close(); stream.close()
        assert len(json.loads(call('ListRemotes')))==2
        # Cancelling an uncommitted account removes its temporary configuration.
        channel=socket.socket(socket.AF_UNIX); channel.connect(call('BeginSetup')); stream=channel.makefile('rwb',buffering=0)
        result=request({'action':'configure','provider':'alias','parameters':{'remote':str(target)}})
        assert 'error' not in result
        channel.close(); stream.close()
        time.sleep(.3)
        names=subprocess.check_output(['rclone','--config',str(base/'config/nyx/cloud/rclone.conf'),'listremotes'],text=True).splitlines()
        assert len(names)==1, names
        conf=base/'config/nyx/cloud/rclone.conf'; assert conf.stat().st_mode & 0o077 == 0
        call('UpdatePreferences', managed, 'Fresh account', json.dumps({'mount_on_login':True,'cache_mib':64,'bandwidth':''}))
        # Login behavior is also startup behavior after service activation.
        daemon.terminate(); daemon.wait(timeout=15)
        daemon=subprocess.Popen([os.environ.get('NYX_CLOUD_BIN',str(ROOT/'target/debug/nyx-cloudd'))], stdout=log, stderr=log)
        wait(lambda: call('ListRemotes'))
        rows=wait(lambda: (r if r['mount_status']=='mounted' else None) if (r:=next(x for x in json.loads(call('ListRemotes')) if x['id']==managed)) else None)
        mount=pathlib.Path(rows['mount_path']); assert (mount/'sample.txt').read_text()=='cloud test\n'
        (mount/'created.txt').write_text('uploaded\n')
        wait(lambda: (target/'created.txt').exists())
        assert (target/'created.txt').read_text()=='uploaded\n'
        (mount/'created.txt').rename(mount/'renamed.txt'); wait(lambda: (target/'renamed.txt').exists())
        (mount/'renamed.txt').unlink(); wait(lambda: not (target/'renamed.txt').exists())
        call('Unmount', managed); wait(lambda: next(x for x in json.loads(call('ListRemotes')) if x['id']==managed)['mount_status']=='unmounted')
        call('TestConnection', managed)
        time.sleep(.5)
        call('Remove', managed, 'true'); assert (target/'sample.txt').exists()
        call('Remove', account, 'false'); assert external.read_bytes()==original
        assert json.loads(call('ListRemotes'))==[]
        # Real local WebDAV exercises password configuration and expired auth.
        probe=socket.socket(); probe.bind(('127.0.0.1',0)); port=probe.getsockname()[1]; probe.close()
        def serve(password):
            return subprocess.Popen(['rclone','serve','webdav',str(target),'--addr',f'127.0.0.1:{port}','--user','tester','--pass',password], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        server=serve('test-password')
        def listening():
            try:
                socket.create_connection(('127.0.0.1',port),timeout=.2).close(); return True
            except OSError: return False
        try:
            wait(listening)
            channel=socket.socket(socket.AF_UNIX); channel.connect(call('BeginSetup')); stream=channel.makefile('rwb',buffering=0)
            result=request({'action':'configure','provider':'webdav','parameters':{'url':f'http://127.0.0.1:{port}','vendor':'other','user':'tester','pass':'test-password'}})
            assert 'error' not in result, result
            result=request({'action':'finish','provider':'webdav','name':'WebDAV test'})
            assert 'id' in result, result
            webdav=result['id']; channel.close(); stream.close()
            assert 'test-password' not in call('ListRemotes')
            server.terminate(); server.wait(timeout=5)
            call('Mount',webdav)
            wait(lambda: next(x for x in json.loads(call('ListRemotes')) if x['id']==webdav)['connection']=='error')
            server=serve('test-password'); wait(listening)
            wait(lambda: next(x for x in json.loads(call('ListRemotes')) if x['id']==webdav)['mount_status']=='mounted',seconds=65)
            call('Unmount',webdav)
            wait(lambda: next(x for x in json.loads(call('ListRemotes')) if x['id']==webdav)['mount_status']=='unmounted')
            server.terminate(); server.wait(timeout=5)
            server=serve('changed-password'); wait(listening)
            call('TestConnection',webdav)
            wait(lambda: next(x for x in json.loads(call('ListRemotes')) if x['id']==webdav)['connection']=='authentication_required')
            call('Remove',webdav,'true')
            assert (target/'sample.txt').exists()
        finally:
            server.terminate(); server.wait(timeout=5)
        print('PASS: local WebDAV setup, password privacy, network reconnection and expired authentication')
        print('PASS: discovery, adoption, fresh setup, permissions, FUSE read/write/rename/delete, safe unmount, configuration-only removal, staged reauthentication, mount-on-login startup')
    finally:
        for mount in (base/'home').iterdir():
            subprocess.run(['fusermount3','-u',str(mount)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        daemon.terminate()
        try: daemon.wait(timeout=15)
        except subprocess.TimeoutExpired: daemon.kill(); daemon.wait()
        log.seek(0); logs=log.read()
        if logs: print('Service diagnostics:',logs[:2000])
