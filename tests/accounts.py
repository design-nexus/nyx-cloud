#!/usr/bin/env python3
"""Targeted account/Notes migration checks on a private bus, with a local remote."""
import ast,json,os,pathlib,socket,subprocess,tempfile,time
ROOT=pathlib.Path(__file__).resolve().parents[1]
BUS='io.github.design_nexus.nyx.Cloud1'
def call(method,*args):
    owner=subprocess.check_output(['gdbus','call','--session','--dest','org.freedesktop.DBus','--object-path','/org/freedesktop/DBus','--method','org.freedesktop.DBus.NameHasOwner',BUS],text=True)
    if owner.strip() != '(true,)':
        raise subprocess.CalledProcessError(1,['Cloud service not ready'])

    out=subprocess.check_output(['gdbus','call','--session','--dest',BUS,'--object-path','/io/github/design_nexus/nyx/Cloud1','--method',BUS+'.'+method,*args],stderr=subprocess.PIPE,text=True)
    return ast.literal_eval(out)[0] if out.strip()!='()' else None
with tempfile.TemporaryDirectory(prefix='nyx-account-test-') as directory:
    base=pathlib.Path(directory)
    for key,name in [('HOME','home'),('XDG_CONFIG_HOME','config'),('XDG_DATA_HOME','data'),('XDG_CACHE_HOME','cache'),('XDG_RUNTIME_DIR','runtime')]:
        path=base/name;path.mkdir(mode=0o700);os.environ[key]=str(path)
    target=base/'remote';target.mkdir();vault=base/'vault';vault.mkdir()
    subprocess.run(['rclone','config','create','gdrive','alias','remote',str(target)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    config=base/'config/rclone/rclone.conf';original=config.read_bytes()
    daemon=subprocess.Popen([os.environ.get('NYX_CLOUD_BIN',str(ROOT/'target/release/nyx-cloudd'))],stderr=subprocess.PIPE)
    try:
        for _ in range(100):
            try:call('ListRemotes');break
            except subprocess.CalledProcessError:time.sleep(.1)
        remote=call('ImportRemote','gdrive');assert call('ImportRemote','gdrive')==remote
        assert config.read_bytes()==original
        notes={'enabled':True,'vault':str(vault),'folder':'Notes'}
        call('ConfigureServices',remote,'google-test','false',json.dumps(notes))
        row=json.loads(call('ListRemotes'))[0]
        assert row['managed'] and row['account_id']=='google-test' and not row['storage_enabled'] and row['notes']['enabled']
        sock=socket.socket(socket.AF_UNIX);sock.connect(call('BeginNotes',remote));stream=sock.makefile('rwb',buffering=0)
        def request(data):
            stream.write((json.dumps(data)+'\n').encode());return json.loads(stream.readline())
        assert request({'action':'list'})==[]
        first=request({'action':'put','path':'hello.md','parents':[],'bytes':list(b'# Hello\n')})
        assert 'id'in first,first
        assert bytes(request({'action':'get','id':first['id'],'path':'hello.md'})['bytes'])==b'# Hello\n'
        assert 'error'in request({'action':'put','path':'hello.md','parents':[],'bytes':[88]})
        second=request({'action':'put','path':'hello.md','parents':[first['id']],'bytes':list(b'# Updated\n')})
        assert 'id'in second,second
        assert len(request({'action':'list'}))==2
        assert bytes(request({'action':'get','id':first['id'],'path':'hello.md'})['bytes'])==b'# Hello\n'
        assert 'error'in request({'action':'put','path':'../escape','parents':[],'bytes':[]})
        binary=os.environ.get('NYX_NOTES_TEST_BINARY')
        if binary:
            test_env=os.environ.copy();test_env['NYX_CLOUD_TEST_ID']=remote;test_env['NYX_NOTES_TEST_ROOT']=str(vault)
            subprocess.run([binary,'cloud_account_transport','--ignored','--nocapture'],env=test_env,check=True)
        notes['enabled']=False;call('ConfigureServices',remote,'google-test','false',json.dumps(notes))
        assert 'error'in request({'action':'list'})
        sock.close();stream.close()
        call('Remove',remote,'true');assert config.read_bytes()==original
        assert len(list((target/'Notes/content').glob('*/*.md')))>=2
        print('PASS: managed import, unchanged external config, stable account link, Notes without Storage, immutable Markdown revisions, stale-write/traversal rejection, disable and data-preserving removal')
    finally:
        daemon.terminate();daemon.wait(timeout=15)
        errors=daemon.stderr.read().decode();assert not errors,errors
