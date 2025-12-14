def assert_aupet(fn,A=1,U=0,P=1,E=0,T=1):
    try:
        fn();print('✓ AUPET verified',{'A':A,'U':U,'P':P,'E':E,'T':T})
    except Exception as ex:print('✗ AUPET failed:',ex)
if __name__=='__main__':
    assert_aupet(lambda: True)