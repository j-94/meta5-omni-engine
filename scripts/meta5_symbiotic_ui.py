from flask import Flask;from flask_socketio import SocketIO,emit
app=__name__;sio=SocketIO(Flask(app))
@sio.on('message')
def _(json):emit('response',{'echo':json})
if __name__=='__main__':sio.run(Flask(app),port=5555)